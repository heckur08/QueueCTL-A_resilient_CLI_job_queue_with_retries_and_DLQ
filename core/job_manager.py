# core/job_manager.py
import sqlite3
from datetime import datetime, timezone, timedelta
from config import get_config

DB_PATH = get_config("DB_PATH")

class JobManager:
    def __init__(self):
        self.conn = sqlite3.connect(DB_PATH) 
        self.conn.row_factory = sqlite3.Row 
        self._initialize_db()

    def _initialize_db(self):
        """Creates the jobs table if it doesn't exist."""
        cursor = self.conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                command TEXT NOT NULL,
                state TEXT NOT NULL,         -- pending, processing, completed, failed, dead
                attempts INTEGER DEFAULT 0,
                max_retries INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                next_run_at TEXT,            -- Used for calculating backoff delay
                worker_id TEXT               -- For concurrency control (locking)
            );
        """)
        self.conn.commit()

    def _utc_now(self):
        """Returns current time in ISO format with milliseconds and timezone for precise comparison."""
        return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    def _get_job_config(self, job_id, config_key):
        """Helper to retrieve specific job configuration (e.g., max_retries)."""
        cursor = self.conn.execute(f"SELECT {config_key} FROM jobs WHERE id = ?", (job_id,))
        row = cursor.fetchone()
        return row[config_key] if row else get_config(config_key.upper())

    def add_job(self, job_data):
        """Implements the queuectl enqueue command."""
        now = self._utc_now()
        data = {
            "id": job_data["id"],
            "command": job_data["command"],
            "state": "pending",
            "attempts": 0,
            "max_retries": job_data.get("max_retries", get_config("MAX_RETRIES")),
            "created_at": now,
            "updated_at": now,
            "next_run_at": now, 
            "worker_id": None
        }
        
        try:
            self.conn.execute("""
                INSERT INTO jobs VALUES (
                    :id, :command, :state, :attempts, :max_retries, 
                    :created_at, :updated_at, :next_run_at, :worker_id
                )
            """, data)
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            print(f"Error: Job ID {data['id']} already exists.")
            return False

    def get_job_summary(self):
        """Implements part of queuectl status."""
        cursor = self.conn.execute("SELECT state, COUNT(*) as count FROM jobs GROUP BY state")
        return {row['state']: row['count'] for row in cursor.fetchall()}

    def list_jobs(self, state=None):
        """Implements queuectl list --state."""
        query = "SELECT * FROM jobs"
        params = []
        if state:
            query += " WHERE state = ?"
            params.append(state)
        
        query += " ORDER BY created_at ASC" 
        
        cursor = self.conn.execute(query, params)
        return [dict(row) for row in cursor.fetchall()]

    def fetch_job_for_worker(self, worker_id):
        """
        Fetches the next pending/retryable job and marks it as processing 
        (the atomic lock).
        """
        now = self._utc_now()
        
        try:
            self.conn.execute("BEGIN IMMEDIATE") 

            cursor = self.conn.execute(f"""
                SELECT id, command, attempts, max_retries
                FROM jobs
                WHERE state IN ('pending', 'failed')
                AND next_run_at <= '{now}' 
                ORDER BY updated_at ASC, created_at ASC 
                LIMIT 1
            """)
            job_row = cursor.fetchone()

            if not job_row:
                self.conn.commit()
                return None

            job = dict(job_row)
            
            # Lock the job
            self.conn.execute("""
                UPDATE jobs 
                SET state = 'processing', updated_at = ?, worker_id = ?
                WHERE id = ? 
            """, (now, worker_id, job['id']))
            
            self.conn.commit()
            return job

        except Exception as e:
            self.conn.rollback()
            print(f"Database error during job fetch: {e}")
            return None

    def update_job_state(self, job_id, new_state, current_attempts):
        """
        Updates a job's state after processing and applies retry/DLQ logic.
        Returns (success: bool, delay_in_seconds: int)
        """
        now = self._utc_now()
        update_fields = {'state': new_state, 'updated_at': now, 'worker_id': None, 'attempts': current_attempts}
        delay_seconds = 0
        
        if new_state == 'completed':
             # No delay, successful
             pass 

        elif new_state == 'failed':
            max_retries = self._get_job_config(job_id, 'max_retries')
            new_attempts = current_attempts + 1
            update_fields['attempts'] = new_attempts

            if new_attempts > max_retries:
                # Max retries exceeded, move to Dead Letter Queue
                update_fields['state'] = 'dead'
            else:
                # Calculate exponential backoff: delay = base ^ attempts seconds
                backoff_base = get_config("BACKOFF_BASE")
                delay_seconds = backoff_base ** new_attempts 
                
                current_dt = datetime.strptime(now, '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc)
                future_time = current_dt + timedelta(seconds=delay_seconds)
                
                update_fields['next_run_at'] = future_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                
        # Build the dynamic SET clause
        set_clause = ", ".join([f"{k} = :{k}" for k in update_fields.keys()])
        update_fields['id'] = job_id
        
        try:
            self.conn.execute(f"UPDATE jobs SET {set_clause} WHERE id = :id", update_fields)
            self.conn.commit()
            return True, delay_seconds
        except Exception as e:
            self.conn.rollback()
            print(f"Database error updating job {job_id}: {e}")
            return False, 0

    # --- DLQ Methods ---
    
    def dlq_retry_job(self, job_id):
        """Resets a dead job (DLQ) back to pending."""
        now = self._utc_now()
        try:
            self.conn.execute("""
                UPDATE jobs 
                SET state = 'pending', attempts = 0, updated_at = ?, next_run_at = ?
                WHERE id = ? AND state = 'dead'
            """, (now, now, job_id))
            
            if self.conn.total_changes == 0:
                self.conn.rollback()
                return False

            self.conn.commit()
            return True
        except Exception as e:
            self.conn.rollback()
            print(f"Database error retrying DLQ job {job_id}: {e}")
            return False

    def get_dlq_jobs(self):
        """Returns a list of all dead jobs."""
        return self.list_jobs(state='dead')