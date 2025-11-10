# core/worker.py
import os
import time
import subprocess
import signal # Imported for graceful shutdown
from multiprocessing import Process
from datetime import datetime
from core.job_manager import JobManager
from config import get_config

class WorkerProcess(Process):
    """
    A single worker process that continuously polls the database for jobs,
    executes them, and manages their state transitions.
    """
    def __init__(self, worker_id, poll_interval=1):
        super().__init__()
        self.worker_id = worker_id
        self.poll_interval = poll_interval
        # Signal handler for graceful shutdown (SIGTERM)
        signal.signal(signal.SIGTERM, self.handle_shutdown)
        self.running = True 

    def handle_shutdown(self, signum, frame):
        """Sets the running flag to False on receiving SIGTERM."""
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] 🛑 Worker {self.worker_id} received shutdown signal. Finishing current job...")
        self.running = False

    def run(self):
        """The main loop for the worker process."""
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 🟢 Worker {self.worker_id} started (PID: {os.getpid()}).")
        
        manager = JobManager() 

        while self.running:
            job = manager.fetch_job_for_worker(self.worker_id)

            if job:
                self._execute_job(job, manager)
            else:
                time.sleep(self.poll_interval)
        
        print(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ Worker {self.worker_id} stopped gracefully.")

    def _execute_job(self, job, manager):
        """Executes the job command and handles the success/failure logic, including detailed logging."""
        job_id = job['id']
        command = job['command']
        current_attempts = job['attempts'] # Attempts BEFORE this run
        attempts = current_attempts + 1 # Attempts AFTER this run

        print(f"[{datetime.now().strftime('%H:%M:%S')}] 🛠️ Worker {self.worker_id} processing Job {job_id} (Attempt: {attempts}). Command: {command}")
        
        exit_code = -1
        try:
            result = subprocess.run(
                command, 
                shell=True, 
                check=False,
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE,
                text=True 
            )
            exit_code = result.returncode
            
        except FileNotFoundError:
            # Command not found is treated as a failure for retries
            print(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ Command not found: {command}.")
            # Continue to failure handling below

        except Exception as e:
            # General execution error
            print(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ Unexpected execution error: {e}.")
            # Continue to failure handling below
        
        # --- State Handling ---
        
        if exit_code == 0:
            # SUCCESS
            print(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ Job {job_id} completed successfully (Exit 0).")
            manager.update_job_state(job_id, 'completed', current_attempts)
            
        else:
            # FAILURE: Non-zero exit code or FileNotFoundError (exit_code is still -1)
            success, delay_seconds = manager.update_job_state(job_id, 'failed', current_attempts)
            
            if not success:
                 print(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ ERROR: Failed to update DB state for {job_id}.")
                 return

            if delay_seconds > 0:
                # Log the retry and the explicit backoff time
                backoff_base = get_config("BACKOFF_BASE")
                print(f"[{datetime.now().strftime('%H:%M:%S')}] 🔄 Job {job_id} failed (Exit {exit_code}). **Retrying.** Waiting for **{delay_seconds} seconds** (Backoff: {backoff_base}^{attempts}).")
            else:
                # This should only happen if the job moved to DEAD
                print(f"[{datetime.now().strftime('%H:%M:%S')}] 🚨 Job {job_id} failed (Exit {exit_code}). **MAX RETRIES REACHED.** Job moved to **DEAD** state.")


# --- PID Management Helpers (No changes needed here) ---

def get_running_workers():
    """Reads the PID file and returns a list of PIDs found in the file."""
    pid_file = get_config("WORKER_PID_FILE")
    pids = []
    try:
        with open(pid_file, 'r') as f:
            pids = [int(p.strip()) for p in f if p.strip().isdigit()]
            return pids
    except FileNotFoundError:
        return []

def write_worker_pid(pid):
    """Writes a new worker PID to the tracking file."""
    pid_file = get_config("WORKER_PID_FILE")
    with open(pid_file, 'a') as f:
        f.write(f"{pid}\n")
        
def clear_worker_pids():
    """Clears the worker tracking file."""
    pid_file = get_config("WORKER_PID_FILE")
    if os.path.exists(pid_file):
        os.remove(pid_file)