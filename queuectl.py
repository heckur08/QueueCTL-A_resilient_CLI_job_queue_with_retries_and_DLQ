# queuectl.py
import click
import json
import os
import signal
import time
from multiprocessing import Process

# Import core components
from core.job_manager import JobManager
from core.worker import WorkerProcess, write_worker_pid, get_running_workers, clear_worker_pids
from config import get_config

# --- Setup ---
@click.group()
def cli():
    """queuectl: A CLI-based background job queue system. Manages jobs, workers, and DLQ."""
    pass

# --- 1. ENQUEUE Command ---
@cli.command()
@click.argument('job_json', type=str)
def enqueue(job_json):
    """
    Add a new job to the queue. 
    Example: queuectl enqueue '{"id":"job1","command":"sleep 2"}'
    """
    try:
        job_data = json.loads(job_json)
        if "id" not in job_data or "command" not in job_data:
            raise ValueError("Job JSON must contain 'id' and 'command'.")
        
        manager = JobManager()
        if manager.add_job(job_data):
            click.echo(f"✅ Job '{job_data['id']}' successfully **enqueued**.")
        
    except json.JSONDecodeError:
        click.echo("❌ Error: Invalid JSON format provided.")
    except ValueError as e:
        click.echo(f"❌ Error: {e}")
    except Exception as e:
        click.echo(f"❌ A database error occurred: {e}")


# --- 2. LIST Command ---
@cli.command()
@click.option('--state', '-s', type=click.Choice(['pending', 'processing', 'completed', 'failed', 'dead'], case_sensitive=False),
              help="Filter jobs by state.")
def list(state):
    """List jobs by state or list all jobs."""
    manager = JobManager()
    jobs = manager.list_jobs(state)
    
    if not jobs:
        click.echo(f"No jobs found{(' in state: ' + state) if state else ''}.")
        return

    click.echo(f"📋 Jobs ({len(jobs)} found):")
    for job in jobs:
        status_symbol = "🟢" if job['state'] == 'completed' else "🟡" if job['state'] == 'pending' else "🔴"
        click.echo(f"{status_symbol} ID: {job['id']:<10} | State: {job['state']:<12} | Attempts: {job['attempts']}/{job['max_retries']} | Command: {job['command']}")


# --- 3. STATUS Command ---
@cli.command()
def status():
    """Show summary of all job states & active workers."""
    manager = JobManager()
    summary = manager.get_job_summary()
    active_workers = get_running_workers()

    click.echo("--- 📊 Job Queue Status ---")
    
    # Display job state summary
    click.echo("Job Counts:")
    for state, count in summary.items():
        click.echo(f"  - **{state.capitalize():<12}**: {count}")

    # Display Worker Status
    click.echo("\nWorker Status:")
    click.echo(f"  - **Active Workers**: {len(active_workers)}")
    if active_workers:
        click.echo(f"  - PIDs: {', '.join(map(str, active_workers))}")

    click.echo(f"\nConfiguration: MAX_RETRIES = {get_config('MAX_RETRIES')}, BACKOFF_BASE = {get_config('BACKOFF_BASE')}")


# --- 4. WORKER START/STOP Commands ---
@cli.group()
def worker():
    """Commands to manage the background worker processes."""
    pass

@worker.command()
@click.option('--count', default=1, type=int, help='Number of workers to start.')
def start(count):
    """Start one or more workers."""
    if count < 1:
        click.echo("⚠️ Count must be at least 1.")
        return

    running_pids = get_running_workers()
    if running_pids:
        click.echo(f"⚠️ {len(running_pids)} workers are already running. Stop them first.")
        return

    clear_worker_pids() # Clear any stale PIDs

    click.echo(f"Starting **{count}** worker process(es)...")
    for i in range(count):
        worker_id = f"worker-{os.getpid()}-{i}" # Unique ID for the worker
        p = WorkerProcess(worker_id=worker_id)
        p.start()
        write_worker_pid(p.pid)
        click.echo(f"🟢 Started worker {i+1}/{count} (ID: {worker_id}, PID: {p.pid})")
    
    click.echo("\nWorkers started. Use **queuectl worker stop** to shut down gracefully.")


@worker.command()
def stop():
    """Stop running workers gracefully."""
    pids = get_running_workers()
    if not pids:
        click.echo("⚠️ No active workers found to stop.")
        return

    click.echo(f"Attempting **graceful shutdown** of {len(pids)} workers...")
    
    for pid in pids:
        try:
            # os.kill sends a signal (SIGTERM by default)
            os.kill(pid, signal.SIGTERM) 
            click.echo(f"🛑 Sent graceful stop signal to PID **{pid}**.")
        except ProcessLookupError:
            click.echo(f"⚠️ Warning: PID {pid} was not found (already stopped).")
        except Exception as e:
            click.echo(f"❌ Error stopping PID {pid}: {e}")
            
    time.sleep(1) # Give workers a moment to clean up
    clear_worker_pids()
    click.echo("\nWorker stop signals sent and PID file cleared.")


# --- 5. DLQ Commands ---
@cli.group()
def dlq():
    """Commands to manage the Dead Letter Queue (DLQ)."""
    pass

@dlq.command(name='list')
def dlq_list():
    """View jobs in the Dead Letter Queue."""
    manager = JobManager()
    jobs = manager.get_dlq_jobs()
    
    if not jobs:
        click.echo("The **Dead Letter Queue** is empty.")
        return
        
    click.echo(f"🚨 Dead Letter Queue ({len(jobs)} jobs):")
    for job in jobs:
        click.echo(f"🔴 ID: {job['id']:<10} | Attempts: {job['attempts']}/{job['max_retries']} | Command: {job['command']}")

@dlq.command()
@click.argument('job_id', type=str)
def retry(job_id):
    """
    Move a permanently failed job (DLQ) back to the pending queue.
    """
    manager = JobManager()
    if manager.dlq_retry_job(job_id):
        click.echo(f"🔄 Job '{job_id}' successfully moved back to **pending** queue with attempts reset to 0.")
    else:
        click.echo(f"❌ Failed to retry Job '{job_id}'. Check if it exists and is in the '**dead**' state.")


# --- 6. CONFIG Command ---
@cli.group()
def config():
    """Manage system configurations (e.g., max-retries)."""
    pass

@config.command()
@click.argument('key', type=str)
@click.argument('value', type=str)
def set(key, value):
    """
    Set a configuration value (e.g., max-retries 5 or backoff-base 3).
    NOTE: For persistence, you'd need to update the configuration source.
    """
    click.echo("⚠️ This command requires persistent configuration logic to be fully effective.")
    click.echo(f"⚙️ Setting value for {key} = {value}")

# --- Execute CLI ---
if __name__ == '__main__':
    # Make sure we use the correct Python interpreter alias
    cli()