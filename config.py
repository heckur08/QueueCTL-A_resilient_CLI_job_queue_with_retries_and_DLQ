# config.py
import os

# Default configuration settings
DEFAULT_CONFIG = {
    "DB_PATH": "queuectl.db",
    "MAX_RETRIES": 3,
    "BACKOFF_BASE": 2, # For delay = base ^ attempts
    # Worker PID file path (use /tmp for POSIX, or C:\temp for Windows clarity)
    "WORKER_PID_FILE": os.path.join(os.getcwd(), "queuectl_workers.pid") 
}

def get_config(key):
    """Simple getter for configuration values."""
    return DEFAULT_CONFIG.get(key)