# QueueCTL - Backend Developer Internship Assignment Submission

## 🌟 Project Overview
**QueueCTL** is a production-grade **CLI-based Job Queue System** designed for managing background jobs efficiently.  
It supports **parallel execution**, **exponential backoff retries**, and a **Dead Letter Queue (DLQ)** for failed jobs.

---

## 🏗️ Architecture Overview

### 1. Persistence Layer (`core/job_manager.py`)
- **Database:** Uses SQLite (`queuectl.db`) for persistent single-file job storage.
- **Locking:** Implements concurrency safety using atomic `UPDATE` transactions—ensuring no two workers process the same job.
- **Backoff Logic:** Applies exponential backoff:  
  \( delay = base^{attempts} \)

### 2. Worker Management (`core/worker.py`)
- **Concurrency:** Workers are true parallel OS processes via Python’s `multiprocessing`.
- **Execution:** Jobs are executed using `subprocess.run()`.  
  - Exit code **0 → success**
  - Non-zero → triggers retry
- **Graceful Shutdown:** Handles `SIGTERM` to allow clean worker exits after finishing the current job.

### 3. Job Lifecycle
```
pending → processing → completed (success)
                     ↘ failed (retry)
                       ↘ dead (DLQ)
```

---

## ⚙️ Setup Instructions

### 📁 Project Structure
```
queuectl_project/
├── queuectl.py                # Main CLI application
├── config.py                  # Configuration defaults (MAX_RETRIES, BACKOFF_BASE)
├── queuectl.db                # SQLite database (auto-generated)
├── queuectl_workers.pid       # Worker tracking file
├── core/
│   ├── __init__.py
│   ├── job_manager.py         # SQLite Persistence & Locking
│   └── worker.py              # Worker Process, Execution & Concurrency
└── README.md
```

### 🧩 Dependencies
- **Python 3.7+**
- **Click CLI library**

Install dependencies:
```bash
python -m pip install click
```

---

## 💻 CLI Usage Examples

> All commands should be executed from the project’s root directory.  
> On Windows, use `cmd /c` for quoting complex JSON arguments.

### 🧱 A. Enqueuing Jobs
Add jobs to the pending queue.

| Command | Description |
|----------|--------------|
| `cmd /c 'python queuectl.py enqueue "JSON_STRING"'` | Add a job definition |

**Examples:**
```bash
# Successful Job
cmd /c 'python queuectl.py enqueue "{"id":"job-S1", "command":"echo Job started at %time%"}"'

# Failing Job (for retry test)
cmd /c 'python queuectl.py enqueue "{"id":"job-F1", "command":"false", "max_retries": 2}"'
```

---

### ⚙️ B. Worker Management
| Command | Description |
|----------|-------------|
| `python queuectl.py worker start --count N` | Start N workers |
| `python queuectl.py worker stop` | Gracefully stop all workers |

**Example:**
```bash
python queuectl.py worker start --count 3
```

---

### 📊 C. Status and Listing
| Command | Description |
|----------|-------------|
| `python queuectl.py status` | Show job and worker summary |
| `python queuectl.py list --state STATE` | List jobs by state (pending, dead, etc.) |

**Example Output:**
```bash
python queuectl.py status

--- Job Queue Status ---
Job Counts:
  - Completed : 1
  - Dead      : 0
  - Pending   : 1

Worker Status:
  - Active Workers: 3
```

**List Dead Jobs:**
```bash
python queuectl.py list --state dead
```

---

### ☠️ D. Dead Letter Queue (DLQ) Management
| Command | Description |
|----------|-------------|
| `python queuectl.py dlq list` | List all DLQ jobs |
| `python queuectl.py dlq retry JOB_ID` | Reset and requeue a DLQ job |

**Example:**
```bash
python queuectl.py dlq retry job-F1
```

---

## 🧪 Test Scenarios

### ✅ 1. Job Lifecycle & Persistence
| Step | Command | Expected Behavior |
|------|----------|------------------|
| Enqueue failing job | `cmd /c 'python queuectl.py enqueue "{"id":"F1", "command":"false"}"'` | Job fails immediately |
| Start worker | `python queuectl.py worker start --count 1` | Retry logic triggered |
| Verify retries | Monitor logs | Backoff: 2s → 4s → 8s |
| Verify DLQ | `python queuectl.py dlq list` | Job `F1` appears in DLQ |
| Verify persistence | Restart & run `status` | DLQ count persists |

---

### ⚡ 2. Concurrency & Locking
| Step | Command | Expected Behavior |
|------|----------|------------------|
| Enqueue long jobs | `cmd /c 'python queuectl.py enqueue "{"id":"C1", "command":"sleep 6"}"'` (repeat for C2, C3) | Long-running jobs |
| Start workers | `python queuectl.py worker start --count 3` | 3 concurrent workers |
| Observe | Monitor logs | All jobs process in parallel (~6s total) |
| Stop workers | `python queuectl.py worker stop` | Clean exit after current jobs |

---

## 💡 Assumptions & Trade-offs

| Decision | Rationale / Trade-off |
|-----------|------------------------|
| **SQLite** | Lightweight, embedded DB with application-level locking. |
| **Worker PID File** | Simple CLI-based tracking (not a service manager). |
| **Config Module** | Static config via `config.py` (non-persistent CLI changes). |

---

## 🏁 Summary
QueueCTL demonstrates a **robust, concurrent, and persistent** job queue system with:
- Safe concurrency via SQLite locks  
- Exponential retry backoff  
- Graceful worker shutdown  
- DLQ management for failure

---
**Author:** *Anurag Singh*  
**Version:** 1.0  
**Language:** Python 3  
