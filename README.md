# golang-background-jobs

# Fault-Tolerant Scheduled Payment Worker in Go

A high-performance, distributed-safe background worker system implemented in Go. This project demonstrates how to decouple time-intensive third-party I/O operations (like payment gateways) from the synchronous user request lifecycle while guaranteeing atomic processing, zero-overlap scaling, and data preservation during unexpected server shutdowns.

## 🚀 Key Features & Achievements

- **Decoupled Architecture:** Offloads high-latency external network calls from the main application thread to specialized concurrent worker pools.
- **Horizontal Scale Safety:** Uses atomic state transitions (`FOR UPDATE SKIP LOCKED` simulation) to ensure multiple deployed server instances can sweep the same database concurrently without double-processing jobs.
- **Defensive Network Boundaries:** Wraps external API requests in strict `context.WithTimeout` lifecycles to eliminate lingering zombie routines and memory leaks during downstream outages.
- **Zero-Corruption Graceful Shutdown:** Catches OS termination signals (`SIGTERM`, `os.Interrupt`) to smoothly drain active workers via `sync.WaitGroup` before terminating the runtime environment.

---

## 🛠️ System Architecture

1. **The Scheduler (Ticker):** An internal clock mechanism wakes up at a configured interval.
2. **The Atomic Sweep:** The database is queried for records that are due. Matching records are immediately marked as `processing` in an atomic step.
3. **The Worker Pool:** Locked jobs are handed off to concurrent worker goroutines.
4. **The Gateway Call:** Workers attempt to execute the payment with a strict network timeout constraint.
5. **Finalization:** The database record is transitioned to its terminal state (`completed` or `failed`).

---

## 💻 Getting Started

### Prerequisites

- Go 1.21 or higher

### Installation & Run

1. Clone the repository:

```bash
git clone [https://github.com/yourusername/go-scheduled-payment-worker.git](https://github.com/yourusername/go-scheduled-payment-worker.git)
cd go-scheduled-payment-worker
```
