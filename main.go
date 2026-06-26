package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type PaymentJob struct {
	ID     int
	UserID int
	Amount int // in cents
}

// Global thread-safe memory store to simulate our database table records
var (
	mockDBMutex sync.Mutex
	mockDB      = []PaymentJob{
		{ID: 101, UserID: 99, Amount: 4999}, // This should only run ONCE
	}
)

func main() {
	log.Println("🚀 Starting background worker application...")

	// 1. Listen for OS shutdown signals (Ctrl+C or SIGTERM)
	shutdownSig := make(chan os.Signal, 1)
	signal.Notify(shutdownSig, os.Interrupt, syscall.SIGTERM)

	// 2. Track active background processing routines
	var wg sync.WaitGroup

	// 3. Create a root cancelable context
	ctx, cancel := context.WithCancel(context.Background())

	// Simulate your DB pool (using nil here to fall back on our mock state machine)
	var db *sql.DB

	// 4. Start the scheduler loop asynchronously
	go StartPaymentScheduler(ctx, db, &wg)

	// Block main execution until a shutdown signal is intercepted
	sig := <-shutdownSig
	log.Printf("⚠️ Received signal %v. Initiating graceful shutdown...", sig)

	// 5. Cancel the context to stop the cron ticker from generating new loops
	cancel()

	// 6. Wait for all active payment executions to finish their work safely
	log.Println("⏳ Waiting for active payments to finish processing...")
	wg.Wait()

	log.Println("🛑 Clean shutdown complete. App closed safely with 0 data corruption.")
}

// StartPaymentScheduler manages the time intervals
func StartPaymentScheduler(ctx context.Context, db *sql.DB, wg *sync.WaitGroup) {
	ticker := time.NewTicker(5 * time.Second) // Check every 5 seconds
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("⏰ Scheduler ticker loop stopped.")
			return
		case <-ticker.C:
			log.Println("⏰ Checking for due payments...")
			processDuePayments(ctx, db, wg)
		}
	}
}

// processDuePayments sweeps the DB and locks target rows atomically
func processDuePayments(ctx context.Context, db *sql.DB, wg *sync.WaitGroup) {
	// Mock Path: Simulates atomic extraction & mutation using Mutex locks
	if db == nil {
		mockDBMutex.Lock()

		var jobsToProcess []PaymentJob
		if len(mockDB) > 0 {
			// Pull the pending job out of the "database" slice
			jobsToProcess = append(jobsToProcess, mockDB[0])
			// Atomic state mutation: Clear the slice so it's no longer 'pending'
			mockDB = mockDB[1:]
			log.Printf("📥 Found 1 due payment in mock DB. Claiming job ownership...")
		}

		mockDBMutex.Unlock()

		for _, job := range jobsToProcess {
			wg.Add(1)
			go func(j PaymentJob) {
				defer wg.Done()
				executePayment(ctx, db, j)
			}(job)
		}
		return
	}

	// Real SQL Production Path (Requires physical database connection setup)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		log.Printf("Failed to begin transaction: %v", err)
		return
	}
	defer tx.Rollback()

	query := `
		SELECT id, user_id, amount
		FROM scheduled_payments
		WHERE status = 'pending' AND execute_at <= NOW()
		LIMIT 100
		FOR UPDATE SKIP LOCKED;`

	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		log.Printf("Query failed: %v", err)
		return
	}
	defer rows.Close()

	var jobs []PaymentJob
	for rows.Next() {
		var j PaymentJob
		if err := rows.Scan(&j.ID, &j.UserID, &j.Amount); err != nil {
			continue
		}
		jobs = append(jobs, j)
	}

	if len(jobs) == 0 {
		return
	}

	for _, job := range jobs {
		_, err := tx.ExecContext(ctx, "UPDATE scheduled_payments SET status = 'processing' WHERE id = $1", job.ID)
		if err != nil {
			log.Printf("Failed to lock job %d: %v", job.ID, err)
			return
		}
	}

	if err := tx.Commit(); err != nil {
		log.Printf("Failed to commit transaction: %v", err)
		return
	}

	for _, job := range jobs {
		wg.Add(1)
		go func(j PaymentJob) {
			defer wg.Done()
			executePayment(ctx, db, j)
		}(job)
	}
}

// executePayment handles the external third-party I/O
func executePayment(ctx context.Context, db *sql.DB, job PaymentJob) {
	log.Printf("💳 Processing payment %d for User %d (Amount: $%d)...", job.ID, job.UserID, job.Amount/100)

	// Local context with a strict timeout limit for network calls
	networkCtx, networkCancel := context.WithTimeout(ctx, 5*time.Second)
	defer networkCancel()

	err := simulateExternalStripeCall(networkCtx)

	status := "completed"
	if err != nil {
		log.Printf("❌ Payment failed for job %d: %v", job.ID, err)
		status = "failed"
	}

	if db == nil {
		log.Printf("✅ [Mock] Job %d state transit successfully to: %s", job.ID, status)
		return
	}

	// Update record final state in physical DB
	_, dbErr := db.ExecContext(context.Background(),
		"UPDATE scheduled_payments SET status = $1, processed_at = NOW() WHERE id = $2",
		status, job.ID,
	)
	if dbErr != nil {
		log.Printf("❌ Failed to save final status to database for job %d: %v", job.ID, dbErr)
	} else {
		log.Printf("🏁 Job %d finalized as %s", job.ID, status)
	}
}

func simulateExternalStripeCall(ctx context.Context) error {
	select {
	case <-time.After(2 * time.Second): // Simulate network latency
		return nil
	case <-ctx.Done(): // Triggered if timeout or global shutdown hits first
		return ctx.Err()
	}
}
