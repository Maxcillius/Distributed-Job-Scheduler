package pkg

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/maxcillius/Distributed-Job-Scheduler/db"
	"github.com/maxcillius/Distributed-Job-Scheduler/repository"
	"github.com/maxcillius/Distributed-Job-Scheduler/types"
	amqp "github.com/rabbitmq/amqp091-go"
	"gopkg.in/yaml.v3"
)

func executeWithBackoff(ctx context.Context, log logr.Logger, operationName string, maxRetries int, baseDelay time.Duration, fn func() error) error {
	for attempt := 0; attempt < maxRetries; attempt++ {
		err := fn()
		if err == nil {
			if attempt > 0 {
				log.Info("Operation succeeded after retries", "operation", operationName)
			}
			return nil
		}

		if attempt == maxRetries-1 {
			return fmt.Errorf("%s failed permanently after %d attempts: %w", operationName, maxRetries, err)
		}

		backoff := time.Duration(float64(baseDelay) * math.Pow(2, float64(attempt)))

		jitter := time.Duration(rand.Float64() * float64(backoff) * 0.2)
		sleepDuration := backoff + jitter

		log.Error(err, "Operation failed, backing off and retrying",
			"operation", operationName,
			"attempt", attempt+1,
			"sleep", sleepDuration.String())

		select {
		case <-time.After(sleepDuration):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func Scheduler(ctx context.Context, log logr.Logger, trigChan <-chan struct{}, errChan chan<- error, pool *repository.DbCall) {
	log.Info("Reconcile trigger received. Scheduling jobs...")

	amqpURL, ok := os.LookupEnv("RABBITMQ")
	if !ok || amqpURL == "" {
		reportError(ctx, errChan, fmt.Errorf("RABBITMQ is not set"))
		return
	}

	var conn *amqp.Connection

	err := executeWithBackoff(ctx, log, "RabbitMQ Connection", 5, 2*time.Second, func() error {
		var connErr error
		conn, connErr = amqp.Dial(amqpURL)
		return connErr
	})

	if err != nil {
		reportError(ctx, errChan, fmt.Errorf("failed to connect to RabbitMQ: %w", err))
		return
	}
	defer conn.Close()
	log.Info("Connected to RabbitMQ")

	ch, err := conn.Channel()
	if err != nil {
		reportError(ctx, errChan, fmt.Errorf("failed to open a channel: %w", err))
		return
	}
	defer ch.Close()
	log.Info("Opened a channel")

	err = ch.ExchangeDeclare(
		"jobs_dlx",
		"direct",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		reportError(ctx, errChan, fmt.Errorf("failed to declare dead-letter exchange: %w", err))
		return
	}
	log.Info("Declared Dead Letter Exchange")

	_, err = ch.QueueDeclare(
		"jobs_dlq",
		true,
		false,
		false,
		false,
		amqp.Table{
			amqp.QueueTypeArg: amqp.QueueTypeQuorum,
		},
	)
	if err != nil {
		reportError(ctx, errChan, fmt.Errorf("failed to declare dead-letter queue: %w", err))
		return
	}
	log.Info("Declared Dead Letter Queue")

	// 3. Bind the DLQ to the DLX
	err = ch.QueueBind(
		"jobs_dlq",
		"jobs_failed",
		"jobs_dlx",
		false,
		nil,
	)
	if err != nil {
		reportError(ctx, errChan, fmt.Errorf("failed to bind dead-letter queue: %w", err))
		return
	}

	q, err := ch.QueueDeclare(
		"jobs",
		true,
		false,
		false,
		false,
		amqp.Table{
			amqp.QueueTypeArg:           amqp.QueueTypeQuorum,
			"x-dead-letter-exchange":    "jobs_dlx",
			"x-dead-letter-routing-key": "jobs_failed",
			"x-delivery-limit":          5,
		},
	)
	if err != nil {
		reportError(ctx, errChan, fmt.Errorf("failed to declare main jobs queue: %w", err))
		return
	}
	log.Info("Declared main jobs queue with DLX routing")

	for {
		select {
		case <-ctx.Done():
			return
		case <-trigChan:
			fmt.Println("[Manager] Reconcile trigger received. Scheduling jobs...")

			entries, err := os.ReadDir("./Jobs")
			if err != nil {
				reportError(ctx, errChan, fmt.Errorf("failed to read jobs dir: %w", err))
				continue
			}

			for _, entry := range entries {
				if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".yml") {
					task, err := parseJobFile(entry.Name())
					if err != nil {
						reportError(ctx, errChan, fmt.Errorf("failed to parse job file %s: %w", entry.Name(), err))
						continue
					}

					h := sha256.New()
					h.Write([]byte(fmt.Sprintf("%s%s%s%s", task.Name, task.Command, task.WorkDir, task.Args)))
					jobID := fmt.Sprintf("%x", h.Sum(nil))

					exist, err := pool.IsJobActive(ctx, jobID)
					if err != nil {
						log.Error(err, fmt.Sprintf("failed to check the status of %s", task.Name))
						continue
					}
					if exist {
						continue
					}

					data, err := json.Marshal(task)
					if err != nil {
						reportError(ctx, errChan, fmt.Errorf("failed to marshal the data of %s: %w", task.Name, err))
						continue
					}

					jobDetails := db.InsertJobParams{
						ID:             jobID,
						Name:           task.Name,
						Command:        task.Command,
						Args:           task.Args,
						Workdir:        pgtype.Text{String: task.WorkDir, Valid: true},
						Timeoutseconds: pgtype.Int4{Int32: int32(task.TimeoutSeconds), Valid: true},
						Status:         "waiting",
					}

					err = pool.UpsertJobDefinition(ctx, jobDetails)
					if err != nil {
						log.Error(err, fmt.Sprintf("failed to insert in database %s", task.Name))
						// logging error at scheduler level
						reportError(ctx, errChan, fmt.Errorf("failed to insert job in database %s: %w", task.Name, err))
						continue
						// Avoid pushing jobs into queue if failed to insert into the database
					}

					if err := ch.PublishWithContext(ctx, "", q.Name, false, false,
						amqp.Publishing{
							ContentType: "text/plain",
							Body:        []byte(data),
						}); err != nil {
						log.Error(err, "failed to schedule job", "job_name", task.Name)
						reportError(ctx, errChan, fmt.Errorf("failed to schedule job %s: %w", task.Name, err))
						continue
					}
					log.Info("Dispatched job", "job_name", task.Name)
				}
			}
		}
	}
}

func parseJobFile(filename string) (*types.JobTask, error) {
	path := filepath.Join("./Jobs", filename)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var task types.JobTask
	if err := yaml.Unmarshal(data, &task); err != nil {
		return nil, err
	}
	return &task, nil
}
