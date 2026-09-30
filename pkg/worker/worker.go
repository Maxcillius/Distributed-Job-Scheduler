package worker

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/go-logr/logr"
	"github.com/maxcillius/Distributed-Job-Scheduler/repository"
	"github.com/maxcillius/Distributed-Job-Scheduler/types"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	maxContainerMemory    = 512 * 1024 * 1024
	maxContainerNanoCPUs  = 1_000_000_000
	maxContainerProcesses = 128
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

func StartWorker(ctx context.Context, log logr.Logger, pool *repository.DbCall) {
	amqpURL, ok := os.LookupEnv("RABBITMQ")
	if !ok || amqpURL == "" {
		log.Error(fmt.Errorf("RABBITMQ is not set"), "cannot start worker")
		return
	}

	var conn *amqp.Connection

	err := executeWithBackoff(ctx, log, "RabbitMQ Connection", 5, 2*time.Second, func() error {
		var connErr error
		conn, connErr = amqp.Dial(amqpURL)
		return connErr
	})

	if err != nil {
		log.Error(err, "failed to connect to RabbitMQ")
		return
	}
	defer conn.Close()
	log.Info("Connected to RabbitMQ")

	ch, err := conn.Channel()
	if err != nil {
		log.Error(err, "failed to build queue channel")
		return
	}
	log.Info("Opened a channel")
	defer ch.Close()

	dockerCli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		log.Error(err, "failed to initialize Docker client")
		return
	}
	defer dockerCli.Close()
	log.Info("Connected to Docker Engine")

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
		log.Error(err, "failed to declare consumer queue")
		return
	}
	log.Info("Declared a queue")

	jobs, err := ch.Consume(
		q.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Error(err, "failed to register a consumer")
		return
	}
	log.Info("Registered a consumer")

	for {
		select {
		case <-ctx.Done():
			log.Info("Shutting down the worker")
			return
		case d, ok := <-jobs:
			if !ok {
				log.Error(fmt.Errorf("RabbitMQ delivery channel closed"), "stopping worker")
				return
			}
			payload := d.Body
			var task types.JobTask
			if err := json.Unmarshal([]byte(payload), &task); err != nil {
				log.Error(err, "[Worker] Invalid job payload, sending to DLQ")
				if nackErr := d.Nack(false, false); nackErr != nil {
					log.Error(nackErr, "failed to reject invalid job payload")
				}
				continue
			}

			h := sha256.New()
			h.Write([]byte(fmt.Sprintf("%s%s%s%s", task.Name, task.Command, task.WorkDir, task.Args)))
			jobID := fmt.Sprintf("%x", h.Sum(nil))

			log.Info("Received Task", "job_name", task.Name)

			if err := pool.UpdateJobStatus(ctx, jobID, "running"); err != nil {
				log.Error(err, fmt.Sprintf("failed to update job status. job: %s", task.Name))

				if nackErr := d.Nack(false, true); nackErr != nil {
					log.Error(nackErr, "failed to requeue job after status update failure", "job_name", task.Name)
				}
				continue
			}

			err = runDockerJob(ctx, dockerCli, task)
			if err != nil {
				log.Error(err, fmt.Sprintf("job execution failed: %s", task.Name))
				if ctx.Err() != nil {
					if nackErr := d.Nack(false, true); nackErr != nil {
						log.Error(nackErr, "failed to requeue interrupted job", "job_name", task.Name)
					}
					continue
				}
				if statusErr := pool.UpdateJobStatus(ctx, jobID, "failed"); statusErr != nil {
					log.Error(statusErr, "failed to update job status to failed", "job_name", task.Name)
					if nackErr := d.Nack(false, true); nackErr != nil {
						log.Error(nackErr, "failed to requeue job after failed status update", "job_name", task.Name)
					}
					continue
				}
				if ackErr := d.Ack(false); ackErr != nil {
					log.Error(ackErr, "failed to acknowledge failed job", "job_name", task.Name)
				}
				continue
			}

			if err := pool.UpdateJobStatus(ctx, jobID, "done"); err != nil {
				log.Error(err, fmt.Sprintf("failed to update job status to done: %s", task.Name))
				if nackErr := d.Nack(false, true); nackErr != nil {
					log.Error(nackErr, "failed to requeue job after completion status update failure", "job_name", task.Name)
				}
				continue
			}

			if err := d.Ack(false); err != nil {
				log.Error(err, "failed to acknowledge job: %s", task.Name)
			} else {
				log.Info("Job finished and acknowledged successfully", "job_name", task.Name)
			}
		}
	}
}

func runDockerJob(ctx context.Context, cli *client.Client, task types.JobTask) error {
	jobCtx := ctx
	if task.TimeoutSeconds > 0 {
		var cancel context.CancelFunc
		jobCtx, cancel = context.WithTimeout(ctx, time.Duration(task.TimeoutSeconds)*time.Second)
		defer cancel()
	}

	imageName := task.Image
	if imageName == "" {
		imageName = "alpine:latest"
	}

	reader, err := cli.ImagePull(jobCtx, imageName, image.PullOptions{})
	if err != nil {
		return fmt.Errorf("failed to pull image %s: %w", imageName, err)
	}
	defer reader.Close()
	_, _ = io.Copy(io.Discard, reader)

	var cmd []string
	if task.Command != "" {
		cmd = append(cmd, task.Command)
	}
	if len(task.Args) > 0 {
		cmd = append(cmd, task.Args...)
	}

	var dockerCmd []string
	if len(cmd) > 0 {
		dockerCmd = cmd
	} else {
		dockerCmd = nil
	}

	var envs []string
	for k, v := range task.Env {
		envs = append(envs, fmt.Sprintf("%s=%s", k, v))
	}

	workDir := task.WorkDir
	if workDir == "." {
		workDir = ""
	} else if workDir != "" && !strings.HasPrefix(workDir, "/") {
		workDir = "/" + workDir
	}

	processLimit := int64(maxContainerProcesses)
	resp, err := cli.ContainerCreate(jobCtx, &container.Config{
		Image:      imageName,
		Cmd:        dockerCmd,
		Env:        envs,
		WorkingDir: workDir,
	}, &container.HostConfig{
		Resources: container.Resources{
			Memory:     maxContainerMemory,
			MemorySwap: maxContainerMemory,
			NanoCPUs:   maxContainerNanoCPUs,
			PidsLimit:  &processLimit,
		},
	}, nil, nil, "")
	if err != nil {
		return fmt.Errorf("failed to create container: %w", err)
	}

	defer func() {
		_ = cli.ContainerRemove(context.Background(), resp.ID, container.RemoveOptions{Force: true})
	}()

	if err := cli.ContainerStart(jobCtx, resp.ID, container.StartOptions{}); err != nil {
		return fmt.Errorf("failed to start container: %w", err)
	}

	out, err := cli.ContainerLogs(jobCtx, resp.ID, container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Follow:     true,
	})
	if err == nil {
		go func() {
			defer out.Close()
			_, _ = stdcopy.StdCopy(os.Stdout, os.Stderr, out)
		}()
	}

	statusCh, errCh := cli.ContainerWait(jobCtx, resp.ID, container.WaitConditionNotRunning)
	select {
	case err, ok := <-errCh:
		if !ok {
			return fmt.Errorf("container wait error channel closed unexpectedly")
		}
		if err != nil {
			return fmt.Errorf("container wait error: %w", err)
		}
	case status, ok := <-statusCh:
		if !ok {
			return fmt.Errorf("container status channel closed unexpectedly")
		}
		if status.StatusCode != 0 {
			return fmt.Errorf("container exited with non-zero status: %d", status.StatusCode)
		}
	case <-jobCtx.Done():
		return fmt.Errorf("job timed out or was cancelled: %w", jobCtx.Err())
	}

	return nil
}
