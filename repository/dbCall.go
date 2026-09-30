package repository

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/maxcillius/Distributed-Job-Scheduler/db"
)

func NewConnection(ctx context.Context) (*DbCall, error) {
	dbUrl, ok := os.LookupEnv("DATABASE_URL")
	if !ok || dbUrl == "" {
		return nil, fmt.Errorf("invalid DATABASE_URL")
	}

	dbConn, err := pgxpool.New(ctx, dbUrl)
	if err != nil {
		return nil, err
	}

	err = dbConn.Ping(ctx)
	if err != nil {
		dbConn.Close()
		return nil, fmt.Errorf("error pinging datbase: %w", err)
	}

	db := db.New(dbConn)

	return &DbCall{db}, nil
}

type DbCall struct {
	db *db.Queries
}

func (d *DbCall) UpdateJobStatus(ctx context.Context, jobRunId string, status string) error {
	_, err := d.db.UpdateJob(ctx, db.UpdateJobParams{ID: string(jobRunId), Status: status})
	if err != nil {
		return err
	}
	return nil
}

func (d *DbCall) IsJobActive(ctx context.Context, jobRunId string) (bool, error) {
	status, err := d.db.GetJobStatus(ctx, jobRunId)
	if err != nil {
		if err == pgx.ErrNoRows {
			return false, nil
		}
		return false, err
	}
	return status.Status == "waiting" || status.Status == "running", nil
}

func (d *DbCall) UpsertJobDefinition(ctx context.Context, job db.InsertJobParams) error {
	_, err := d.db.InsertJob(ctx, db.InsertJobParams{
		ID:             job.ID,
		Name:           job.Name,
		Command:        job.Command,
		Args:           job.Args,
		Workdir:        job.Workdir,
		Timeoutseconds: job.Timeoutseconds,
	})
	if err != nil {
		return err
	}
	return nil
}
