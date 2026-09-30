package pkg

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/go-logr/logr"
)

const (
	mirrorDir  = "./.mirror"
	stageDir   = "./.stage"
	backupDir  = "./.jobs-backup"
	jobsDir    = "./Jobs"
	pollPeriod = 10 * time.Second
)

func Watcher(ctx context.Context, log logr.Logger, trigChan chan<- struct{}, errChan chan<- error) {
	repoURL := os.Getenv("REPO_URL")

	if len(repoURL) == 0 {
		reportError(ctx, errChan, fmt.Errorf("REPO_URL is not set"))
		return
	}

	ticker := time.NewTicker(pollPeriod)
	defer ticker.Stop()

	if err := syncRepo(ctx, trigChan, repoURL, log); err != nil {
		reportError(ctx, errChan, fmt.Errorf("initial sync failed: %w", err))
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := syncRepo(ctx, trigChan, repoURL, log); err != nil {
				reportError(ctx, errChan, fmt.Errorf("sync failed: %w", err))
			}
		}
	}
}

func reportError(ctx context.Context, errChan chan<- error, err error) bool {
	select {
	case errChan <- err:
		return true
	case <-ctx.Done():
		return false
	}
}

func syncRepo(ctx context.Context, trigChan chan<- struct{}, repoURL string, l logr.Logger) error {
	if _, err := os.Stat(mirrorDir); errors.Is(err, os.ErrNotExist) {
		l.Info("Cloning repo...")
		if err := exec.CommandContext(ctx, "git", "clone", repoURL, mirrorDir).Run(); err != nil {
			return fmt.Errorf("clone failed: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("failed to inspect mirror directory: %w", err)
	}

	cmdPull := exec.CommandContext(ctx, "git", "pull")
	cmdPull.Dir = mirrorDir
	if err := cmdPull.Run(); err != nil {
		return fmt.Errorf("pull failed: %w", err)
	}

	cmdRev := exec.CommandContext(ctx, "git", "rev-parse", "HEAD")
	cmdRev.Dir = mirrorDir
	out, err := cmdRev.Output()
	if err != nil {
		return fmt.Errorf("rev-parse failed: %w", err)
	}
	newCommit := strings.TrimSpace(string(out))

	currentCommit, err := getCurrentCommit()
	if err != nil {
		return fmt.Errorf("failed to read current commit: %w", err)
	}
	jobsInfo, jobsErr := os.Stat(jobsDir)
	jobsExist := jobsErr == nil && jobsInfo.IsDir()
	if jobsErr != nil && !errors.Is(jobsErr, os.ErrNotExist) {
		return fmt.Errorf("failed to inspect jobs directory: %w", jobsErr)
	}
	if jobsErr == nil && !jobsInfo.IsDir() {
		return fmt.Errorf("jobs path exists but is not a directory")
	}

	if newCommit != currentCommit || !jobsExist {
		l.Info("Change detected (Old: %s, New: %s). Swapping directories...\n", currentCommit, newCommit)

		if err := os.RemoveAll(stageDir); err != nil {
			return fmt.Errorf("failed to clear stage directory: %w", err)
		}

		if err := exec.CommandContext(ctx, "cp", "-r", mirrorDir, stageDir).Run(); err != nil {
			return fmt.Errorf("copy to stage failed: %w", err)
		}

		if err := os.RemoveAll(backupDir); err != nil {
			return fmt.Errorf("failed to clear backup directory: %w", err)
		}
		if jobsExist {
			if err := os.Rename(jobsDir, backupDir); err != nil {
				return fmt.Errorf("failed to preserve old Jobs dir: %w", err)
			}
		}

		if err := os.Rename(stageDir, jobsDir); err != nil {
			if jobsExist {
				if rollbackErr := os.Rename(backupDir, jobsDir); rollbackErr != nil {
					return errors.Join(fmt.Errorf("install staged Jobs dir: %w", err), fmt.Errorf("restore previous Jobs dir: %w", rollbackErr))
				}
			}
			return fmt.Errorf("failed to install staged Jobs dir: %w", err)
		}

		if err := saveCurrentCommit(newCommit); err != nil {
			return fmt.Errorf("failed to save current commit: %w", err)
		}
		if jobsExist {
			if err := os.RemoveAll(backupDir); err != nil {
				return fmt.Errorf("failed to remove previous Jobs dir: %w", err)
			}
		}

		select {
		case trigChan <- struct{}{}:
		default:
		}
	}
	return nil
}

func getCurrentCommit() (string, error) {
	data, err := os.ReadFile(".commit")
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func saveCurrentCommit(hash string) error {
	return os.WriteFile(".commit", []byte(hash), 0644)
}
