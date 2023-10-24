package app

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"os"
	"path"
	"regexp"
	"strings"
	"time"

	"golang.org/x/exp/slog"
)

// UploadManager is a stateful wrapper around BasinUploader. It periodically
// checks for new db dumps and uploads them to the provider using BasinUploader.
type UploadManager struct {
	ctx      context.Context
	cancel   context.CancelFunc
	dbDir    string
	table    string
	interval time.Duration
	uploader *BasinUploader
}

// NewUploadManager creates new UploadManager.
func NewUploadManager(
	ctx context.Context,
	dbDir string,
	string string,
	uploader *BasinUploader,
	interval time.Duration,
) *UploadManager {
	ctx, cancel := context.WithCancel(ctx)
	return &UploadManager{
		uploader: uploader,
		dbDir:    dbDir,
		table:    string,
		interval: interval,
		ctx:      ctx,
		cancel:   cancel,
	}
}

// Start starts the upload manager. It will periodically call Upload method.
func (u *UploadManager) Start() {
	slog.Info("uploader is starting with", "interval", u.interval)
	ticker := time.NewTicker(u.interval)
	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				slog.Info("uploader is stopping")
				return
			case <-ticker.C:
				// Upload dumps that match <timestamp>.db pattern.
				// Skip the current.db file.
				if err := u.Upload(`^\d+\.db$`); err != nil {
					slog.Error("upload error", "err", err)
					continue
				}
			}
		}
	}(u.ctx)
}

// Stop stops the upload manager.
func (u *UploadManager) Stop() {
	u.cancel()
}

func (u *UploadManager) export(f fs.DirEntry) (string, error) {
	expPath := path.Join(u.dbDir, fmt.Sprintf("%s.parquet", f.Name()))

	slog.Info("backing up db dump", "at", f.Name(), "to", expPath)
	dbPath := path.Join(u.dbDir, f.Name())
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return "", err
	}
	defer db.Close()

	expQuery := fmt.Sprintf(
		`INSTALL parquet; LOAD parquet;
		COPY (SELECT * FROM %s) TO '%s' (FORMAT PARQUET)`,
		u.table, expPath)

	_, err = db.ExecContext(context.Background(), expQuery)
	if err != nil {
		return "", fmt.Errorf("cannot export to parquet file: %s", err)
	}

	return expPath, nil
}

func (u *UploadManager) deleteDBFile(f fs.DirEntry) error {
	dbPath := path.Join(u.dbDir, f.Name())
	slog.Info("deleting db dump", "at", dbPath)
	if err := os.Remove(dbPath); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("cannot delete dump file: %s", err)
	}
	return nil
}

func (u *UploadManager) deleteWALFile(f fs.DirEntry) error {
	walPath := path.Join(u.dbDir, fmt.Sprintf("%s.wal", f.Name()))
	slog.Info("deleting db WAL file", "at", walPath)
	if err := os.Remove(walPath); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("cannot delete wal file: %s", err)
	}

	return nil
}

func (u *UploadManager) deleteParquetFile(f fs.DirEntry) error {
	parquetPath := path.Join(u.dbDir, fmt.Sprintf("%s.parquet", f.Name()))
	slog.Info("deleting db parquet export", "at", parquetPath)
	if err := os.Remove(parquetPath); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("cannot delete parquet file: %s", err)
	}

	return nil
}

func (u *UploadManager) cleanup(f fs.DirEntry) error {
	if err := u.deleteDBFile(f); err != nil {
		return err
	}

	if err := u.deleteWALFile(f); err != nil {
		return err
	}

	if err := u.deleteParquetFile(f); err != nil {
		return err
	}

	return nil
}

// Upload uploads all db dumps that match the filterReg.
// It returns an error if any of the dumps cannot be uploaded.
// FilterReg is a regular expression that matches file names
// to be uploaded.
func (u *UploadManager) Upload(pattern string) error {
	// this regex will not match current.db
	re := regexp.MustCompile(pattern)
	slog.Info("finding db dumps to upload", "pattern", pattern)

	files, err := os.ReadDir(u.dbDir)
	if err != nil {
		return fmt.Errorf("cannot read dir: %s", err)
	}

	for _, f := range files {
		fname := f.Name()
		if re.MatchString(fname) {
			exportPath, err := u.export(f)
			if err != nil {
				// ignore the error if the table does not exist in _this_ db
				// it may have happened the upload was trigger by a shutdown
				// instead of a regular upload cycle
				msg := fmt.Sprintf("Table with name %s does not exist", u.table)
				if strings.Contains(err.Error(), msg) {
					slog.Info("attempt to upload empty dump", "table", u.table)
					// delete the db file and continue
					// there won't be any WAL file becasue the db is empty
					if err := u.deleteDBFile(f); err != nil {
						return err
					}
					continue
				}

				return fmt.Errorf("export: %s", err)
			}

			if err := u.uploader.Upload(u.ctx, exportPath, nil); err != nil {
				return fmt.Errorf("upload: %s", err)
			}

			if err := u.cleanup(f); err != nil {
				return fmt.Errorf("cleanup: %s", err)
			}
		}
	}

	return nil
}
