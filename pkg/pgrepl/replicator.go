package pgrepl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"golang.org/x/exp/slog"
)

const (
	// The logical decoder we're using.
	// https://github.com/eulerto/wal2json
	outputPlugin = "wal2json"
)

// Publication is the name a publication.
// Currently it corresponds to a table's name.
type Publication string

// FullName is the name used to create a publication in Postgres.
func (p Publication) FullName() string {
	return fmt.Sprintf("pub_basin_%s", p)
}

// PgReplicator is a component that replicates Postgres data.
type PgReplicator struct {
	slot   string
	pgConn *pgconn.PgConn

	// channel of replicated Txs.
	feed chan *Tx

	// The tables that will be replicated.
	// We get them by querying pg_publication.
	tables []string

	// The commitLSN is the LSN used to start the replication.
	// It either comes from the confirmed_flush_lsn of an existing replication slot
	// or a recently created replication slot.
	commitLSN pglogrepl.LSN

	// The committedLSN is the last committed LSN, updated by the Commit method
	// and used in the KeepAlive message.
	committedLSN pglogrepl.LSN

	// Sync to help synchronize the Commit method and the KeepAlive access to the committedLSN.
	commitSync sync.Mutex

	closeOnce sync.Once
}

// New creates a new Postgres replicator.
func New(connStr string, publication Publication) (*PgReplicator, error) {
	ctx := context.Background()

	config, err := pgconn.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("parse config: %s", err)
	}

	r := &PgReplicator{}
	r.feed = make(chan *Tx)
	r.slot = fmt.Sprintf("basin_%s", publication)

	// Connect to the database
	pgxConn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		return nil, fmt.Errorf("connect: %s", err)
	}
	conn := &Conn{pgxConn}
	defer func() {
		if err := conn.Close(ctx); err != nil {
			slog.Error("failed to close connection", "error", err)
		}
	}()

	// Get a connection with replication flag.
	// This is the connection that will be used for now on.
	config.RuntimeParams["replication"] = "database"
	r.pgConn, err = pgconn.ConnectConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("connect: %s", err)
	}

	// Test connection to the database.
	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping: %s", err)
	}

	// Check if publication exists
	table, err := conn.GetPublicationTable(ctx, publication)
	if err != nil {
		return nil, err
	}
	r.tables = []string{table}

	// Fetch the confirmed flush lsn.
	lsn, err := conn.ConfirmedFlushLSN(ctx, r.slot)

	// If no replication slot was found we create one.
	if errors.Is(err, pgx.ErrNoRows) {
		result, err := pglogrepl.CreateReplicationSlot(
			context.Background(), r.pgConn, r.slot, outputPlugin, pglogrepl.CreateReplicationSlotOptions{
				Temporary:      false,
				SnapshotAction: "NOEXPORT_SNAPSHOT",
			},
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create replication slot: %s", err)
		}

		var commitLSN pglogrepl.LSN
		if err := commitLSN.Scan(result.ConsistentPoint); err != nil {
			return nil, fmt.Errorf("failed to scan lsn: %s", err)
		}
		r.commitLSN = commitLSN
		return r, nil
	} else if err != nil {
		return nil, fmt.Errorf("failed to fetch confirmed flush lsn: %s", err)
	}

	r.commitLSN = lsn

	return r, nil
}

// StartReplication starts replicattion.
func (r *PgReplicator) StartReplication(ctx context.Context) (chan *Tx, error) {
	if err := pglogrepl.StartReplication(
		ctx,
		r.pgConn,
		r.slot,
		r.commitLSN,

		// Plugin options for wal2json.
		// Check https://github.com/eulerto/wal2json for more options.
		pglogrepl.StartReplicationOptions{PluginArgs: []string{
			"\"pretty-print\" 'false'",
			"\"include-transaction\" 'true'",
			"\"include-lsn\" 'true'",
			"\"include-timestamp\" 'true'",
			"\"include-pk\" 'true'",
			"\"format-version\" '2'",
			"\"include-xids\" 'true'",
			fmt.Sprintf("\"add-tables\" '%s'", strings.Join(r.tables, ",")),
		}}); err != nil {
		return nil, err
	}
	slog.Info("Logical replication started", "slot", r.slot)

	go func() {
		records := []Record{}
		var commitLSN string

		// Consume all records between BEGIN and COMMIT inside a Transaction
		for {
			record, err := r.consumeRecord(ctx)
			if err != nil {
				slog.Error("consume record", "error", err)
				continue
			}

			// Empty records that came from KeepAlive messages
			if record.Action == "" {
				continue
			}

			// BEGIN
			if record.Action == "B" {
				commitLSN = record.EndLsn
				continue
			}

			// COMMIT
			if record.Action == "C" {
				// commit and begin end_lsn should match
				if record.EndLsn != commitLSN {
					slog.Error("commit and begin end_lsn don't match", "commit_lsn", commitLSN, "end_lsn", record.EndLsn)
					continue
				}

				var lsn pglogrepl.LSN
				_ = lsn.Scan(commitLSN)

				if len(records) > 0 {
					r.feed <- &Tx{
						CommitLSN: lsn,
						Records:   records,
					}
				}

				records = []Record{}
				commitLSN = ""
				continue
			}

			records = append(records, record)
		}
	}()

	return r.feed, nil
}

// Commit send a signal to Postgres that the lsn was consumed.
func (r *PgReplicator) Commit(ctx context.Context, lsn pglogrepl.LSN) error {
	r.commitSync.Lock()
	defer r.commitSync.Unlock()

	if err := pglogrepl.SendStandbyStatusUpdate(
		ctx, r.pgConn, pglogrepl.StandbyStatusUpdate{WALWritePosition: lsn},
	); err != nil {
		return fmt.Errorf("send status update: %s", err)
	}

	r.committedLSN = lsn

	return nil
}

// Shutdown stops the replication by closing the Postgres connection and the feed channel.
func (r *PgReplicator) Shutdown() {
	r.closeOnce.Do(func() {
		close(r.feed)
	})
}

func (r *PgReplicator) consumeRecord(ctx context.Context) (Record, error) {
	rawMsg, err := r.pgConn.ReceiveMessage(ctx)
	if err != nil {
		if pgconn.Timeout(err) {
			return Record{}, fmt.Errorf("timeout: %s", err)
		}
	}

	if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
		return Record{}, fmt.Errorf("received Postgres WAL error: %s", errMsg.Code)
	}

	msg, ok := rawMsg.(*pgproto3.CopyData)
	if !ok {
		if msg != nil {
			slog.Error("unexpected message: %s\n", rawMsg)
		}
		return Record{}, nil
	}

	switch msg.Data[0] {
	case pglogrepl.PrimaryKeepaliveMessageByteID:
		pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
		if err != nil {
			return Record{}, fmt.Errorf("ParsePrimaryKeepaliveMessage failed: %s", err)
		}

		if pkm.ReplyRequested {
			slog.Info("primary keep alive reply requested")

			if err := r.sendStandbyStatusUpdate(ctx); err != nil {
				return Record{}, err
			}
		}
	case pglogrepl.XLogDataByteID:
		xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
		if err != nil {
			return Record{}, fmt.Errorf("ParseXLogData failed: %s", err)
		}

		var r Record
		if err := json.Unmarshal(xld.WALData, &r); err != nil {
			return Record{}, fmt.Errorf("unmarshal: %s", err)
		}

		return r, nil
	}

	return Record{}, nil
}

func (r *PgReplicator) sendStandbyStatusUpdate(ctx context.Context) error {
	r.commitSync.Lock()
	defer r.commitSync.Unlock()

	if err := pglogrepl.SendStandbyStatusUpdate(
		ctx, r.pgConn, pglogrepl.StandbyStatusUpdate{WALWritePosition: r.committedLSN},
	); err != nil {
		return fmt.Errorf("SendStandbyStatusUpdate failed: %s", err)
	}
	return nil
}
