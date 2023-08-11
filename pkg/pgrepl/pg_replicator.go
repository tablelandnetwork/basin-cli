package pgrepl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

const (
	outputPlugin = "wal2json"
)

// PgReplicator is a component that replicates Postgres data.
type PgReplicator struct {
	conn *pgconn.PgConn
	feed chan Wal

	slotName string

	lastApplied pglogrepl.LSN

	// wal log pointer
	timeline int32
	xlogpos  pglogrepl.LSN

	// sync
	once sync.Once
}

// New creates a new Postgres replicator.
func New(connStr string, slotName string) (*PgReplicator, error) {
	ctx := context.Background()

	r := &PgReplicator{}
	r.feed = make(chan Wal)
	r.slotName = slotName

	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		return nil, fmt.Errorf("connect: %s", err)
	}
	defer func() {
		if err := conn.Close(ctx); err != nil {
			log.Println(err)
		}
	}()

	replConn, err := pgx.Connect(ctx, connStr+"?replication=database")
	if err != nil {
		return nil, fmt.Errorf("connect: %s", err)
	}

	r.conn = replConn.PgConn()

	// test connection
	if err := conn.Ping(ctx); err != nil {
		return nil, err
	}

	var lsn pglogrepl.LSN
	if err := conn.QueryRow(
		context.Background(),
		"SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1", slotName,
	).Scan(&lsn); err != nil {
		// if no replication slot was found we create one
		if errors.Is(err, pgx.ErrNoRows) {
			_, err = pglogrepl.CreateReplicationSlot(
				context.Background(), r.conn, slotName, outputPlugin, pglogrepl.CreateReplicationSlotOptions{Temporary: false},
			)
			if err != nil {
				return nil, fmt.Errorf("failed to create replication slot %s", err)
			}

			sysident, err := pglogrepl.IdentifySystem(context.Background(), r.conn)
			if err != nil {
				return nil, err
			}

			r.xlogpos = sysident.XLogPos
			r.lastApplied = r.xlogpos

			return r, nil
		}

		return nil, fmt.Errorf("failed to query pg_replication_slots: %s", err)
	}

	r.xlogpos = lsn
	r.lastApplied = r.xlogpos

	return r, nil
}

// StartReplication starts replicattion.
func (r *PgReplicator) StartReplication() (chan Wal, error) {
	if err := pglogrepl.StartReplication(
		context.Background(),
		r.conn,
		r.slotName,
		r.xlogpos,
		pglogrepl.StartReplicationOptions{PluginArgs: []string{
			"\"pretty-print\" 'true'",
			"\"include-timestamp\" 'true'",
			"\"format-version\" '2'",
		}}); err != nil {
		return nil, err
	}
	log.Println("Logical replication started on slot", r.slotName)

	go func() {
		nextStandbyMessageDeadline := time.Now().Add(time.Second * 10)
		for {
			if time.Now().After(nextStandbyMessageDeadline) {
				if err := pglogrepl.SendStandbyStatusUpdate(
					context.Background(),
					r.conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: r.lastApplied},
				); err != nil {
					return
				}
				log.Printf("Sent Standby status message at %s\n", r.lastApplied.String())
				nextStandbyMessageDeadline = time.Now().Add(time.Second * 10)
			}

			ctx, cancel := context.WithDeadline(context.Background(), nextStandbyMessageDeadline)
			rawMsg, err := r.conn.ReceiveMessage(ctx)
			cancel()
			if err != nil {
				if pgconn.Timeout(err) {
					continue
				}
			}

			if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
				log.Printf("received Postgres WAL error: %+v\n", errMsg)
			}

			msg, ok := rawMsg.(*pgproto3.CopyData)
			if !ok {
				if msg != nil {
					log.Printf("Received unexpected message: %T\n", rawMsg)
				}
				continue
			}

			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					log.Printf("ParsePrimaryKeepaliveMessage failed: %s\n", err)
				}

				if pkm.ReplyRequested {
					nextStandbyMessageDeadline = time.Time{}
				}
			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					log.Printf("ParseXLogData failed: %s\n", err)
				}
				var w JSONWal
				if err := json.Unmarshal(xld.WALData, &w); err != nil {
					log.Printf("unmarshal: %s\n", err)
				}

				r.feed <- Wal{
					Pos:      r.xlogpos,
					Timeline: r.timeline,
					Payload:  w,
				}

				r.xlogpos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
			}
		}
	}()

	return r.feed, nil
}

// SendFeedback send a signal to Postgres that the lsn was consumed.
func (r *PgReplicator) SendFeedback(xlogpos pglogrepl.LSN) error {
	if err := pglogrepl.SendStandbyStatusUpdate(
		context.Background(), r.conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: xlogpos},
	); err != nil {
		return fmt.Errorf("send update: %s", err)
	}

	r.lastApplied = xlogpos

	return nil
}

// Shutdown stops the replication by closing the Postgres connection and the feed channel.
func (r *PgReplicator) Shutdown(ctx context.Context) error {
	r.once.Do(func() {
		if err := r.conn.Close(ctx); err != nil {
			log.Printf("close connection: %s\n", err)
		}

		close(r.feed)
	})

	return nil
}
