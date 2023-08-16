package pgrepl

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
)

// Conn adds more methods to pgx.Conn.
type Conn struct {
	*pgx.Conn
}

// FetchPublicationTables fetches all tables that needs replication from publications.
func (c *Conn) FetchPublicationTables(ctx context.Context) ([]string, error) {
	// TODO: adapt query for CREATE PUBLICATION pub FOR ALL TABLES; case.
	rows, err := c.Query(ctx,
		`SELECT 
			schemaname, tablename 
		FROM pg_publication p
		JOIN pg_publication_tables pt ON p.pubname = pt.pubname
	`)
	if errors.Is(err, pgx.ErrNoRows) {
		return []string{}, nil
	} else if err != nil {
		return []string{}, fmt.Errorf("query: %s", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var schema, table string
		if err := rows.Scan(&schema, &table); err != nil {
			return []string{}, fmt.Errorf("scan: %s", err)
		}
		tables = append(tables, fmt.Sprintf("%s.%s", schema, table))
	}

	return tables, nil
}

// ConfirmedFlushLSN fetches the confirmed flush LSN.
func (c *Conn) ConfirmedFlushLSN(ctx context.Context, slot string) (pglogrepl.LSN, error) {
	var lsn pglogrepl.LSN
	if err := c.QueryRow(
		ctx,
		"SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1", slot,
	).Scan(&lsn); err != nil {
		return 0, fmt.Errorf("query row: %w", err)
	}
	return lsn, nil
}
