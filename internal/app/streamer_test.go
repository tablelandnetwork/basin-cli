package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/require"

	basincapnp "github.com/tablelandnetwork/basin-cli/pkg/capnp"
	"github.com/tablelandnetwork/basin-cli/pkg/pgrepl"
)

const ex1 = `{
	"commit_lsn":957398296,
	"records":[
		{
			"action":"I",
			"xid":1058,
			"lsn":"0/3910B898",
			"nextlsn":"",
			"timestamp":"2023-08-22 14:44:02.043586-03",
			"schema":"public",
			"table":"t",
			"columns":[
				{"name":"id","type":"integer","value":200232},
				{"name":"name","type":"text","value":"100"}
			],
			"pk":[{"name":"id","type":"integer"}]
		}
	]
}`
const ex2 = ` {
	"commit_lsn":957398297,
	"records":[
		{
			"action":"I",
			"xid":1059,
			"lsn":"0/3910B899",
			"nextlsn":"",
			"timestamp":"2023-08-22 14:45:02.043586-03",
			"schema":"public",
			"table":"t",
			"columns":[
				{"name":"id","type":"integer","value":200233},
				{"name":"name","type":"text","value":"200"}
			],
			"pk":[{"name":"id","type":"integer"}]
		}
	]
}
`

// recvWAL reads one line from the reader and unmarshals it into a transaction.
func recvWAL(t *testing.T, json_in string, feed chan *pgrepl.Tx) {
	var tx pgrepl.Tx
	require.NoError(t, json.Unmarshal([]byte(json_in), &tx))
	feed <- &tx
}

type testRow struct {
	id   int
	name string
}

func importDuckDB(t *testing.T, file *os.File) *sql.Rows {
	db, err := sql.Open("duckdb", path.Join(t.TempDir(), "temp.db"))
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec("INSTALL parquet; LOAD parquet;")
	if err != nil {
		t.Fatal(err)
	}

	parquetQuery := fmt.Sprintf(
		"SELECT * FROM read_parquet('%s')", file.Name())
	rows, err := db.Query(parquetQuery)
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func queryResult(t *testing.T, rows *sql.Rows) (result []testRow) {
	var id int
	var name string
	for rows.Next() {
		require.NoError(t, rows.Scan(&id, &name))
		row := testRow{
			id:   id,
			name: name,
		}
		result = append(result, row)
	}

	return result
}

const (
	pk        = "f81ab2709b7cf1f2ebbbd50bd730b267879a495318f7aac16bbe7caa8a8f2d8d"
	testTable = "t"
	testNS    = "public"
)

var cols = []Column{
	{Name: "id", Typ: "integer", IsNull: false, IsPrimary: true},
	{Name: "name", Typ: "text", IsNull: false, IsPrimary: false},
}

// <T1, R, T2, U, C>
func TestBasinStreamerOne(t *testing.T) {
	// used for testing
	privateKey, err := crypto.HexToECDSA(pk)
	require.NoError(t, err)

	// This chan will receive the wal records from the replicator
	feed := make(chan *pgrepl.Tx)
	testDBDir := t.TempDir()
	uploadInterval := 6 * time.Second
	replaceThreshold := 3 * time.Second
	dbm := NewDBManager(
		testDBDir, testTable, cols, replaceThreshold)
	providerMock := &basinProviderMock{
		owner:          make(map[string]string),
		uploaderInputs: make(chan *os.File),
	}
	uploader := NewBasinUploader(testNS, testTable, providerMock, privateKey)
	upm := NewUploadManager(
		context.Background(), testDBDir, testTable, uploader, uploadInterval,
	)
	// starts upload manager in a separate goroutine
	upm.Start()
	streamer := NewBasinStreamer(testNS, &replicatorMock{feed: feed}, dbm)
	go func() {
		// start listening to WAL records in a separate goroutine
		err = streamer.Run(context.Background())
		require.NoError(t, err)
	}()

	// 1. receive first tx
	recvWAL(t, ex1, feed)

	// 2. sleep for replaceThreshold time and receive next message
	//    to trigger db replacement
	time.Sleep(replaceThreshold)
	recvWAL(t, ex2, feed)

	// wait for the first upload to kick in
	time.Sleep(uploadInterval - replaceThreshold)

	// Assert that the first tx was replayed by importing the
	// exported parquet file
	file := <-providerMock.uploaderInputs
	rows := importDuckDB(t, file)
	result := queryResult(t, rows)
	require.Equal(t, 1, len(result))
	require.Equal(t, 200232, result[0].id)
	require.Equal(t, "100", result[0].name)

	// Manually trigger uplaod of current.db like it would happen
	// when the replication process starts again.
	go func() {
		upm.Upload(`^current.db$`)
	}()

	// Assert that the second tx was replayed and uploaded by importing the
	// exported parquet file.
	file = <-providerMock.uploaderInputs
	rows = importDuckDB(t, file)
	result = queryResult(t, rows)
	require.Equal(t, 1, len(result))
	require.Equal(t, 200233, result[0].id)
	require.Equal(t, "200", result[0].name)
}

// <T1, T2, R, U, C>
func TestBasinStreamerTwo(t *testing.T) {
	privateKey, err := crypto.HexToECDSA(pk)
	require.NoError(t, err)

	// This chan will receive the wal records from the replicator
	feed := make(chan *pgrepl.Tx)
	testDBDir := t.TempDir()
	uploadInterval := 4 * time.Second
	replaceThreshold := 3 * time.Second
	dbm := NewDBManager(
		testDBDir, testTable, cols, replaceThreshold)
	providerMock := &basinProviderMock{
		owner:          make(map[string]string),
		uploaderInputs: make(chan *os.File),
	}
	uploader := NewBasinUploader(testNS, testTable, providerMock, privateKey)
	upm := NewUploadManager(
		context.Background(), testDBDir, testTable, uploader, uploadInterval,
	)
	// starts upload manager in a separate goroutine
	upm.Start()
	streamer := NewBasinStreamer(testNS, &replicatorMock{feed: feed}, dbm)
	go func() {
		// start listening to WAL records in a separate goroutine
		err = streamer.Run(context.Background())
		require.NoError(t, err)
	}()

	// 1. receive first tx
	recvWAL(t, ex1, feed)

	// 2. receive second tx
	recvWAL(t, ex2, feed)

	// wait for the first upload to kick in
	time.Sleep(uploadInterval)

	// nothing should be uploaded because the second tx was received before
	// the replaceThreshold was reached. the current.db should be uploaded
	// when we shutdown the uploader
	select {
	case <-providerMock.uploaderInputs:
		t.FailNow() // should not be reached
	default:
		// manually trigger upload of current.db like
		// it happens when the replication process starts again.
		go func() {
			upm.Upload(`^current.db$`)
		}()
	}

	// Assert that the both first and second tx
	// were replayed by importing the exported parquet file
	file := <-providerMock.uploaderInputs
	rows := importDuckDB(t, file)
	result := queryResult(t, rows)
	require.Equal(t, 2, len(result))
	require.Equal(t, 200232, result[0].id)
	require.Equal(t, "100", result[0].name)
	require.Equal(t, 200233, result[1].id)
	require.Equal(t, "200", result[1].name)
}

// <T1, R, T2, C, U> (opposite of 1st case)
func TestBasinStreamerThree(t *testing.T) {
	privateKey, err := crypto.HexToECDSA(pk)
	require.NoError(t, err)

	// This chan will receive the wal records from the replicator
	feed := make(chan *pgrepl.Tx)
	testDBDir := t.TempDir()
	uploadInterval := 8 * time.Second
	replaceThreshold := 3 * time.Second
	dbm := NewDBManager(
		testDBDir, testTable, cols, replaceThreshold)
	providerMock := &basinProviderMock{
		owner:          make(map[string]string),
		uploaderInputs: make(chan *os.File),
	}
	uploader := NewBasinUploader(testNS, testTable, providerMock, privateKey)
	upm := NewUploadManager(
		context.Background(), testDBDir, testTable, uploader, uploadInterval,
	)
	// starts upload manager in a separate goroutine
	upm.Start()
	streamer := NewBasinStreamer(testNS, &replicatorMock{feed: feed}, dbm)
	go func() {
		// start listening to WAL records in a separate goroutine
		err = streamer.Run(context.Background())
		require.NoError(t, err)
	}()

	// 1. receive first tx
	recvWAL(t, ex1, feed)

	// wait for the first upload to kick in
	time.Sleep(replaceThreshold + 1)

	// 2. receive second tx
	recvWAL(t, ex2, feed)

	// shutdown uploader while the second tx is being replayed
	go func() {
		upm.Stop()
		NewUploadManager(
			context.Background(), testDBDir, testTable, uploader, uploadInterval,
		).Start()
	}()

	file := <-providerMock.uploaderInputs
	rows := importDuckDB(t, file)
	result := queryResult(t, rows)
	require.Equal(t, 1, len(result))
	require.Equal(t, 200232, result[0].id)
	require.Equal(t, "100", result[0].name)

	// When replication start again, the existing current.db will be uploaded
	go func() {
		upm = NewUploadManager(
			context.Background(), testDBDir, testTable, uploader, uploadInterval,
		)
		require.NoError(t, upm.Upload(`^current.db$`))
	}()
	file = <-providerMock.uploaderInputs
	rows = importDuckDB(t, file)
	result = queryResult(t, rows)
	require.Equal(t, 1, len(result))
	require.Equal(t, 200233, result[0].id)
	require.Equal(t, "200", result[0].name)

}

type replicatorMock struct {
	feed chan *pgrepl.Tx
}

var _ Replicator = (*replicatorMock)(nil)

func (rm *replicatorMock) StartReplication(_ context.Context) (chan *pgrepl.Tx, string, error) {
	return rm.feed, "", nil
}

func (rm *replicatorMock) Commit(_ context.Context, _ pglogrepl.LSN) error {
	return nil
}

func (rm *replicatorMock) Shutdown() {
	close(rm.feed)
}

type basinProviderMock struct {
	owner          map[string]string
	uploaderInputs chan *os.File
}

func (bp *basinProviderMock) Create(
	_ context.Context, ns string, _ string, _ basincapnp.Schema, owner common.Address,
) (bool, error) {
	bp.owner[ns] = owner.Hex()
	return false, nil
}

func (bp *basinProviderMock) Reconnect() error {
	return nil
}

func (bp *basinProviderMock) Upload(
	ctx context.Context, ns string, rel string, size uint64, f io.Reader, signer *Signer, progress io.Writer,
) error {
	file := f.(*os.File)
	file.Fd()

	// re-create a copy of the file for assertions
	// because the original file will be deleted by the uploader
	newFile, err := os.Create(file.Name() + ".copy")
	if err != nil {
		return err
	}
	_, err = io.Copy(newFile, file)
	if err != nil {
		return err
	}

	err = newFile.Sync() // flush to disk
	if err != nil {
		return err
	}

	bp.uploaderInputs <- newFile
	return nil
}
