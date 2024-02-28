package app

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"

	"github.com/filecoin-project/lassie/pkg/lassie"
	"github.com/filecoin-project/lassie/pkg/storage"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
	carstorage "github.com/ipld/go-car/v2/storage"
	"github.com/ipld/go-car/v2/storage/deferred"
	trustlessutils "github.com/ipld/go-trustless-utils"
)

type retriever interface {
	retrieveStdout(context.Context, cid.Cid, int64) error
	retrieveFile(context.Context, cid.Cid, string, int64) error
}

// Retriever is responsible for retrieving file from the network.
type Retriever struct {
	store   retriever
	timeout int64
}

// NewRetriever creates a new Retriever.
func NewRetriever(provider VaultsProvider, timeout int64) *Retriever {
	return &Retriever{
		store: &coldStore{
			retriever: &cacheStore{
				provider: provider,
			},
		},
		timeout: timeout,
	}
}

// Retrieve retrieves file from the network.
func (r *Retriever) Retrieve(ctx context.Context, c cid.Cid, output string) error {
	if output == "-" || output == "" {
		return r.store.retrieveStdout(ctx, c, r.timeout)
	}

	return r.store.retrieveFile(ctx, c, output, r.timeout)
}

type cacheStore struct {
	provider VaultsProvider
}

func (cs *cacheStore) retrieveStdout(ctx context.Context, cid cid.Cid, timeout int64) error {
	if _, err := cs.provider.RetrieveEvent(ctx, RetrieveEventParams{
		Timeout: timeout,
		CID:     cid,
	}, os.Stdout); err != nil {
		return fmt.Errorf("failed to retrieve to file: %s", err)
	}

	return nil
}

func (cs *cacheStore) retrieveFile(ctx context.Context, cid cid.Cid, output string, timeout int64) error {
	f, err := os.OpenFile(output, os.O_RDWR|os.O_CREATE, 0o666)
	if err != nil {
		return fmt.Errorf("failed to open tmp file: %s", err)
	}
	defer func() {
		_ = f.Close()
	}()

	_, _ = f.Seek(0, io.SeekStart)
	_, err = cs.provider.RetrieveEvent(ctx, RetrieveEventParams{
		Timeout: timeout,
		CID:     cid,
	}, f)
	if err != nil {
		return fmt.Errorf("failed to retrieve to file: %s", err)
	}

	return nil
}

type coldStore struct {
	retriever retriever
}

func (cs *coldStore) retrieveFile(ctx context.Context, c cid.Cid, output string, timeout int64) error {
	// try cache first. no matter the error try cold store
	err := cs.retriever.retrieveFile(ctx, c, output, timeout)
	if err == nil {
		return nil
	}

	lassie, err := lassie.NewLassie(ctx)
	if err != nil {
		return fmt.Errorf("failed to create lassie instance: %s", err)
	}

	carOpts := []car.Option{
		car.WriteAsCarV1(true),
		car.StoreIdentityCIDs(false),
		car.UseWholeCIDs(false),
	}

	carPath := path.Join(".", fmt.Sprintf("%s.car", c.String()))
	carWriter := deferred.NewDeferredCarWriterForPath(carPath, []cid.Cid{c}, carOpts...)

	carStore := storage.NewCachingTempStore(
		carWriter.BlockWriteOpener(), storage.NewDeferredStorageCar(os.TempDir(), c),
	)
	defer func() {
		_ = carStore.Close()
	}()

	request, err := types.NewRequestForPath(carStore, c, "", trustlessutils.DagScopeAll, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %s", err)
	}

	if _, err := lassie.Fetch(ctx, request, []types.FetchOption{}...); err != nil {
		return fmt.Errorf("failed to fetch: %s", err)
	}

	carFile, err := os.Open(carPath)
	if err != nil {
		return fmt.Errorf("opening car file: %s", err)
	}
	defer func() {
		_ = os.Remove(carFile.Name())
		_ = carFile.Close()
	}()

	rc, err := extract(carFile)
	if err != nil {
		return fmt.Errorf("extract: %s", err)
	}

	f, err := os.OpenFile(output, os.O_RDWR|os.O_CREATE, 0o666)
	if err != nil {
		return fmt.Errorf("failed to open tmp file: %s", err)
	}
	defer func() {
		_ = f.Close()
	}()

	if _, err := io.Copy(f, rc); err != nil {
		return fmt.Errorf("failed to write to stdout: %s", err)
	}

	return nil
}

func (cs *coldStore) retrieveStdout(ctx context.Context, c cid.Cid, timeout int64) error {
	// try cache first. no matter the error try cold store
	err := cs.retriever.retrieveStdout(ctx, c, timeout)
	if err == nil {
		return nil
	}

	lassie, err := lassie.NewLassie(ctx)
	if err != nil {
		return fmt.Errorf("failed to create lassie instance: %s", err)
	}

	carOpts := []car.Option{
		car.WriteAsCarV1(true),
		car.StoreIdentityCIDs(false),
		car.UseWholeCIDs(false),
	}

	// Create a temporary file only for writing to stdout case
	tmpFile, err := os.CreateTemp("", fmt.Sprintf("%s.car", c.String()))
	if err != nil {
		return fmt.Errorf("failed to create temporary file: %s", err)
	}
	defer func() {
		_ = os.Remove(tmpFile.Name())
	}()
	carWriter := deferred.NewDeferredCarWriterForPath(tmpFile.Name(), []cid.Cid{c}, carOpts...)

	carStore := storage.NewCachingTempStore(
		carWriter.BlockWriteOpener(), storage.NewDeferredStorageCar(os.TempDir(), c),
	)
	defer func() {
		_ = carStore.Close()
	}()

	request, err := types.NewRequestForPath(carStore, c, "", trustlessutils.DagScopeAll, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %s", err)
	}

	if _, err := lassie.Fetch(ctx, request, []types.FetchOption{}...); err != nil {
		return fmt.Errorf("failed to fetch: %s", err)
	}

	_, _ = tmpFile.Seek(0, io.SeekStart)
	rc, err := extract(tmpFile)
	if err != nil {
		return fmt.Errorf("extract: %s", err)
	}

	_, err = io.Copy(os.Stdout, rc)
	if err != nil {
		return fmt.Errorf("failed to write to stdout: %s", err)
	}

	return nil
}

func extract(f *os.File) (io.ReadCloser, error) {
	store, err := carstorage.OpenReadable(f)
	if err != nil {
		return nil, err
	}

	blkCid, err := cid.Parse(store.Roots()[0].String())
	if err != nil {
		return nil, err
	}

	rc, err := store.GetStream(context.Background(), blkCid.KeyString())
	if err != nil {
		return nil, err
	}

	return rc, nil
}
