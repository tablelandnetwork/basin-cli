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
	if output == "-" {
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
	// Write to the provided path or current directory
	if output == "" {
		output = "." // Default to current directory
	}
	// Ensure path is a valid directory
	info, err := os.Stat(output)
	if err != nil {
		return fmt.Errorf("failed to access output directory: %s", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("output path is not a directory: %s", output)
	}

	f, err := os.OpenFile(path.Join(output, cid.String()), os.O_RDWR|os.O_CREATE, 0o666)
	if err != nil {
		return fmt.Errorf("failed to open tmp file: %s", err)
	}
	defer func() {
		_ = os.Remove(f.Name())
		_ = f.Close()
	}()

	_, _ = f.Seek(0, io.SeekStart)

	filename, err := cs.provider.RetrieveEvent(ctx, RetrieveEventParams{
		Timeout: timeout,
		CID:     cid,
	}, f)
	if err != nil {
		return fmt.Errorf("failed to retrieve to file: %s", err)
	}

	if err := os.Rename(f.Name(), path.Join(output, fmt.Sprintf("%s-%s", cid.String(), filename))); err != nil {
		return fmt.Errorf("failed renaming the file: %s", err)
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

	if output == "" {
		output = "." // Default to current directory
	}
	// Ensure path is a valid directory
	info, err := os.Stat(output)
	if err != nil {
		return fmt.Errorf("failed to access output directory: %s", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("output path is not a directory: %s", output)
	}
	carPath := path.Join(output, fmt.Sprintf("%s.car", c.String()))
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
	_, err = io.Copy(os.Stdout, tmpFile)
	if err != nil {
		return fmt.Errorf("failed to write to stdout: %s", err)
	}

	return nil
}
