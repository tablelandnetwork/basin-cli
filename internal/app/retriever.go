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

// Retriever is responsible for retrieving file from the network.
type Retriever struct {
	cacheStore *cacheStore
	coldStore  *coldStore // nolint
}

// NewRetriever creates a new Retriever.
func NewRetriever(provider VaultsProvider) *Retriever {
	return &Retriever{
		cacheStore: &cacheStore{
			provider: provider,
		},
	}
}

// Retrieve retrieves file from the network.
func (r *Retriever) Retrieve(ctx context.Context, c cid.Cid, output string, name string) error {
	if output == "-" {
		return r.cacheStore.retrieveStdout(ctx, c)
	}

	return r.cacheStore.retrieveFile(ctx, c, output, name)
}

type cacheStore struct {
	provider VaultsProvider
}

func (cs *cacheStore) retrieveStdout(ctx context.Context, cid cid.Cid) error {
	if err := cs.provider.RetrieveEvent(ctx, RetrieveEventParams{
		CID: cid,
	}, os.Stdout); err != nil {
		return fmt.Errorf("failed to retrieve to file: %s", err)
	}

	return nil
}

func (cs *cacheStore) retrieveFile(ctx context.Context, cid cid.Cid, output string, name string) error {
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

	f, err := os.OpenFile(path.Join(output, name), os.O_RDWR|os.O_CREATE, 0o666)
	if err != nil {
		return fmt.Errorf("failed to open tmp file: %s", err)
	}
	_, _ = f.Seek(0, io.SeekStart)

	if err := cs.provider.RetrieveEvent(ctx, RetrieveEventParams{
		CID: cid,
	}, f); err != nil {
		return fmt.Errorf("failed to retrieve to file: %s", err)
	}

	return nil
}

type coldStore struct{} // nolint

func (cs *coldStore) retrieve(ctx context.Context, c cid.Cid, path string) error { // nolint
	lassie, err := lassie.NewLassie(ctx)
	if err != nil {
		return fmt.Errorf("failed to create lassie instance: %s", err)
	}

	carOpts := []car.Option{
		car.WriteAsCarV1(true),
		car.StoreIdentityCIDs(false),
		car.UseWholeCIDs(false),
	}

	carWriter := deferred.NewDeferredCarWriterForPath(path, []cid.Cid{c}, carOpts...)
	defer func() {
		_ = carWriter.Close()
	}()
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