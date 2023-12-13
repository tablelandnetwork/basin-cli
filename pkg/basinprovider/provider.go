package basinprovider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/tablelandnetwork/basin-cli/internal/app"
)

// BasinProvider implements the app.BasinProvider interface.
type BasinProvider struct {
	provider string
	client   *http.Client
}

var _ app.BasinProvider = (*BasinProvider)(nil)

// New creates a new BasinProvider.
func New(provider string) *BasinProvider {
	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	return &BasinProvider{
		provider: provider,
		client:   client,
	}
}

// CreateVault creates a vault.
func (bp *BasinProvider) CreateVault(ctx context.Context, params app.CreateVaultParams) error {
	form := url.Values{}
	form.Add("account", params.Account.Hex())
	form.Add("cache", fmt.Sprint(params.CacheDuration))

	req, err := http.NewRequestWithContext(
		ctx, http.MethodPost, fmt.Sprintf("%s/vaults/%s", bp.provider, params.Vault), strings.NewReader(form.Encode()))
	if err != nil {
		return fmt.Errorf("could not create request: %s", err)
	}
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	resp, err := bp.client.Do(req)
	if err != nil {
		return fmt.Errorf("request to create vault failed: %s", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusCreated {
		return errors.New("account was not created")
	}

	return nil
}

// ListVaults lists all vaults from a given account.
func (bp *BasinProvider) ListVaults(
	ctx context.Context, params app.ListVaultsParams,
) ([]app.Vault, error) {
	req, err := http.NewRequestWithContext(
		ctx, http.MethodGet, fmt.Sprintf("%s/vaults/?account=%s", bp.provider, params.Account.Hex()), nil)
	if err != nil {
		return []app.Vault{}, fmt.Errorf("could not create request: %s", err)
	}

	resp, err := bp.client.Do(req)
	if err != nil {
		return []app.Vault{}, fmt.Errorf("request to list vaults failed: %s", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	var vaults []app.Vault
	if err := json.NewDecoder(resp.Body).Decode(&vaults); err != nil {
		return []app.Vault{}, fmt.Errorf("failed to read response: %s", err)
	}
	return vaults, nil
}

// ListVaultEvents lists all events from a given vault.
func (bp *BasinProvider) ListVaultEvents(
	ctx context.Context, params app.ListVaultEventsParams,
) ([]app.EventInfo, error) {
	req, err := http.NewRequestWithContext(
		ctx, http.MethodGet, fmt.Sprintf("%s/vaults/%s/events", bp.provider, params.Vault), nil)
	if err != nil {
		return []app.EventInfo{}, fmt.Errorf("could not create request: %s", err)
	}

	q := req.URL.Query()
	q.Add("limit", fmt.Sprint(params.Limit))
	q.Add("offset", fmt.Sprint(params.Offset))
	q.Add("before", fmt.Sprint(params.Before.Seconds()))
	q.Add("after", fmt.Sprint(params.After.Seconds()))
	req.URL.RawQuery = q.Encode()

	resp, err := bp.client.Do(req)
	if err != nil {
		return []app.EventInfo{}, fmt.Errorf("request to list vault events failed: %s", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	var events []app.EventInfo
	if err := json.NewDecoder(resp.Body).Decode(&events); err != nil {
		return []app.EventInfo{}, fmt.Errorf("failed to read response: %s", err)
	}
	return events, nil
}

// WriteVaultEvent write an event.
func (bp *BasinProvider) WriteVaultEvent(ctx context.Context, params app.WriteVaultEventParams) error {
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		fmt.Sprintf("%s/vaults/%s/events", bp.provider, params.Vault),
		io.TeeReader(params.Content, params.ProgressBar),
	)
	if err != nil {
		return fmt.Errorf("could not create request: %s", err)
	}

	q := req.URL.Query()
	q.Add("timestamp", fmt.Sprint(params.Timestamp.Seconds()))
	q.Add("signature", fmt.Sprint(params.Signature))
	req.URL.RawQuery = q.Encode()
	req.ContentLength = params.Size

	client := &http.Client{
		Timeout: 0,
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("request to write vault event failed: %s", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	return nil
}
