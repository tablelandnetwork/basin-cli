package vaultsprovider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/tablelandnetwork/basin-cli/internal/app"
)

// VaultsProvider implements the app.VaultsProvider interface.
type VaultsProvider struct {
	provider string
	client   *http.Client
}

var _ app.VaultsProvider = (*VaultsProvider)(nil)

// New creates a new VaultsProvider.
func New(provider string) *VaultsProvider {
	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	return &VaultsProvider{
		provider: provider,
		client:   client,
	}
}

// CreateVault creates a vault.
func (bp *VaultsProvider) CreateVault(ctx context.Context, params app.CreateVaultParams) error {
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
func (bp *VaultsProvider) ListVaults(
	ctx context.Context, params app.ListVaultsParams,
) ([]app.VaultWithCacheDuration, error) {
	req, err := http.NewRequestWithContext(
		ctx, http.MethodGet, fmt.Sprintf("%s/v2/vaults/?account=%s", bp.provider, params.Account.Hex()), nil)
	if err != nil {
		return []app.VaultWithCacheDuration{}, fmt.Errorf("could not create request: %s", err)
	}

	resp, err := bp.client.Do(req)
	if err != nil {
		return []app.VaultWithCacheDuration{}, fmt.Errorf("request to list vaults failed: %s", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	var vaults []app.VaultWithCacheDuration
	if err := json.NewDecoder(resp.Body).Decode(&vaults); err != nil {
		return []app.VaultWithCacheDuration{}, fmt.Errorf("failed to read response: %s", err)
	}
	return vaults, nil
}

// ListVaultEvents lists all events from a given vault.
func (bp *VaultsProvider) ListVaultEvents(
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
func (bp *VaultsProvider) WriteVaultEvent(ctx context.Context, params app.WriteVaultEventParams) error {
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		fmt.Sprintf("%s/vaults/%s/events", bp.provider, params.Vault),
		io.TeeReader(params.Content, params.ProgressBar),
	)
	if err != nil {
		return fmt.Errorf("could not create request: %s", err)
	}

	req.Header.Add("filename", params.Filename)

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

	if resp.StatusCode != http.StatusCreated {
		type response struct {
			Error string
		}
		var r response
		if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
			return fmt.Errorf("failed to decode response: %s", err)
		}

		return fmt.Errorf(r.Error)
	}

	return nil
}

// RetrieveEvent retrieves an event.
func (bp *VaultsProvider) RetrieveEvent(
	ctx context.Context, params app.RetrieveEventParams, w io.Writer,
) (string, error) {
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		fmt.Sprintf("%s/events/%s", bp.provider, params.CID.String()),
		nil,
	)
	if err != nil {
		return "", fmt.Errorf("could not create request: %s", err)
	}

	client := &http.Client{
		Timeout: time.Duration(params.Timeout) * time.Second,
	}

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("request to write vault event failed: %s", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode == http.StatusNotFound {
		return "", app.ErrNotFoundInCache
	}

	re := regexp.MustCompile(`".+"`)
	filename := re.FindString(resp.Header.Get("content-disposition"))
	if len(filename) == 0 {
		return "", errors.New("filename not found")
	}

	parts := strings.Split(filename[1:len(filename)-1], "-")
	if len(parts) < 2 {
		return "", errors.New("filename format is not correct")
	}

	if _, err := io.Copy(w, resp.Body); err != nil {
		return "", errors.New("failed copy response body")
	}
	return strings.Join(parts[1:], "-"), nil
}
