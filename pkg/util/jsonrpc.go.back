/*
Copyright (c) Arm Limited and Contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package util

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

// SpdkNode defines interface for SPDK storage node
//
// - Info returns node info(rpc url) for debugging purpose
// - LvStores returns available volume stores(name, size, etc) on that node.
// - VolumeInfo returns a string map to be passed to client node. Client node
//   needs these info to mount the target. E.g, target IP, service port, nqn.
// - Create/Delete/Publish/UnpublishVolume per CSI controller service spec.
//
// NOTE: concurrency, idempotency, message ordering
//
// In below text, "implementation" refers to the code implements this interface,
// and "caller" is the code uses the implementation.
//
// Concurrency requirements for implementation and caller:
// - Implementation should make sure CreateVolume is thread safe. Caller is free
//   to request creating multiple volumes in same volume store concurrently, no
//   data race should happen.
// - Implementation should make sure PublishVolume/UnpublishVolume/DeleteVolume
//   for *different volumes* thread safe. Caller may issue these requests to
//   *different volumes", in same volume store or not, concurrently.
// - PublishVolume/UnpublishVolume/DeleteVolume for *same volume* is not thread
//   safe, concurrent access may lead to data race. Caller must serialize these
//   calls to *same volume*, possibly by mutex or message queue per volume.
// - Implementation should make sure LvStores and VolumeInfo are thread safe,
//   but it doesn't lock the returned resources. It means caller should adopt
//   optimistic concurrency control and retry on specific failures.
//   E.g, caller calls LvStores and finds a volume store with enough free space,
//   it calls CreateVolume but fails with "not enough space" because another
//   caller may issue similar request at same time. The failed caller may redo
//   above steps(call LvStores, pick volume store, CreateVolume) under this
//   condition, or it can simply fail.
//
// Idempotent requirements for implementation:
// Per CSI spec, it's possible that same request been sent multiple times due to
// issues such as a temporary network failure. Implementation should have basic
// logic to deal with idempotency.
// E.g, ignore publishing an already published volume.
//
// Out of order messages handling for implementation:
// Out of order message may happen in kubernetes CSI framework. E.g, unpublish
// an already deleted volume. The baseline is there should be no code crash or
// data corruption under these conditions. Implementation may try to detect and
// report errors if possible.
type CSINode interface {
	VgList() ([]VG, error)
	VolumeInfo(lvolID string) (map[string]string, error)
	CreateVolume(lvsName string, sizeMiB int64) (string, error)
	DeleteVolume(lvolID string) error
}

// logical volume store
type VG struct {
	Name         string
	TotalSizeMiB int64
	FreeSizeMiB  int64
}

// errors deserve special care
var (
	// json response errors: errors.New("json: tag-string")
	// matches if "tag-string" founded in json error string
	ErrJSONNoSpaceLeft  = errors.New("json: No space left")
	ErrJSONNoSuchDevice = errors.New("json: No such device")

	// internal errors
	ErrVolumeDeleted     = errors.New("volume deleted")
	ErrVolumePublished   = errors.New("volume already published")
	ErrVolumeUnpublished = errors.New("volume not published")
)

// jsonrpc http proxy
type rpcClient struct {
	rpcURL     string
	rpcUser    string
	rpcPass    string
	httpClient *http.Client
	rpcID      int32 // json request message ID, auto incremented
}

func NewCSINode(rpcURL, rpcUser, rpcPass, targetType, targetAddr string) (CSINode, error) {
	client := rpcClient{
		rpcURL:     rpcURL,
		rpcUser:    rpcUser,
		rpcPass:    rpcPass,
		httpClient: &http.Client{Timeout: cfgRPCTimeoutSeconds * time.Second},
	}

	return newNVMf(&client, "TCP", targetAddr), nil
}

func (client *rpcClient) info() string {
	return client.rpcURL
}

func (client *rpcClient) vgList() ([]VG, error) {
	var result []struct {
		Name         string `json:"name"`
		TotalSizeMiB int64
		FreeSizeMiB  int64
	}

	err := client.call("getVGList", nil, &result)
	if err != nil {
		return nil, err
	}

	lvs := make([]VG, len(result))
	for i := range result {
		r := &result[i]
		lvs[i].Name = r.Name
		lvs[i].TotalSizeMiB = r.TotalSizeMiB
		lvs[i].FreeSizeMiB = r.FreeSizeMiB
	}

	return lvs, nil
}

func (client *rpcClient) createVolume(vgName string, sizeMiB int64) (string, error) {
	params := struct {
		LvolName string `json:"lvol_name"`
		Size     int64  `json:"size"`
		VgName   string `json:"vg_name"`
	}{
		LvolName: uuid.New().String(),
		Size:     sizeMiB,
		VgName:   vgName,
	}

	var lvolID string

	err := client.call("createNVMeOF", &params, &lvolID)

	return lvolID, err
}

func (client *rpcClient) deleteVolume(lvolID string) error {
	params := struct {
		Name string `json:"name"`
	}{
		Name: lvolID,
	}

	err := client.call("deleteNVMeOF", &params, nil)

	return err
}

// low level rpc request/response handling
func (client *rpcClient) call(endpoint string, args, result interface{}) error {
	type rpcRequest struct {
		ID       int32
		Endpoint string
	}

	id := atomic.AddInt32(&client.rpcID, 1)
	request := rpcRequest{
		ID:       id,
		Endpoint: endpoint,
	}

	var data []byte
	var err error

	if args == nil {
		data, err = json.Marshal(request)
	} else {
		requestWithParams := struct {
			rpcRequest
			Params interface{} `json:"params"`
		}{
			request,
			args,
		}
		data, err = json.Marshal(requestWithParams)
	}

	url := client.rpcURL + endpoint

	req, err := http.NewRequest("POST", url, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("%s: %s", endpoint, err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := client.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("%s: %s", endpoint, err)
	}

	defer resp.Body.Close()

	response := struct {
		Result interface{} `json:"result"`
	}{
		Result: result,
	}

	err = json.NewDecoder(resp.Body).Decode(&response)
	if err != nil {
		return fmt.Errorf("%s: %s", endpoint, err)
	}

	return nil
}
