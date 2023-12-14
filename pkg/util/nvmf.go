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
	"fmt"
	"sync"

	"k8s.io/klog"
)

const invalidNSID = 0

type nodeNVMf struct {
	client *rpcClient

	targetType   string // RDMA, TCP
	targetAddr   string
	targetPort   string
	transCreated int32

	lvols map[string]*lvolNVMf
	mtx   sync.Mutex // for concurrent access to lvols map
}

type lvolNVMf struct {
	nsID  int
	nqn   string
	model string
	uuid  string
	port  int
}

func (lvol *lvolNVMf) reset() {
	lvol.nsID = invalidNSID
	lvol.nqn = ""
	lvol.model = ""
	lvol.uuid = ""
}

func newNVMf(client *rpcClient, targetType, targetAddr string) *nodeNVMf {
	return &nodeNVMf{
		client:     client,
		targetAddr: targetAddr,
		targetType: targetType,
		targetPort: "4420",
		lvols:      make(map[string]*lvolNVMf),
	}
}

func (node *nodeNVMf) Info() string {
	return node.client.info()
}

func (node *nodeNVMf) LvList() ([]LV, error) {
	return node.client.lvList()
}

// VolumeInfo returns a string:string map containing information necessary
// for CSI node(initiator) to connect to this target and identify the disk.
func (node *nodeNVMf) VolumeInfo(lvolID string) (map[string]string, error) {
	node.mtx.Lock()
	lvol, exists := node.lvols[lvolID]
	node.mtx.Unlock()

	if !exists {
		return nil, fmt.Errorf("volume not exists: %s", lvolID)
	}

	return map[string]string{
		"targetType": node.targetType,
		"targetAddr": node.targetAddr,
		"targetPort": node.targetPort,
		"nqn":        lvol.nqn,
		"model":      lvol.model,
		"uuid":       lvol.uuid,
	}, nil
}

// CreateVolume creates a logical volume and returns volume ID
func (node *nodeNVMf) CreateVolume(lvsName string, sizeMiB int64) (string, error) {
	lvolID, uuid, err := node.client.createVolume(lvsName, sizeMiB)
	if err != nil {
		return "", err
	}

	node.mtx.Lock()
	defer node.mtx.Unlock()

	_, exists := node.lvols[lvolID]
	if exists {
		return "", fmt.Errorf("volume ID already exists: %s", lvolID)
	}

	node.lvols[lvolID] = &lvolNVMf{nsID: invalidNSID}
	node.lvols[lvolID].model = lvolID
	node.lvols[lvolID].nqn = "nqn.gluesys.csi:" + lvolID
	node.lvols[lvolID].uuid = uuid

	klog.V(5).Infof("volume created: %s", lvolID)
	return lvolID, nil
}

func (node *nodeNVMf) DeleteVolume(lvolID string) error {
	err := node.client.deleteVolume(lvolID)
	if err != nil {
		return err
	}
	fmt.Println("nvmf.go : DeleteVolume -> lvolID = ", lvolID)

	node.mtx.Lock()
	defer node.mtx.Unlock()

	delete(node.lvols, lvolID)

	_, exists := node.lvols[lvolID]
	if !exists {
		fmt.Println("Not Deleted\n")
	}

	klog.V(5).Infof("volume deleted: %s", lvolID)
	return nil
}
