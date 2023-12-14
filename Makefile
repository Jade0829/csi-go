# Copyright (c) Arm Limited and Contributors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# output dir
OUT_DIR := ./_out
# dir for tools: e.g., golangci-lint
TOOL_DIR := $(OUT_DIR)/tool
# use golangci-lint for static code check
GOLANGCI_VERSION := v1.52.2
GOLANGCI_BIN := $(TOOL_DIR)/golangci-lint
# go source, scripts
SOURCE_DIRS := cmd pkg
SCRIPT_DIRS := scripts deploy e2e
# goarch for cross building
ifeq ($(origin GOARCH), undefined)
  GOARCH := $(shell go env GOARCH)
endif
# csi image info (spdkcsi/spdkcsi:canary)
ifeq ($(origin CSI_IMAGE_REGISTRY), undefined)
  CSI_IMAGE_REGISTRY := spdkcsi
endif
ifeq ($(origin CSI_IMAGE_TAG), undefined)
  CSI_IMAGE_TAG := canary
endif
CSI_IMAGE := $(CSI_IMAGE_REGISTRY)/gcsi:$(CSI_IMAGE_TAG)

# default target
all: gcsi

# build binary
.PHONY: gcsi
gcsi:
	@echo === building gluesys csi binary
	@CGO_ENABLED=0 GOARCH=$(GOARCH) GOOS=linux go build -buildvcs=false -o $(OUT_DIR)/gcsi ./cmd/



.PHONY: clean
clean:
	rm -f $(OUT_DIR)/gcsi
	go clean -testcache
