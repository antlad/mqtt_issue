# Copyright Go-IIoT (https://github.com/goiiot)
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

BUILD_DIR = ../build
CFLAGS = -fPIC -O2 -std=c99 -I $(BUILD_DIR) -L $(BUILD_DIR)
TARGET = $(BUILD_DIR)/example

LINUX_FLAGS = -lpthread

# detect build system for native build
HOST_SYSTEM = $(shell uname -s)
SYSTEM ?= $(HOST_SYSTEM)

# linux
ifeq ($(SYSTEM),Linux)
	PLATFORM_FLAGS := -lpthread
endif
# macOS
ifeq ($(SYSTEM),Darwin)
	PLATFORM_FLAGS := -framework CoreFoundation -framework Security
endif

# set ld path for linux, dyld path for darwin
# linux
ifeq ($(SYSTEM),Linux)
	RUNTIME_FLAGS := LD_LIBRARY_PATH=$(BUILD_DIR)
endif
# macOS
ifeq ($(SYSTEM),Darwin)
	RUNTIME_FLAGS := DYLD_LIBRARY_PATH=$(BUILD_DIR)
endif

.PHONY: build run clean

ARCHIVE_TARGET := $(TARGET)-a
SHARED_TARGET := $(TARGET)-so

build:
	$(CC) $(CFLAGS) -o $(ARCHIVE_TARGET) \
		example.c $(BUILD_DIR)/libmqtt.a $(PLATFORM_FLAGS)
	$(CC) $(CFLAGS) -o $(SHARED_TARGET) \
		-lmqtt $(PLATFORM_FLAGS) example.c

run: run-a

run-a:
	$(ARCHIVE_TARGET)

run-so:
	$(RUNTIME_FLAGS) $(SHARED_TARGET)

clean:
	rm -rf $(ARCHIVE_TARGET) $(SHARED_TARGET)
