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

BUILD_DIR := build
TARGET := libmqtt
BUILD_ARGS := -ldflags=-s

LIB_SHARED := $(BUILD_DIR)/$(TARGET).so
LIB_ARCHIVE := $(BUILD_DIR)/$(TARGET).a

.PHONY: lib example clean

lib: $(LIB_ARCHIVE) $(LIB_SHARED)

$(LIB_SHARED):
	env CGO_ENABLED=1 go build -buildmode=c-shared \
	    $(BUILD_ARGS) -o $(LIB_SHARED)

$(LIB_ARCHIVE):
	env CGO_ENABLED=1 go build -buildmode=c-archive \
	    $(BUILD_ARGS) -o $(LIB_ARCHIVE)

example: $(LIB_ARCHIVE)
	$(MAKE) -C example build

clean:
	$(MAKE) -C example clean
	rm -rf $(BUILD_DIR)