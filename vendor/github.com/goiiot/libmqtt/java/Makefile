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

C_BUILD_DIR := ../c/build
BUILD_DIR := build

JAVA_PACKAGE := org.goiiot.libmqtt
JAVA_PACKAGE_PATH := org/goiiot/libmqtt

JAVA_HOME ?= /Library/Java/JavaVirtualMachines/jdk1.8.0_131.jdk/Contents/Home
CFLAGS := -std=c++0x -I $(C_BUILD_DIR) -I build -L $(C_BUILD_DIR)

SRC_SET := libmqtt_jni.cpp handlers_jni.cpp options_jni.cpp

# detect build system for native build
HOST_SYSTEM = $(shell uname | cut -f 1 -d_)
SYSTEM ?= $(HOST_SYSTEM)

# linux
ifeq ($(SYSTEM),Linux)
	PLATFORM_FLAGS := -fPIC -shared -lpthread -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux
	SUFFIX=so
endif
# macOS
ifeq ($(SYSTEM),Darwin)
	PLATFORM_FLAGS := -dynamiclib -framework JavaVM -framework CoreFoundation -framework Security -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/darwin
	SUFFIX=jnilib
endif

.PHONY: build compile-java gen-jni-header clean

build: gen-jni-header
	$(CC) $(CFLAGS) $(PLATFORM_FLAGS) -o $(BUILD_DIR)/libmqtt-jni.$(SUFFIX) -lmqtt $(SRC_SET)

gen-jni-header: compile-java
	javah -jni -classpath $(BUILD_DIR) -d $(BUILD_DIR) $(JAVA_PACKAGE).LibMQTT

compile-java:
	mkdir -p build
	javac -Xlint:unchecked $(JAVA_PACKAGE_PATH)/LibMQTT.java -d $(BUILD_DIR)

run-example: compile-example
	cd $(BUILD_DIR) && \
	DYLD_LIBRARY_PATH=../$(C_BUILD_DIR) \
	LD_LIBRARY_PATH=../$(C_BUILD_DIR) \
	java -Djava.library.path=. \
	$(JAVA_PACKAGE_PATH)/example/Example && \
	cd ..

compile-example:
	javac -Xdiags:verbose $(JAVA_PACKAGE_PATH)/example/Example.java -d $(BUILD_DIR)

clean:
	rm -rf $(BUILD_DIR)