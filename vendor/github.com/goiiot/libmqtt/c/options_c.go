// +build cgo lib

/*
 * Copyright Go-IIoT (https://github.com/goiiot)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

/*
#ifndef _LIBMQTT_LOG_H_
#define _LIBMQTT_LOG_H_

typedef enum {
  libmqtt_log_silent = 0,
  libmqtt_log_verbose = 1,
  libmqtt_log_debug = 2,
  libmqtt_log_info = 3,
  libmqtt_log_warning = 4,
  libmqtt_log_error = 5,
} libmqtt_log_level;

typedef enum {
  libmqtt_mqtt_version_311 = 4,
  libmqtt_mqtt_version_5 = 5,
} libmqtt_mqtt_version;

#endif //_LIBMQTT_LOG_H_
*/
import "C"
import (
	"math"
	"sync"
	"time"
	"unsafe"

	mqtt "github.com/goiiot/libmqtt"
)

var (
	// clientOptions = make(map[int][]mqtt.Option)
	clientOptions = &sync.Map{}
)

func newClientOptions() int {
	options := make([]mqtt.Option, 0)
	for id := 1; id < math.MaxInt64; id++ {
		if _, loaded := clientOptions.LoadOrStore(id, options); !loaded {
			return id
		}
	}
	return -1
}

func getClientOptions(id int) []mqtt.Option {
	if options, ok := clientOptions.Load(id); ok {
		return options.([]mqtt.Option)
	}
	return nil
}

// Libmqtt_new_client ()
// create a new client, return the id of client
// if no client id available will return -1
//export Libmqtt_new_client
func Libmqtt_new_client() C.int {
	return C.int(newClientOptions())
}

// Libmqtt_setup_client (int client)
// setup the client with previously defined options
//export Libmqtt_setup_client
func Libmqtt_setup_client(client C.int) *C.char {
	cid := int(client)
	options := getClientOptions(cid)
	if options == nil {
		return C.CString("no such client")
	}

	c, err := mqtt.NewClient(options...)
	if err != nil {
		storeClient(cid, c)
		return C.CString(err.Error())
	}

	return nil
}

// Libmqtt_client_with_clean_session (bool flag)
//export Libmqtt_client_with_clean_session
func Libmqtt_client_with_clean_session(client C.int, flag bool) {
	addOption(client, mqtt.WithCleanSession(flag))
}

// Libmqtt_client_with_identity (int client, char * username, char * password)
//export Libmqtt_client_with_identity
func Libmqtt_client_with_identity(client C.int, username, password *C.char) {
	addOption(client, mqtt.WithIdentity(C.GoString(username), C.GoString(password)))
}

// Libmqtt_client_with_keepalive (int client, int keepalive, float factor)
//export Libmqtt_client_with_keepalive
func Libmqtt_client_with_keepalive(client C.int, keepalive C.int, factor C.float) {
	addOption(client, mqtt.WithKeepalive(uint16(keepalive), float64(factor)))
}

// Libmqtt_client_with_auto_reconnect (int client, bool enable)
//export Libmqtt_client_with_auto_reconnect
func Libmqtt_client_with_auto_reconnect(client C.int, enable bool) {
	addOption(client, mqtt.WithAutoReconnect(enable))
}

// Libmqtt_client_with_backoff_strategy (int client, int first_delay, int maxDelay, double factor)
//export Libmqtt_client_with_backoff_strategy
func Libmqtt_client_with_backoff_strategy(client C.int, firstDelay C.int, maxDelay C.int, factor C.double) {
	addOption(client, mqtt.WithBackoffStrategy(
		time.Duration(firstDelay)*time.Millisecond,
		time.Duration(maxDelay)*time.Millisecond,
		float64(factor),
	))
}

// Libmqtt_client_with_client_id (int client, char * client_id)
//export Libmqtt_client_with_client_id
func Libmqtt_client_with_client_id(client C.int, clientID *C.char) {
	addOption(client, mqtt.WithClientID(C.GoString(clientID)))
}

// Libmqtt_client_with_will (int client, char *topic, int qos, bool retain, char *payload, int payloadSize)
//export Libmqtt_client_with_will
func Libmqtt_client_with_will(client C.int, topic *C.char, qos C.int, retain bool, payload *C.char, payloadSize C.int) {
	addOption(client, mqtt.WithWill(
		C.GoString(topic),
		mqtt.QosLevel(qos),
		retain,
		C.GoBytes(unsafe.Pointer(payload), payloadSize)))
}

// Libmqtt_client_with_secure_server (int client, char *server)
//export Libmqtt_client_with_secure_server
func Libmqtt_client_with_secure_server(client C.int, server *C.char) {
	addOption(client, mqtt.WithSecureServer(C.GoString(server)))
}

// Libmqtt_client_with_server (int client, char *server)
//export Libmqtt_client_with_server
func Libmqtt_client_with_server(client C.int, server *C.char) {
	addOption(client, mqtt.WithServer(C.GoString(server)))
}

// Libmqtt_client_with_tls (int client, char * certFile, char * keyFile, char * caCert, char * serverNameOverride, bool skipVerify)
//export Libmqtt_client_with_tls
func Libmqtt_client_with_tls(client C.int, certFile, keyFile, caCert, serverNameOverride *C.char, skipVerify bool) {
	addOption(
		client,
		mqtt.WithTLS(
			C.GoString(certFile),
			C.GoString(keyFile),
			C.GoString(caCert),
			C.GoString(serverNameOverride),
			skipVerify,
		),
	)
}

// Libmqtt_client_with_dial_timeout (int client, int timeout)
//export Libmqtt_client_with_dial_timeout
func Libmqtt_client_with_dial_timeout(client C.int, timeout C.int) {
	addOption(client, mqtt.WithDialTimeout(uint16(timeout)))
}

// Libmqtt_client_with_buf_size (int client, int send_buf_size, int recv_buf_size)
//export Libmqtt_client_with_buf_size
func Libmqtt_client_with_buf_size(client C.int, sendBuf C.int, recvBuf C.int) {
	addOption(client, mqtt.WithBuf(int(sendBuf), int(recvBuf)))
}

// Libmqtt_client_with_log (int client, libmqtt_log_level l)
//export Libmqtt_client_with_log
func Libmqtt_client_with_log(client C.int, l C.libmqtt_log_level) {
	addOption(client, mqtt.WithLog(mqtt.LogLevel(l)))
}

// Libmqtt_client_with_version (int client, libmqtt_log_level l)
//export Libmqtt_client_with_version
func Libmqtt_client_with_version(client C.int, version C.libmqtt_mqtt_version, compromise bool) {
	addOption(client, mqtt.WithVersion(mqtt.ProtoVersion(version), compromise))
}

// Libmqtt_client_with_none_persist (int client)
//export Libmqtt_client_with_none_persist
func Libmqtt_client_with_none_persist(client C.int) {
	addOption(client, mqtt.WithPersist(mqtt.NonePersist))
}

// Libmqtt_client_with_mem_persist (int client, int maxCount, bool dropOnExceed, bool duplicateReplace)
//export Libmqtt_client_with_mem_persist
func Libmqtt_client_with_mem_persist(client C.int, maxCount C.int, dropOnExceed bool, duplicateReplace bool) {
	addOption(client, mqtt.WithPersist(mqtt.NewMemPersist(&mqtt.PersistStrategy{
		MaxCount:         uint32(maxCount),
		DropOnExceed:     dropOnExceed,
		DuplicateReplace: duplicateReplace,
	})))
}

/*

  Topic Router Options

*/

// Libmqtt_client_with_std_router use mqtt standard router
//export Libmqtt_client_with_std_router
func Libmqtt_client_with_std_router(client C.int) {
	addOption(client, mqtt.WithRouter(mqtt.NewStandardRouter()))
}

// Libmqtt_client_with_text_router use text router
//export Libmqtt_client_with_text_router
func Libmqtt_client_with_text_router(client C.int) {
	addOption(client, mqtt.WithRouter(mqtt.NewTextRouter()))
}

// Libmqtt_client_with_regex_router use regex matching for router
//export Libmqtt_client_with_regex_router
func Libmqtt_client_with_regex_router(client C.int) {
	addOption(client, mqtt.WithRouter(mqtt.NewRegexRouter()))
}

/*

  Session Persist Options

*/

// Libmqtt_client_with_file_persist (int client, char *dirPath, int maxCount, bool dropOnExceed, bool duplicateReplace)
//export Libmqtt_client_with_file_persist
func Libmqtt_client_with_file_persist(client C.int, dirPath *C.char, maxCount C.int, dropOnExceed bool, duplicateReplace bool) {
	addOption(client, mqtt.WithPersist(mqtt.NewFilePersist(C.GoString(dirPath), &mqtt.PersistStrategy{
		MaxCount:         uint32(maxCount),
		DropOnExceed:     dropOnExceed,
		DuplicateReplace: duplicateReplace,
	})))
}

// Libmqtt_client_with_redis_persist use redis as persist method
func Libmqtt_client_with_redis_persist(client C.int) {

}

func addOption(client C.int, option mqtt.Option) {
	if options := getClientOptions(int(client)); options != nil {
		options = append(options, option)
		clientOptions.Store(int(client), options)
	}
}
