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

package extension

import (
	"bytes"

	"github.com/boltdb/bolt"
	"github.com/coreos/etcd/clientv3"
	"github.com/go-redis/redis"

	mqtt "github.com/goiiot/libmqtt"
)

const defaultRedisKey = "libmqtt"

// NewRedisPersist creates a new redisPersist for session persist
// with provided redis connection and mainKey,
//
// if mainKey is empty here, the default mainKey "libmqtt" will be used
// if no redis client (nil) provided, will return nil
func NewRedisPersist(conn *redis.Client, mainKey string) mqtt.PersistMethod {
	if conn == nil {
		return nil
	}

	if mainKey == "" {
		mainKey = defaultRedisKey
	}

	buf := &bytes.Buffer{}
	return &redisPersist{
		conn:    conn,
		mainKey: mainKey,
		buf:     buf,
	}
}

// NewEtcdPersist creates a new EtcdPersist for session persist with provided etcd client
func NewEtcdPersist(client *clientv3.Client) mqtt.PersistMethod {
	return nil
}

// NewBoltPersist creates a new boltPersist for session persist with provided bucket
func NewBoltPersist(bucket *bolt.Bucket) mqtt.PersistMethod {
	return nil
}
