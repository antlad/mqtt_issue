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

#include "libmqtt.h"

static JavaVM *jvm;

void conn_handler(int client, char *server, libmqtt_connack_t code, char *err);

void sub_handler(int client, char *topic, int qos, char *err);

void pub_handler(int client, char *topic, char *err);

void unsub_handler(int client, char *topic, char *err);

void net_handler(int client, char *server, char *err);

void persist_handler(int client, char *err);
