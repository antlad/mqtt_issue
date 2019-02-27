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

#include "org_goiiot_libmqtt_LibMQTT.h"
#include "libmqtt.h"
#include "handlers_jni.h"

/*
 * Method:    _newClient
 * Signature: ()I
 */
JNIEXPORT jint JNICALL
Java_org_goiiot_libmqtt_LibMQTT__1newClient
(JNIEnv *env, jclass c) {

  return Libmqtt_new_client();
}

/*
 * Method:    _setServer
 * Signature: (ILjava/lang/String;)V
 */
JNIEXPORT void JNICALL
Java_org_goiiot_libmqtt_LibMQTT__1setServer
(JNIEnv *env, jclass c, jint id, jstring server) {

  auto srv = env->GetStringUTFChars(server, 0);

  Libmqtt_client_with_server(id, (char *) srv);

  env->ReleaseStringUTFChars(server, srv);
}

/*
 * Method:    _setCleanSession
 * Signature: (IZ)V
 */
JNIEXPORT void JNICALL
Java_org_goiiot_libmqtt_LibMQTT__1setCleanSession
(JNIEnv *env, jclass c, jint id, jboolean flag) {

  Libmqtt_client_with_clean_session(id, flag);
}

/*
 * Method:    _setKeepalive
 * Signature: (IID)V
 */
JNIEXPORT void JNICALL
Java_org_goiiot_libmqtt_LibMQTT__1setKeepalive
(JNIEnv *env, jclass c, jint id, jint keepalive, jdouble factor) {

  Libmqtt_client_with_keepalive(id, keepalive, factor);
}

/*
 * Method:    _setClientID
 * Signature: (ILjava/lang/String;)V
 */
JNIEXPORT void JNICALL
Java_org_goiiot_libmqtt_LibMQTT__1setClientID
(JNIEnv *env, jclass c, jint id, jstring client_id) {

  auto cid = env->GetStringUTFChars(client_id, 0);

  Libmqtt_client_with_client_id(id, (char *)cid);

  env->ReleaseStringUTFChars(client_id, cid);
}

/*
 * Method:    _setDialTimeout
 * Signature: (II)V
 */
JNIEXPORT void JNICALL
Java_org_goiiot_libmqtt_LibMQTT__1setDialTimeout
(JNIEnv *env, jclass c, jint id, jint timeout) {

  Libmqtt_client_with_dial_timeout(id, timeout);
}

/*
 * Method:    _setIdentity
 * Signature: (ILjava/lang/String;Ljava/lang/String;)V
 */
JNIEXPORT void JNICALL
Java_org_goiiot_libmqtt_LibMQTT__1setIdentity
(JNIEnv *env, jclass c, jint id, jstring username, jstring password) {

  auto user = env->GetStringUTFChars(username, 0);
  auto pass = env->GetStringUTFChars(password, 0);

  Libmqtt_client_with_identity(id, (char *)user, (char *)pass);

  env->ReleaseStringUTFChars(username, user);
  env->ReleaseStringUTFChars(password, pass);
}

/*
 * Method:    _setLog
 * Signature: (II)V
 */
JNIEXPORT void JNICALL
Java_org_goiiot_libmqtt_LibMQTT__1setLog
(JNIEnv *env, jclass c, jint id, jint log_level) {

  Libmqtt_client_with_log(id, (libmqtt_log_level) log_level);
}

/*
 * Method:    _setSendBuf
 * Signature: (II)V
 */
JNIEXPORT void JNICALL
Java_org_goiiot_libmqtt_LibMQTT__1setSendBuf
(JNIEnv *env, jclass c, jint id, jint size) {

  Libmqtt_client_with_send_buf(id, size);
}

/*
 * Method:    _setRecvBuf
 * Signature: (II)V
 */
JNIEXPORT void JNICALL
Java_org_goiiot_libmqtt_LibMQTT__1setRecvBuf
(JNIEnv *env, jclass c, jint id, jint size) {

  Libmqtt_client_with_send_buf(id, size);
}

/*
 * Method:    _setTLS
 * Signature: (ILjava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Z)V
 */
JNIEXPORT void JNICALL
Java_org_goiiot_libmqtt_LibMQTT__1setTLS
(JNIEnv *env, jclass c, jint id, jstring cert,
 jstring key, jstring ca, jstring srv_name, jboolean skip_verify) {

  auto c_cert = env->GetStringUTFChars(cert, 0);
  auto c_key = env->GetStringUTFChars(key, 0);
  auto c_ca = env->GetStringUTFChars(ca, 0);
  auto c_srv = env->GetStringUTFChars(srv_name, 0);

  Libmqtt_client_with_tls(id, (char *)c_cert, (char *)c_key, (char *)c_ca,
                          (char *) c_srv, skip_verify);

  env->ReleaseStringUTFChars(cert, c_cert);
  env->ReleaseStringUTFChars(key, c_key);
  env->ReleaseStringUTFChars(ca, c_ca);
  env->ReleaseStringUTFChars(srv_name, c_srv);
}

/*
 * Method:    _setWill
 * Signature: (ILjava/lang/String;IZ[B)V
 */
JNIEXPORT void JNICALL
Java_org_goiiot_libmqtt_LibMQTT__1setWill
(JNIEnv *env, jclass c, jint id, jstring topic,
 jint qos, jboolean retain, jbyteArray payload) {

  auto c_topic = env->GetStringUTFChars(topic, 0);
  auto len = env->GetArrayLength(payload);
  auto body = env->GetByteArrayElements(payload, 0);

  Libmqtt_client_with_will(id, (char *) c_topic, qos, retain, (char *) body, len);

  env->ReleaseStringUTFChars(topic, c_topic);
  env->ReleaseByteArrayElements(payload, body, 0);
}

/*
 * Method:    _setNonePersist
 * Signature: (I)V
 */
JNIEXPORT void JNICALL
Java_org_goiiot_libmqtt_LibMQTT__1setNonePersist
(JNIEnv *env, jclass c, jint id) {

  Libmqtt_client_with_none_persist(id);
}

/*
 * Method:    _setMemPersist
 * Signature: (IIZZ)V
 */
JNIEXPORT void JNICALL
Java_org_goiiot_libmqtt_LibMQTT__1setMemPersist
(JNIEnv *env, jclass c, jint id, jint max_count,
 jboolean ex_drop, jboolean dup_replace) {

  Libmqtt_client_with_mem_persist(id, max_count, ex_drop, dup_replace);
}

/*
 * Method:    _setFilePersist
 * Signature: (ILjava/lang/String;IZZ)V
 */
JNIEXPORT void JNICALL
Java_org_goiiot_libmqtt_LibMQTT__1setFilePersist
(JNIEnv *env, jclass c, jint id, jstring dir,
 jint max_count, jboolean ex_drop, jboolean dup_replace) {

  auto c_dir = env->GetStringUTFChars(dir, 0);

  Libmqtt_client_with_file_persist(id, (char *) c_dir, max_count, ex_drop,
                                   dup_replace);

  env->ReleaseStringUTFChars(dir, c_dir);
}

/*
 * Method:    _setup
 * Signature: (I)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL
Java_org_goiiot_libmqtt_LibMQTT__1setup
(JNIEnv *env, jclass c, jint id) {

  auto err = Libmqtt_setup_client(id);

  if (err != NULL) {
    return env->NewStringUTF(err);
  }

  Libmqtt_set_pub_handler(id, &pub_handler);
  Libmqtt_set_sub_handler(id, &sub_handler);
  Libmqtt_set_net_handler(id, &net_handler);
  Libmqtt_set_unsub_handler(id, &unsub_handler);
  Libmqtt_set_persist_handler(id, &persist_handler);

  return NULL;
}
