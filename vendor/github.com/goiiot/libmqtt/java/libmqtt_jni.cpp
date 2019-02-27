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

using namespace std;

static jclass mTopicCallbackClass;
static jmethodID on_topic_msg_mid;

// TODO: use concurrent hash map to keep thread safe and multiple handlers
static jobject mTopicCallbackObj;

/*
 * Method:    _handle
 * Signature: (ILjava/lang/String;Lorg/goiiot/libmqtt/LibMQTT/TopicMessageCallback;)V
 */
JNIEXPORT void JNICALL
Java_org_goiiot_libmqtt_LibMQTT__1handle
(JNIEnv *env, jclass clazz, jint id, jstring topic, jobject callback) {

  if (callback == NULL) {
    return;
  }

  mTopicCallbackClass = clazz;
  mTopicCallbackObj = callback;
  on_topic_msg_mid = env->GetMethodID(
                       clazz, "onMessage", "(Ljava/lang/String;I[B)V");

  auto c_topic = env->GetStringUTFChars(topic, 0);

  auto handler = [](int client, char *topic, int qos, char *payload, int size) {
    JNIEnv *g_env;
    jvm->AttachCurrentThread((void **)&g_env, NULL);

    if (g_env == NULL) {
      return;
    } else {
      auto result = g_env->NewByteArray(size);
      g_env->SetByteArrayRegion(result, 0, size, (jbyte *) payload);
      g_env->CallVoidMethod(mTopicCallbackObj, on_topic_msg_mid,
                            g_env->NewStringUTF(topic), qos, result);
    }
  };

  Libmqtt_handle(id, (char *) c_topic, (libmqtt_topic_handler) handler);

  env->ReleaseStringUTFChars(topic, c_topic);
}

/*
 * Method:    _conn
 * Signature: (I)V
 */
JNIEXPORT void JNICALL
Java_org_goiiot_libmqtt_LibMQTT__1connect
(JNIEnv *env, jclass c, jint id) {

  Libmqtt_connect(id, &conn_handler);
}

/*
 * Method:    _wait
 * Signature: (I)V
 */
JNIEXPORT void JNICALL
Java_org_goiiot_libmqtt_LibMQTT__1wait
(JNIEnv *env, jclass c, jint id) {

  env->MonitorEnter(c);
  Libmqtt_wait(id);
  env->MonitorExit(c);
}

/*
 * Method:    _pub
 * Signature: (ILjava/lang/String;I[B)V
 */
JNIEXPORT void JNICALL
Java_org_goiiot_libmqtt_LibMQTT__1pub
(JNIEnv *env, jclass c, jint id, jstring topic, jint qos, jbyteArray payload) {

  auto c_topic = env->GetStringUTFChars(topic, 0);
  auto len = env->GetArrayLength(payload);
  auto body = env->GetByteArrayElements(payload, 0);

  Libmqtt_publish(id, (char *)c_topic, qos, (char *)body, len);

  env->ReleaseStringUTFChars(topic, c_topic);
  env->ReleaseByteArrayElements(payload, body, 0);
}

/*
 * Method:    _sub
 * Signature: (ILjava/lang/String;I)V
 */
JNIEXPORT void JNICALL
Java_org_goiiot_libmqtt_LibMQTT__1sub
(JNIEnv *env, jclass c, jint id, jstring topic, jint qos) {

  auto c_topic = env->GetStringUTFChars(topic, 0);

  Libmqtt_subscribe(id, (char *)c_topic, qos);

  env->ReleaseStringUTFChars(topic, c_topic);
}

/*
 * Method:    _unsub
 * Signature: (ILjava/lang/String;)V
 */
JNIEXPORT void JNICALL
Java_org_goiiot_libmqtt_LibMQTT__1unsub
(JNIEnv *env, jclass c, jint id, jstring topic) {

  auto c_topic = env->GetStringUTFChars(topic, 0);

  Libmqtt_unsubscribe(id, (char *)c_topic);

  env->ReleaseStringUTFChars(topic, c_topic);
}

/*
 * Method:    _destroy
 * Signature: (IZ)V
 */
JNIEXPORT void JNICALL
Java_org_goiiot_libmqtt_LibMQTT__1destroy
(JNIEnv *env, jclass c, jint id, jboolean force) {

  Libmqtt_destroy(id, force);
}