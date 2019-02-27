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

package libmqtt

import "testing"

func Test_SilentLogger(t *testing.T) {
	if l := newLogger(Silent); l != nil {
		t.Error("failed at silent logger")
	} else {
		l.v("verbose")
		l.d("debug")
		l.i("info")
		l.w("warning")
		l.e("error")
	}
}

func Test_ErrorLogger(t *testing.T) {
	if l := newLogger(Error); l == nil ||
		l.error == nil || l.warning != nil ||
		l.info != nil || l.debug != nil || l.verbose != nil {
		t.Error("failed at error logger")
	} else {
		l.v("verbose")
		l.d("debug")
		l.i("info")
		l.w("warning")
		l.e("error")
	}
}

func Test_WarningLogger(t *testing.T) {
	if l := newLogger(Warning); l == nil ||
		l.error == nil || l.warning == nil ||
		l.info != nil || l.debug != nil || l.verbose != nil {
		t.Error("failed at warning logger")
	} else {
		l.v("verbose")
		l.d("debug")
		l.i("info")
		l.w("warning")
		l.e("error")
	}
}

func Test_InfoLogger(t *testing.T) {
	if l := newLogger(Info); l == nil ||
		l.error == nil || l.warning == nil ||
		l.info == nil || l.debug != nil || l.verbose != nil {
		t.Error("failed at info logger")
	} else {
		l.v("verbose")
		l.d("debug")
		l.i("info")
		l.w("warning")
		l.e("error")
	}
}

func Test_DebugLogger(t *testing.T) {
	if l := newLogger(Debug); l == nil ||
		l.error == nil || l.warning == nil ||
		l.info == nil || l.debug == nil || l.verbose != nil {
		t.Error("failed at debug logger")
	} else {
		l.v("verbose")
		l.d("debug")
		l.i("info")
		l.w("warning")
		l.e("error")
	}

}

func Test_VerboseLogger(t *testing.T) {
	if l := newLogger(Verbose); l == nil ||
		l.error == nil || l.warning == nil ||
		l.info == nil || l.debug == nil || l.verbose == nil {
		t.Error("failed at verbose logger")
	} else {
		l.v("verbose")
		l.d("debug")
		l.i("info")
		l.w("warning")
		l.e("error")
	}
}
