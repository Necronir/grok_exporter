// Copyright 2018 The grok_exporter Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// +build !darwin

package tailer

// old implementation, darwin is already switched to the new implementation, the other OSes will follow
func RunFseventFileTailer(path string, readall bool, failOnMissingFile bool, logger simpleLogger) Tailer {
	return runFileTailer(path, readall, failOnMissingFile, logger, NewFseventWatcher)
}
