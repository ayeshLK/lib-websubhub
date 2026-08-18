// Copyright 2026 Ayesh Almeida
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package websubhub

import "net/http"

// AddDiscoveryLinks appends WebSub self and hub links without replacing
// existing Link header values.
func AddDiscoveryLinks(header http.Header, self string, hubs ...string) error {
	if header == nil {
		return invalidRequest("header must not be nil")
	}
	if _, err := validateHTTPURL(self, "self URL", false, false); err != nil {
		return err
	}
	if len(hubs) == 0 {
		return invalidRequest("at least one hub URL is required")
	}
	for _, hub := range hubs {
		if _, err := validateHTTPURL(hub, "hub URL", true, true); err != nil {
			return err
		}
	}
	header.Add("Link", encodedLink(self, "self"))
	for _, hub := range hubs {
		header.Add("Link", encodedLink(hub, "hub"))
	}
	return nil
}
