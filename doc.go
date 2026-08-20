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

// Package websubhub implements composable WebSub hub-side protocol mechanics
// for Go applications.
//
// Handler adapts WebSub HTTP requests to typed application callbacks. It owns
// bounded request parsing, subscription and unsubscription admission,
// asynchronous validation, subscriber intent verification, response mapping,
// and lifecycle management. Handler implements [net/http.Handler], so routing,
// middleware, authentication, TLS, and server shutdown remain ordinary
// [net/http] concerns. Inbound topic and callback URLs have percent-encoded
// unreserved characters decoded before application callbacks run.
//
// # Application ownership
//
// The package deliberately does not provide a database or broker abstraction.
// Applications own topic and verified-subscription persistence, atomic renewal
// and unsubscription, mandatory lease expiry, content fan-out and ordering,
// retry and acknowledgement policy, clustering, authorization, quotas,
// observability, and deployment.
//
// [NewHandler] requires [Service.OnSubscriptionVerified] and
// [Service.OnUnsubscriptionVerified]. Successful admission returns HTTP 202
// before asynchronous validation and verification complete; applications
// mutate state only from the verified callbacks.
//
// # Content delivery
//
// [DeliveryClient] snapshots one subscription and performs exactly one HTTP
// delivery attempt through [DeliveryClient.Deliver]. It preserves exact body
// bytes and the complete content type, adds X-Hub-Signature when a secret is
// present, refuses redirects, and bounds response reads. Applications decide
// fan-out, retry, acknowledgement, and dead-letter behavior.
//
// # Publisher extension and discovery
//
// [PublisherClient] and the register, deregister, publish, and event-only
// callbacks implement an optional publisher-to-hub extension. The extension is
// disabled unless [Config.EnablePublisherExtension] is true and is not part of
// the WebSub standard. [AddDiscoveryLinks] appends standards-facing rel=self and
// rel=hub Link header values.
//
// # Security
//
// URL validation confirms absolute HTTP(S) syntax; it is not SSRF protection.
// Applications must enforce destination policy at dial time, authenticate and
// authorize inbound requests, configure TLS, protect persisted subscription
// secrets, apply operational limits, and expire leases. Outbound operations
// propagate context cancellation, refuse redirects, and never mutate a
// caller-supplied [http.Client].
package websubhub
