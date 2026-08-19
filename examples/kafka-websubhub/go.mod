module github.com/ayeshLK/lib-websubhub/examples/kafka-websubhub

go 1.25.0

require (
	github.com/ayeshLK/lib-websubhub v0.0.0
	github.com/twmb/franz-go v1.21.6
)

require (
	github.com/klauspost/compress v1.18.7 // indirect
	github.com/pierrec/lz4/v4 v4.1.26 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.13.1 // indirect
)

replace github.com/ayeshLK/lib-websubhub => ../..
