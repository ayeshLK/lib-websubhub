module github.com/ayeshLK/lib-websubhub/examples/kafka-websubhub

go 1.26.0

require (
	github.com/BurntSushi/toml v1.6.0
	github.com/ayeshLK/lib-websubhub v0.0.0
	github.com/twmb/franz-go v1.22.2
	github.com/twmb/franz-go/pkg/kmsg v1.15.0
)

require (
	github.com/klauspost/compress v1.20.3 // indirect
	github.com/pierrec/lz4/v4 v4.1.33 // indirect
)

replace github.com/ayeshLK/lib-websubhub => ../..
