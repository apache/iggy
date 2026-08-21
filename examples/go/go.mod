module github.com/apache/iggy/examples/go

go 1.25.0

replace github.com/apache/iggy/foreign/go => ../../foreign/go

require github.com/apache/iggy/foreign/go v0.0.0-00010101000000-000000000000

require (
	github.com/avast/retry-go/v5 v5.0.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/klauspost/compress v1.19.2 // indirect
	github.com/klauspost/cpuid/v2 v2.2.10 // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
	golang.org/x/sys v0.30.0 // indirect
)
