# protoc version: lib protoc 3.21.12
go install github.com/golang/protobuf/protoc-gen-go
protoc --version
protoc --go_out=. search.proto ots_filter.proto table_store.proto
