# protoc version: lib protoc 3.21.12
# protoc-gen-go version: github.com/golang/protobuf/protoc-gen-go@v1.3.2

script_path=$(cd `dirname $0`; pwd)

go install github.com/golang/protobuf/protoc-gen-go@v1.3.2
cd ~/go/pkg/mod/github.com/golang/protobuf@v1.3.2/protoc-gen-go/
sudo go build
protoc_gen_go_path=$(pwd)/protoc-gen-go

echo "protoc-gen-go plugin path: $protoc_gen_go_path"

cd $script_path
protoc --version
protoc --plugin=protoc-gen-go=$protoc_gen_go_path --go_out=. search.proto ots_filter.proto table_store.proto
