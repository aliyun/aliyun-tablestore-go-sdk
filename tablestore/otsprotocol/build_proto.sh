protoc \
    --go_out=. \
    --go_opt=paths=source_relative \
    delivery.proto search.proto ots_filter.proto table_store.proto timeseries.proto
