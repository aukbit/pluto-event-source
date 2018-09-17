# event-source-proto
Event Source Protocol Buffer Definition

## Install [Google's protocol buffers](https://github.com/golang/protobuf)
```
$ go get -u google.golang.org/grpc
$ go get -u github.com/golang/protobuf/{proto,protoc-gen-go}
```

## Compile proto file
```
$ protoc eventsource.proto --go_out=plugins=grpc:.
```
