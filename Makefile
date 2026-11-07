dev:
	@go build -ldflags="-s -w" -o _dist/standalone ./cmd/standalone && cd cmd/standalone && docker compose up --build

cluster:
	@go build -ldflags="-s -w" -o _dist/cluster ./cmd/cluster && cd cmd/cluster && docker compose up --build

build:
	@go build -ldflags="-s -w" -o _dist/pcb ./cmd/standalone

build-cluster:
	@go build -ldflags="-s -w" -o _dist/pcb ./cmd/cluster

test:
	@go test -v

integration:
	@go test ./...

parity:
	@PARITY_CHECK=true go test -v

test-cluster:
	@CLUSTER_CHECK=true go test -v

bench:
	@go test -bench=. -run=_ -v

unit:
	@go test ./... -tags unit -v

cover:
	@mkdir -p _dist
	@go test -coverprofile=_dist/coverage.out -v
	@go tool cover -html=_dist/coverage.out -o _dist/coverage.html

cloc:
	@cloc . --exclude-dir=_example,_dist,internal --exclude-ext=pb.go

cloc-native:
	@cloc . --exclude-dir=_example,_dist,internal,module --exclude-ext=pb.go

cloc-wasm:
	@cloc ./module --exclude-dir=_example,_dist,internal,patch --exclude-ext=pb.go

gen: gen-lite

gen-lite:
	@mkdir -p module/pkg/pb
	@protoc internal.proto \
		--plugin protoc-gen-go-lite="${GOBIN}/protoc-gen-go-lite" \
		--go-lite_out=module/pkg/pb  \
		--go-lite_opt=paths=source_relative \
		--go-lite_opt=features=marshal+unmarshal+size+json \
		-I .

gen-lite-install:
	go install github.com/aperturerobotics/protobuf-go-lite/cmd/protoc-gen-go-lite@latest

wasm-storage-queue:
	cd module/storage-queue && tinygo build -buildmode=wasi-legacy -target=wasi -opt=s -gc=leaking -scheduler=none -o ../../embed/storage-queue.wasm -no-debug
wasm-storage-queue-dev:
	cd module/storage-queue && tinygo build -buildmode=wasi-legacy -target=wasi -opt=2 -gc=leaking -scheduler=none -o ../../embed/storage-queue.dev.wasm
wasm-service-http:
	cd module/service-http && tinygo build -buildmode=wasi-legacy -target=wasi -opt=s -gc=leaking -scheduler=none -o ../../embed/service-http.wasm -no-debug
wasm-service-http-dev:
	cd module/service-http && tinygo build -buildmode=wasi-legacy -target=wasi -opt=2 -gc=leaking -scheduler=none -o ../../embed/service-http.dev.wasm
wasm-control-basic:
	cd module/control-basic && tinygo build -buildmode=wasi-legacy -target=wasi -opt=s -gc=leaking -scheduler=none -o ../../embed/control-basic.wasm -no-debug
wasm-control-basic-dev:
	cd module/control-basic && tinygo build -buildmode=wasi-legacy -target=wasi -opt=2 -gc=leaking -scheduler=none -o ../../embed/control-basic.dev.wasm
wasm-dev: wasm-storage-queue-dev wasm-service-http-dev wasm-control-basic-dev
wasm-prod: wasm-storage-queue wasm-service-http wasm-control-basic
wasm-go: wasm-dev wasm-prod

wasm: wasm-go

work:
	go work use embed
	go work use module/lib
	go work use module/service-http
	go work use module/storage-queue
	go work use module/control-basic

.PHONY: test
