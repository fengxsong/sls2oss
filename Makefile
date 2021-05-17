SHORT_NAME ?= sls2oss

BUILD_DATE = $(shell date -u +'%Y-%m-%dT%H:%M:%SZ')
HASH = $(shell git describe --dirty --tags --always)
VERSION ?= unknown
REPO = gl.weeget.cn/devops/sls2oss

BUILD_PATH = main.go
OUTPUT_PATH = build/_output/bin/$(SHORT_NAME)

LDFLAGS := -s -X ${REPO}/internal/version.buildDate=${BUILD_DATE} \
	-X ${REPO}/internal/version.gitCommit=${HASH} \
	-X ${REPO}/internal/version.version=${VERSION}

IMAGE_REPO ?= fengxsong/${SHORT_NAME}
IMAGE_TAG ?= ${HASH}
IMAGE := ${IMAGE_REPO}:${IMAGE_TAG}

tidy:
	go mod tidy

vendor: tidy
	go mod vendor

bin:
	CGO_ENABLED=0 go build -a -installsuffix cgo -ldflags "${LDFLAGS}" -o ${OUTPUT_PATH}-$$(go env GOOS)-$$(go env GOARCH) ${BUILD_PATH} || exit 1

linux-bin:
	GOOS=linux CGO_ENABLED=0 go build -a -installsuffix cgo -ldflags "${LDFLAGS}" -o ${OUTPUT_PATH}-linux-$$(go env GOARCH) ${BUILD_PATH} || exit 1

upx:
	upx ${OUTPUT_PATH}

# Build the docker image
docker-build:
	docker build --rm --build-arg=LDFLAGS="${LDFLAGS}" -t ${IMAGE} -t ${IMAGE_REPO}:latest -f Dockerfile .

# Push the docker image
docker-push:
	docker push ${IMAGE}