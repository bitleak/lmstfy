#!/bin/bash
set -e
TARGET_NAME=lmstfy-server
GO_PROJECT=github.com/bitleak/lmstfy
BUILD_DIR=./_build
VERSION=`grep "^VERSION" Changelog | head -1 | cut -d " " -f2`
BUILD_DATE=`date -u +'%Y-%m-%dT%H:%M:%SZ'`
GIT_REVISION=`git rev-parse --short HEAD`

go build -v -ldflags \
    "-X $GO_PROJECT/version.Version=$VERSION -X $GO_PROJECT/version.BuildDate=$BUILD_DATE -X $GO_PROJECT/version.BuildCommit=$GIT_REVISION" \
    -o ${TARGET_NAME} ${GO_PROJECT}/server
if [[ $? -ne 0 ]]; then
    echo "Failed to build $TARGET_NAME"
    exit 1
fi
echo "Build $TARGET_NAME, OS is $TARGET_OS, Arch is $TARGET_ARCH"

rm -rf ${BUILD_DIR}
mkdir -p ${BUILD_DIR}
mv lmstfy-server ${BUILD_DIR}

echo "Building $TARGET_NAME succeeded, artifact is in ${BUILD_DIR}"
