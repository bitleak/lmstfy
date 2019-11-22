#!/bin/bash
set -e
if test -z "$TARGET_OS"; then
    uname_S=`uname -s`
    case "$uname_S" in
        Darwin)
            TARGET_OS=darwin
            ;;
        OpenBSD)
            TARGET_OS=openbsd
            ;;
        DragonFly)
            TARGET_OS=dragonfly
            ;;
        FreeBSD)
            TARGET_OS=freebsd
            ;;
        NetBSD)
            TARGET_OS=netbsd
            ;;
        SunOS)
            TARGET_OS=solaris
            ;;
        *)
            TARGET_OS=linux
            ;;
    esac
fi

if test -z "$TARGET_ARCH"; then
    uname_M=`uname -m`
    case "$uname_M" in
        x86_64)
            TARGET_ARCH=amd64
            ;;
        arm*)
            TARGET_ARCH=arm
            ;;
        ppc64*)
            TARGET_ARCH=ppc64
            ;;
        aarch64)
            TARGET_ARCH=arm
            ;;
        i386)
            TARGET_ARCH=386
            ;;
        *)
            TARGET_ARCH=amd64
            ;;
    esac
fi

TARGET_NAME=lmstfy-server
GO_PROJECT=github.com/meitu/lmstfy
BUILD_DIR=./_build
VERSION=`grep "^VERSION" Changelog | head -1 | cut -d " " -f2`
BUILD_DATE=`date -u +'%Y-%m-%dT%H:%M:%SZ'`
GIT_REVISION=`git rev-parse --short HEAD`

GOOS="$TARGET_OS" GOARCH="$TARGET_ARCH" go build -i -v -ldflags \
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
