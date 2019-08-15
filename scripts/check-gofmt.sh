#!/bin/sh

badFormatFiles=`go list ./... | sed -e 's=github.com/meitu/lmstfy/=./=' | xargs -n 1 gofmt -l`

echo "$badFormatFiles" | grep "\.go" 1>/dev/null 2>/dev/null
if [ $? -eq 0 ]; then
    # found something
    echo "\n\n###### bad format files ######"
    echo "$badFormatFiles"
    echo "##############################\n\n"

    go list ./... | sed -e 's=github.com/meitu/lmstfy/=./=' | xargs -n 1 gofmt -d

    exit 1
fi

exit 0
