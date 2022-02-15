#!/bin/bash
export AWS_REGION="`cat ./secrets/AWS_REGION.dev.secret`"
export AWS_ACCESS_KEY_ID="`cat ./secrets/AWS_ACCESS_KEY_ID.dev.secret`"
export AWS_SECRET_ACCESS_KEY="`cat ./secrets/AWS_SECRET_ACCESS_KEY.dev.secret`"
export ENCRYPTION_KEY="`cat ./secrets/ENCRYPTION_KEY.dev.secret`"
export ENCRYPTION_BYTES="`cat ./secrets/ENCRYPTION_BYTES.dev.secret`"
export ESURL="`cat ./secrets/ES_URL.prod.secret`"
export STREAM=''
export GERRIT_NO_INCREMENTAL=1
#curl -s -XPOST -H 'Content-Type: application/json' "${ESURL}/last-update-cache/_delete_by_query" -d'{"query":{"term":{"key.keyword":"Gerrit:gerrit.fd.io"}}}' | jq -rS '.' || exit 1
../insights-datasource-github/encrypt "`cat ./secrets/sshkey.secret | base64 -w0`" > ./secrets/sshkey.encrypted.secret || exit 2
../insights-datasource-github/encrypt "`cat ./secrets/user.secret`" > ./secrets/user.encrypted.secret || exit 3
./gerrit --gerrit-disable-host-key-check --gerrit-es-url="${ESURL}" --gerrit-debug=0 --gerrit-user="`cat ./secrets/user.encrypted.secret`" --gerrit-ssh-key="`cat ./secrets/sshkey.encrypted.secret`" --gerrit-stream="${STREAM}" --gerrit-url=gerrit.fd.io --gerrit-pack-size=1000 --gerrit-max-reviews=1000
