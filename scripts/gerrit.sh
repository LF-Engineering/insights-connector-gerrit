#!/bin/bash
# example run: GERRIT_STREAM=xyz ./scripts/gerrit.sh
# ESENV=prod|test
if [ -z "${ESENV}" ]
then
  ESENV=test
fi
# AWSENV=prod|test|dev
if [ -z "${AWSENV}" ]
then
  AWSENV=dev
fi
export AWS_ACCESS_KEY_ID="`cat ./secrets/AWS_ACCESS_KEY_ID.${AWSENV}.secret`"
export AWS_REGION="`cat ./secrets/AWS_REGION.${AWSENV}.secret`"
export AWS_SECRET_ACCESS_KEY="`cat ./secrets/AWS_SECRET_ACCESS_KEY.${AWSENV}.secret`"
# Other example args:
# --gerrit-url=gerrit.onap.org
# --gerrit-project=net
# --gerrit-user="`cat ./secrets/user.secret`"
# --gerrit-ssh-key="`cat ./secrets/key.secret`"
# --gerrit-ssh-key-path="`cat ./secrets/key-path.secret`"
# --gerrit-disable-host-key-check
# --gerrit-ssh-port=29418
# --gerrit-debug=0
#./gerrit --gerrit-es-url="`cat ./secrets/ES_URL.${ESENV}.secret`" --gerrit-stream='' $*
./gerrit --gerrit-disable-host-key-check --gerrit-es-url="`cat ./secrets/ES_URL.${ESENV}.secret`" $*
