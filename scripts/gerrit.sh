#!/bin/bash
# ESENV=prod|test
if [ -z "${ESENV}" ]
then
  ESENV=test
fi
./gerrit --gerrit-url='gerrit.onap.org' --gerrit-es-url="`cat ./secrets/ES_URL.${ESENV}.secret`" --gerrit-user="`cat ./secrets/user.secret`" --gerrit-ssh-key="`cat ./secrets/key.secret`" --gerrit-ssh-key-path='$HOME/.ssh/id_rsa' --gerrit-disable-host-key-check $*
