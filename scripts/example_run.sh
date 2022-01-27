#!/bin/bash
# clear; ./scripts/gerrit.sh --gerrit-debug=0 --gerrit-pack-size=1000 --gerrit-max-reviews=1000 --gerrit-user="`cat ./secrets/user.secret`" --gerrit-ssh-key="`cat ./secrets/sshkey.secret`" --gerrit-date-from=2021 --gerrit-stream=''
# clear; ./scripts/gerrit.sh --gerrit-debug=0 --gerrit-pack-size=1000 --gerrit-max-reviews=1000 --gerrit-user="`cat ./secrets/user.secret`" --gerrit-ssh-key-path="./secrets/sshkey.secret" --gerrit-date-from=2021 --gerrit-stream=''
clear; ./scripts/gerrit.sh --gerrit-url=gerrit.fd.io --gerrit-debug=0 --gerrit-pack-size=1000 --gerrit-max-reviews=1000 --gerrit-user="`cat ./secrets/user.secret`" --gerrit-ssh-key-path="./secrets/sshkey.secret" --gerrit-date-from=2022-01-15
