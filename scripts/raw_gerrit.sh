#!/bin/bash
ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i ./secrets/sshkey.secret -p 29418 "`cat ./secrets/user.secret`@gerrit.fd.io" gerrit query after:"2022-01-01 00:00:00" before:"2100-01-01 00:00:00" 'limit: 1000' '(status:open OR status:closed)' --all-approvals --all-reviewers --comments --format=JSON | jq -rS '.' > output.json.secret
