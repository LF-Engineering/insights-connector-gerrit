#!/bin/bash
# clear; GERRIT_TAGS="c,d,e" ./scripts/gerrit.sh --gerrit-date-from "2021-07" --gerrit-date-to "2021-08" --gerrit-pack-size=500 --gerrit-max-reviews=1000 --gerrit-tags="a,b,c" --gerrit-project=ONAP --gerrit-debug=0
clear; ./scripts/gerrit.sh --gerrit-pack-size=1000 --gerrit-max-reviews=1000 --gerrit-project=ONAP
