#!/usr/bin/env bash -x


touch company.log
touch accession.log
touch fact.log
touch fetch.log

osascript -e "tell application \"Terminal\" to do script \"tail -f ${PWD}/company.log\""
osascript -e "tell application \"Terminal\" to do script \"tail -f ${PWD}/accession.log\""
osascript -e "tell application \"Terminal\" to do script \"tail -f ${PWD}/fact.log\""
osascript -e "tell application \"Terminal\" to do script \"tail -f ${PWD}/fetch.log\""

#go run main.go 



