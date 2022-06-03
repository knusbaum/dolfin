#!/usr/bin/env bash


touch company.log
touch accession.log
touch fact.log
touch fetch.log

xterm -e "tail -f company.log" &
xterm -e "tail -f accession.log" &
xterm -e "tail -f fact.log" &
xterm -e "tail -f fetch.log" &

#terminator

#go run main.go 
