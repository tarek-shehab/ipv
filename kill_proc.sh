#!/bin/bash

for PID in `ps -ef | grep phi_start | awk '{print $2}'`
do
    echo $PID
    kill -9 $PID
done

for PID in `ps -ef | grep chromium-browser | awk '{print $2}'`
do
    echo $PID
    kill -9 $PID
done 