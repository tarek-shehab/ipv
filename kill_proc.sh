#!/bin/bash

for PID in `ps -ef | grep etl_start | awk '{print $2}'`
do
    echo $PID
    kill -9 $PID
done

#for PID in `ps -ef | grep entity_extractor | awk '{print $2}'`
#do
#    echo $PID
#    kill -9 $PID
#done 