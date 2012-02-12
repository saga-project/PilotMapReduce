#!/bin/bash

# Cleaning old temp & output files
rm /N/u/pmantha/gen_data_40/reduce*
rm /N/u/pmantha/gen_data_40/*-*
rm /N/u/pmantha/output/*
rm /N/u/pmantha/temp/*

# Create working directory ( Need to create manually for 1.5.3 version of saga
# Fixed in 1.6 version
mkdir /N/u/pmantha/MapReduce_Python/source/agent

#Executing MapReduce application with all the inputs.
# use python MapReduceApp.py --help for help
# if you use -p option then make sure you have password less login enabled to the target location.

echo " Starting MapReduce"
#k=`expr 64 \* 1024 \* 1024`
k=2500000
echo $k

python genApp.py -i "file://localhost//N/u/pmantha/data" -o  "file://localhost/N/u/pmantha/output" -t "file://localhost/N/u/pmantha/temp" -b 8 -c $k -m "/N/u/pmantha/MapReduce_Python/source/applications/genseq/gen_map_partition.py" -r "/N/u/pmantha/MapReduce_Python/source/applications/genseq/gen_reduce.py" -a "advert://advert.cct.lsu.edu:8080" -u "pbs-ssh://india.futuregrid.org" -n 1 -w 20 -s 8 -d "/N/u/pmantha/MapReduce_Python/source/agent" -q None -x None -y None -e 8 -g /N/u/pmantha/hg/hg19.fa 
