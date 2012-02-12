#!/bin/bash

# Cleaning old temp & output files
rm /N/u/pmantha/data/reduce*
rm /N/u/pmantha/data/*txt*-*
rm /N/u/pmantha/output/*
rm /N/u/pmantha/temp/*
m="$1"

# Create working directory ( Need to create manually for 1.5.3 version of saga
# Fixed in 1.6 version
mkdir /N/u/pmantha/pmantha/MapReduce_Python/source/agent

#Executing MapReduce application with all the inputs.
# use python wordcountApp.py --help for help
# if you use -p option then make sure you have password less login enabled to the target location.

echo " Starting MapReduce"
k=`expr 64 \* 1024 \* 1024`
echo $k

python wordcountApp.py -i "file://localhost//N/u/pmantha/data" -o  "file://localhost/N/u/pmantha/output" -t "file://localhost/N/u/pmantha/temp" -b 32 -c $k -m "/N/u/pmantha/MapReduce_Python/source/applications/wordcount/wordcount_map_partition.py" -r "/N/u/pmantha/MapReduce_Python/source/applications/wordcount/wordcount_reduce.py" -a "advert://advert.cct.lsu.edu:8080" -u "pbs-ssh://localhost" -n 2 -w 40 -s 8 -d "/N/u/pmantha/MapReduce_Python/source/agent" -q None -x None -y None -e 1
