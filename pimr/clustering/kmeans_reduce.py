#!/usr/bin/env python

import sys
import os

# input files from STDIN (standard input)
partitionFiles=sys.argv[1]
partitionList=partitionFiles.split(":")
keyCount={}
k=str(partitionList[0].split("-")[-1:][0])
reduceFile="reduce-"+str(k)
        
with open(reduceFile, 'w') as reduceWrite:
    for fname in partitionList:
        with open(fname) as infile:
            for line in infile:
                reduceWrite.write(line)