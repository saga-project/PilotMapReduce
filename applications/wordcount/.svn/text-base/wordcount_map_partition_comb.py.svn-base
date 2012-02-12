#!/usr/bin/env python

import sys
import os
import pdb

# input comes from STDIN (standard input)
file1=sys.argv[1]
nbr_reduces=sys.argv[2]
sorted_part_nbr=[]
k=[]

f=open(file1,"r")
part_nbr=[[] for _ in range(int(nbr_reduces))]

for line in f:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    words = line.split("---")
    # increase counters
    l=hash(words[0])%int(nbr_reduces)
    part_nbr[l].append(words[0] + "DELIM," + str(words[1]) + "\n" )
        

for i in range(0,int(nbr_reduces)):
    p_name="part-"+str(i) 
    sorted_part_nbr.append(open( file1 + "sorted-"+ p_name,'w'))

for i in range(0,int(nbr_reduces)):
    part_nbr[i].sort()
    for l in part_nbr[i]:
        sorted_part_nbr[i].write(l) 

for i in range(0,int(nbr_reduces)): 
    sorted_part_nbr[i].close()

