#!/usr/bin/env python

import sys
import os

# input comes from STDIN (standard input)
part_files=sys.argv[1]
print part_files
part_list=part_files.split(":")
part_list=part_list[1:]
key_count={}
k=str(part_list[0].split("-")[-1:][0])
reduce_path=(os.path.split(part_list[0]))[0]
reduce_file=reduce_path+"/reduce-"+str(k)
reduce_write=open(reduce_file,'w')

for i in part_list:
    print " file name " + i
    part_file=open(i,'r')
    for line in part_file:
        words=line.split("DELIM,")
        words[1]=words[1].rstrip()
        print str(words)+ "\n"
        if words[0] not in key_count:
           key_count[words[0]] = 0
           key_count[words[0]] = key_count[words[0]]+int(words[1])
        else:
           key_count[words[0]] = key_count[words[0]]+int(words[1])

for k,v in key_count.items():
    reduce_write.write(k+"---"+str(v)+"\n")
