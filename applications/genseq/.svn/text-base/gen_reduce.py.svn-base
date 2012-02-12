#!/usr/bin/env python

import sys
import os
import pdb

# input comes from STDIN (standard input)
part_files=sys.argv[1]
print part_files
part_list=part_files.split(":")
part_list=part_list[1:]
pairs={}
final_pairs={}

k=str(part_list[0].split("-")[-1:][0])
reduce_path=(os.path.split(part_list[0]))[0]
reduce_file=reduce_path+"/reduce-"+str(k)
duplicates_file=reduce_path+"/duplicates-"+str(k)
reduce_write=open(reduce_file,'w')
duplicates=open(duplicates_file,'w')

for i in part_list:
    print " file name " + i
    part_file=open(i,'r')
    for line in part_file:
        inp=line.split("KEYDELIM")
        key=inp[0].strip()
        setreads=inp[1].strip()
        reads=setreads.split("READDELIM")
        if final_pairs.has_key(key):
            final_pairs[key] = final_pairs[key]+ reads
        else: 
            final_pairs[key] = reads

for k,v in final_pairs.items():
    paired_reads=[]
    unpaired_reads=[]
    ispaired = False
    if len(v) > 1:
        for read in v:
            values=read.split()
            if values[-2] == "paired":
                paired_reads.append("\t".join(read.split()))
                ispaired = True
            else:
                unpaired_reads.append("\t".join(read.split()))
        if ispaired:
            i = 0
            firstread = paired_reads[0]
            max_value = firstread.split()[-1]
            max_value_index=0
            for read in paired_reads:
                values = read.split()
                if values[-1] > max_value:
                    max_value_index = i
                i=i+1
            reduce_write.write("\t".join((paired_reads[max_value_index].split())[0:len(paired_reads[max_value_index].split())-2])+ "\n")
            del paired_reads[max_value_index]
            for i in paired_reads:
                duplicates.write("\t".join((i.split())[0:len(i.split())-2]) +"\n")
        else:
            i=0
            firstread = unpaired_reads[0]
            max_value = firstread.split()[-1]
            max_value_index=0
            for read in unpaired_reads:
                values = read.split()
                if values[-1] > max_value:
                    max_value_index = i
                i=i+1
            reduce_write.write("\t".join((unpaired_reads[max_value_index].split())[0:len(unpaired_reads[max_value_index].split())-2])+ "\n")
            del unpaired_reads[max_value_index]
            for i in unpaired_reads:
                duplicates.write("\t".join((i.split())[0:len(i.split())-2]) +"\n")
    else:
        reduce_write.write("\t".join((v[0].split())[0:len(v[0].split())-2]) +"\n") 

