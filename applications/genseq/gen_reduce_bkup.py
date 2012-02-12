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
    paired_reads={}
    unpaired_reads={}
    ispaired = False
    if len(v) > 1:
        for read in v:
            values=read.split()
            if values[-2] == "paired":
                paired_reads[values[-1]]= "\t".join(read.split()[0:len(values)-2])
                ispaired = True
            else:
                unpaired_reads[values[-1]]="\t".join(read.split()[0:len(values)-2])
        if ispaired:
            i=0
            for key in sorted(paired_reads.iterkeys(),reverse=True):
                if i==0:
                    reduce_write.write(paired_reads[key] +"\n")
                else:
                    duplicates.write(paired_reads[key] +"\n")
                i=i+1
        else:
            i=0
            for key in sorted(unpaired_reads.iterkeys(),reverse=True):
                if i==0:
                    reduce_write.write(unpaired_reads[key] +"\n")
                else:
                    duplicates.write(unpaired_reads[key] +"\n")
                i=i+1;
    else:
        reduce_write.write("\t".join((v[0].split())[0:len(v[0].split())-2]) +"\n") 


