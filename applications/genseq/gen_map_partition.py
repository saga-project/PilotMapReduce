#!/usr/bin/env python

import sys
import os
import pdb

# input comes from STDIN (standard input)
file1=sys.argv[1]
file2=sys.argv[2]
nbr_reduces=sys.argv[3]
application_args=sys.argv[4:]
sorted_part_nbr=[]
f=open(file1,"r")
part_nbr=[[] for _ in range(int(nbr_reduces))]

#Map phase logic starts
k="bwa aln " + application_args[0] + " " + file1 + " > " + file1 +"-aln "
print "executing " + k
os.system(k)
k="bwa aln " + application_args[0] + " " + file2 + " > " + file2 +"-aln "
print "executing " + k
os.system(k)
output_sam=os.path.dirname(file1) + "/" + os.path.split(file1)[1] + "_" + os.path.split(file2)[1] + ".sam"
k="bwa sampe " + application_args[0] + " " + file1 + "-aln" + " " + file2 + "-aln" + " " + file1 + " " + file2 +  " > " + output_sam
print "executing " + k
os.system(k)
output_sam_file = open(output_sam,"r")
# bwa alignment done
pairs={}
#Process the sam file
for line in output_sam_file:
    line=line.strip()
    if not line.startswith("@"):
        fields = line.split()
        # if flag is 77 or 141 - the record & its mate are unmapped
        # so ignore them
        if fields[1] == "77" or fields[1] == "141":
            pass
        else:     
            if ( fields[1] == "99" or fields[1] == "147" 
                   or fields[1] == "83" or fields[1] == "163"
                   or fields[1] == "81" or fields[1] == "161"
                   or fields[1] == "97" or fields[1] == "145"
                   or fields[1] == "65" or fields[1] == "129"
                   or fields[1] == "113" or fields[1] == "177"):
                line = line + " paired "
            else:
                line = line + " lonepair "
            chromosome = fields[2]
            position = fields[3]
            qual_score = fields[10]
            score=0.0
            for c in qual_score:
                score +=ord(c) - 64
            quality = score/len(qual_score)
            line= line + " " + str(quality)
            
            if (fields[1] == "89" or fields[1] == "121" or fields[1] == "181" or fields[1] == "117" 
                or fields[1] == "153" or fields[1] == "185" or fields[1] == "147" or fields[1]=="83"
                or fields[1] == "115" or fields[1] == "179" or fields[1] == "81" or fields[1] == "145"
                or fields[1] == "113" or fields[1] == "177" ):
                strand = "Negative"
            else:
                strand = "positive"
            key = chromosome + position + strand
            value = line
            
            if pairs.has_key(key):
                pairs[key] = pairs[key] + "READDELIM" + value
            else:
                pairs[key] = value

for key in pairs.keys():
    l=hash(key)%int(nbr_reduces)
    part_nbr[l].append(key + "KEYDELIM" + pairs[key] + "\n" )
            

for i in range(0,int(nbr_reduces)):
    p_name="part-"+str(i) 
    sorted_part_nbr.append(open( output_sam + "sorted-"+ p_name,'w'))

for i in range(0,int(nbr_reduces)):
    part_nbr[i].sort()
    for l in part_nbr[i]:
        sorted_part_nbr[i].write(l) 

for i in range(0,int(nbr_reduces)): 
    sorted_part_nbr[i].close()

os.system("rm " + file1 + " " + file2)
os.system("rm " + file1 + "-aln " + file2 + "-aln ")
os.system("rm " + output_sam)
