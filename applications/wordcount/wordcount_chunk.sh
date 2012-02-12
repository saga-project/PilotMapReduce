#!/bin/bash

input_dir=$1
temp_dir=$2
chunk_size=$3

for i in `ls $input_dir`
do
    split -d -b $3  $1"/"$i $2"/"$i
done
