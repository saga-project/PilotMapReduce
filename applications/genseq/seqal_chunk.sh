#!/bin/bash

input=$1
chunk_size=$2
h=`hostname`

split -d -l $chunk_size  $input $h$input'-chunk-'
