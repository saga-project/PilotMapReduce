#!/bin/bash

input=$1
chunk_size=$2
h=`hostname`

split -d -b $2  $1 $h$input'-chunk-'
