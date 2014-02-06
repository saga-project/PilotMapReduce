#!/bin/bash

input=$1
h=`hostname`
 
split -a 5 -b $2  $1 $h$input'-chunk-'
