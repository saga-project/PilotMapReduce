#!/bin/bash

input=$1
h=`hostname`
 
split -a 5 -l $2  $1 $h$input'-chunk-'
