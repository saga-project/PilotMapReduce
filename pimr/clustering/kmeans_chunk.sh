#!/bin/bash

input=$1
h=`hostname`

split -d -l $2  $1 $h$input'-chunk-'
