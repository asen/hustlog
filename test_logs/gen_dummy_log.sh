#!/usr/bin/env bash

NUM_LINES=${1:-100}
HITS_PER_SEC=${2:-10}
NOW=${3:-`date +"%s"`}

now="$NOW"
hits_this_sec=0
for i in $(seq 1 $NUM_LINES) ; do
  if [ $hits_this_sec -ge $HITS_PER_SEC ] ; then
    let now=$now+1
    hits_this_sec=0
  fi
  let hits_this_sec=$hits_this_sec+1
  my_ts=`date -r $now "+%Y-%m-%dT%H:%M:%S"` # TODO MacOS/BSD -specific?
  echo "$my_ts $i dummy line $i ($now)"
done

