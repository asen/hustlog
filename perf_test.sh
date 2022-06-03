#!/usr/bin/env bash

for n in 10 100 500 ; do
  fname=test_logs/test_${n}k.log
  if ! test -f $fname ; then
    echo -n "File $fname does not exist - generating (can take a while) ..."
    test_logs/gen_dummy_log.sh ${n}000 > $fname
    echo " Done"
  fi
done

cargo build --release

for n in 10 100 500 ; do
  fname=test_logs/test_${n}k.log
  time ./target/release/hustlog -i $fname -c config_examples/dummy.yml "$@" >/dev/null
done
