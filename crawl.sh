#!/bin/bash -x

host=localhost

./src/consume.py $host:6379:0 flush
./src/consume.py $host:6379:0 config < config.tsv 
./src/consume.py $host:6379:0 whitelist < white.tsv 
./src/consume.py $host:6379:0 seed < seed.tsv 
./src/consume.py $host:6379:0 perform

#./src/consume.py $host:6379:0 persist
#./src/consume.py $host:6379:0 analyze


