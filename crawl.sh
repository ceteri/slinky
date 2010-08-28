#!/bin/bash -x

#host=172.16.100.63
host=localhost

./src/consume.py $host:6379:0 flush
./src/consume.py $host:6379:0 config < imvu_config.tsv 
./src/consume.py $host:6379:0 whitelist < imvu_white.tsv 
./src/consume.py $host:6379:0 seed < imvu_seed.tsv 
./src/consume.py $host:6379:0 perform

#./src/consume.py $host:6379:0 stopwords < stopwords
