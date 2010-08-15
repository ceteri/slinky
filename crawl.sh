#!/bin/bash -x

host=172.16.100.63

./src/slinky.py $host:6379:1 flush
./src/slinky.py $host:6379:1 config < imvu_config.tsv 
./src/slinky.py $host:6379:1 whitelist < imvu_white.tsv 
./src/slinky.py $host:6379:1 stopwords < stopwords
./src/slinky.py $host:6379:1 seed < imvu_seed.tsv 
./src/slinky.py $host:6379:1 crawl
