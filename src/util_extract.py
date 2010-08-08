#!/usr/bin/env python
# encoding: utf-8

## extract particular kinds of records from MapRedue output
## usage: ./util_extract.py <marker>
##
## author: Paco Nathan <ceteri@gmail.com>

import sys

def main (marker):
    ## process output from previous MapReduce

    for line in sys.stdin:
        line = line.strip()

        try:
            l = line.split("\t", 2)
            word, kind, val = l

            if kind == marker:
                print line

        except ValueError, err:
            sys.stderr.write("Value ERROR: %(err)s\n%(data)s\n" % {"err": str(err), "data": str(l)})


if __name__ == "__main__":
    marker = sys.argv[1]
    main(marker)
