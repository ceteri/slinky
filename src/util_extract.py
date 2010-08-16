#!/usr/bin/env python
# encoding: utf-8

## Copyright (C) 2010, Paco Nathan. This work is licensed under
## the BSD License. To view a copy of this license, visit:
##    http://creativecommons.org/licenses/BSD/
## or send a letter to:
##    Creative Commons, 171 Second Street, Suite 300
##    San Francisco, California, 94105, USA
##
## @author Paco Nathan <ceteri@gmail.com>

## extract particular kinds of records from MapRedue output
## usage: ./util_extract.py <marker>

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
