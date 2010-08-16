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

## reformat output from "map_parse.py" to support Gephi graphs
## http://gephi.org/users/supported-graph-formats/csv-format/

import sys


def main ():
    # scan input for records to pass through

    for line in sys.stdin:
        line = line.strip()

        try:
            l = line.split("\t")
            send, kind, recv = l

            print ",".join([send, recv])

        except ValueError, err:
            sys.stderr.write("Value ERROR: %(err)s\n%(data)s\n" % {"err": str(err), "data": str(l)})


if __name__ == "__main__":
    main()
