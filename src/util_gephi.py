#!/usr/bin/env python
# encoding: utf-8

## reformat output from "map_parse.py" to support Gephi graphs
## http://gephi.org/users/supported-graph-formats/csv-format/
##
## author: Paco Nathan <ceteri@gmail.com>

import sys


def main ():
    # scan input for records to pass through

    for line in sys.stdin:
        line = line.strip()

        try:
            l = line.split("\t")
            send, kind, recv, doc_id = l

            print ",".join([send, recv])

        except ValueError, err:
            sys.stderr.write("Value ERROR: %(err)s\n%(data)s\n" % {"err": str(err), "data": str(l)})


if __name__ == "__main__":
    main()
