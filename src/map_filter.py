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

## mapper to apply high-pass filter on "red_idf.py" output
## based on threshold determined by "thresh.R"

import sys


def main ():
    # scan input for records to pass through

    for line in sys.stdin:
        line = line.strip()

        try:
            l = line.split("\t", 2)
            key, kind, val = l

            if kind in ["i", "p", "m"]:
                print line

        except ValueError, err:
            sys.stderr.write("Value ERROR: %(err)s\n%(data)s\n" % {"err": str(err), "data": str(l)})


if __name__ == "__main__":
    main()
