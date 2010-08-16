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

## reducer to calculate TF-IDF from "map_parse.py" output
## http://en.wikipedia.org/wiki/Tf–idf

import math
import sys


class Word ():
    "Aggregate metadata for a parsed/extracted term"

    def __init__ (self, term):
        self.term = term
        self.max_tfidf = 0.0
        self.recs = []


    def add (self, kind, val):
        ## collect data records

        if kind == "m":
            self.max_tfidf = float(val)

        elif kind in ["i", "p"]:
            self.recs.append("\t".join([kind, val]))


    def emit (self, tfidf_thresh):
        ## apply high-pass filter

        if self.max_tfidf >= tfidf_thresh:
            for rec in self.recs:
                print "\t".join([self.term, rec])


def main (tfidf_thresh):
    ## process input from mapper+shuffle

    word_list = {}

    for line in sys.stdin:
        line = line.strip()

        try:
            l = line.split("\t", 2)
            word, kind, val = l

            if word not in word_list:
                w = Word(word)
                word_list[word] = w
            else:
                w = word_list[word]

            w.add(kind, val)

        except ValueError, err:
            sys.stderr.write("Value ERROR: %(err)s\n%(data)s\n" % {"err": str(err), "data": str(l)})

    ## emit results

    for word, w in word_list.items():
        w.emit(tfidf_thresh)


if __name__ == "__main__":
    tfidf_thresh = float(sys.argv[1])
    main(tfidf_thresh)
