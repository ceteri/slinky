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

import re
import string
import sys


if __name__ == "__main__":

    proto_pat = re.compile("^https?\:\/\/([\w\-\.\_]+)(\/.*)$")

    for line in sys.stdin:
        try:
            line = line.strip()
            meta = line.split("\t", 7)

            uri, kind, status, uuid, crawl_time, page_len, domain, reason = meta
            print "#", uri

            m = re.match(proto_pat, uri)

            if not m:
                sys.stderr.write("BAD URL ERROR: " + uri + "\n")
            else:
                domain = m.group(1)
                path_text = m.group(2)
                path_list = []

                query_text = ""
                query_split = string.find(path_text, "?")

                if query_split > 0:
                    query_text = path_text[(query_split + 1):-1]
                    path_text = path_text[0:query_split]

                path_list = path_text.split("/")
                path_list.pop(0)
                path_list.insert(0, domain)

                print "\t".join([uuid, "p"] + path_list)
                print "\t".join([uuid, "q", query_text])

        except ValueError, err:
            sys.stderr.write("ValueError: %(err)s\n%(data)s\n" % {"err": str(err), "data": line})
        except IndexError, err:
            sys.stderr.write("IndexError: %(err)s\n%(data)s\n" % {"err": str(err), "data": line})
