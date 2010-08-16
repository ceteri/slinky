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

import redis
import sys
import time


if __name__ == "__main__":
    red_cli = redis.Redis(host="localhost", port=6379, db=1)
    meta_keys = [ "uuid", "html" ]

    while True:
        uri = red_cli.rpop("needs.text")

        if not uri:
            time.sleep(1800)
        else:
            try:
                uuid, html = red_cli.hmget(uri, meta_keys)

                if uuid and html:
                    print "\t".join([uuid, html])
                    red_cli.hdel(uri, "html")
            except TypeError, err:
                sys.stderr.write("TypeError: %(err)s\n%(data)s\n" % {"err": str(err), "data": uri})
