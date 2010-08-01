#!/usr/bin/python
# encoding: utf-8

import Queue
import base64
import hashlib
import httplib
import json
import robotparser
import redis
import sys
import threading
import time
import urllib2
          

## Redis strategy
## urlencode
# " " -> "+"
# 
## add URL to queue
# 
# if r.setnx(uuid, url):
#     r.sadd("todo", uuid)
# 
## draw from queue
# 
# uuid = r.srandmember("todo")
# 
# if not uuid:
#     # queue is empty; DONE
#     return
# elif r.smove("todo", "pend", uuid):
#     # attempt to fetch URL
#     url = r.get(uuid)
# 
## finalize fetch
# 
# if not redirected:
#     r.setnx(getUUID(norm_uri), norm_uri)
# 
# r.srem("pend", uuid)
# r.hset(norm_uri, "date", date)
# ...
# r.hset(norm_uri, "html", encoded)


## static definitions
      
BOT_NAME = "KwizineLoc/1.0.0"
HTTP_TIMEOUT = 5
REQUEST_SLEEP = 10
MAX_PAGE_LEN = 500000
NUM_WORKER_THREADS = 100


## global variables

debug = True # False

opener = urllib2.build_opener()
opener.addheaders = [('User-agent', BOT_NAME), ('Accept-encoding', 'gzip')]

red_cli = redis.Redis(host='localhost', port=6379, db=0)


## class definitions

class ThreadUrl (threading.Thread):
    """Threaded URL Fetcher"""

    def __init__ (self, queue):
        threading.Thread.__init__(self)
        self.queue = queue


    def getPage (self, url_handle):
        # keep track of the HTTP status and normalized URI
        status = str(url_handle.getcode())
        norm_uri = url_handle.geturl()

        # GET the first MAX_PAGE_LEN bytes of HTML from url
        page = url_handle.read(MAX_PAGE_LEN)

        # determine the content type

        url_info = url_handle.info()
        content_type = "text/html"

        if "content-type" in url_info:
            content_type = url_info["content-type"].split(";", 1)[0].strip()

        # get a date
        date = ""

        if "last-modified" in url_info:
            date = url_info["last-modified"]
        else:
            date = url_info["date"]

        # create an MD5 checksum and Base64 encoding of the content

        m = hashlib.md5()
        m.update(page)
        checksum = m.hexdigest()
        encoded = base64.b64encode(page)

        return status, norm_uri, content_type, date, checksum, encoded, page


    def fetch (self, url):
        status = "0"
        norm_uri = url
        content_type = ""
        date = ""
        checksum = ""
        encoded = ""
        page = ""

        try:
            url_handle = urllib2.urlopen(url, None, HTTP_TIMEOUT)
            status, norm_uri, content_type, date, checksum, encoded, page  = self.getPage(url_handle)

        except httplib.InvalidURL, err:
            sys.stderr.write("HTTP InvalidURL: %(err)s\n%(url)s\n" % {"err": str(err), "url": url})
            print str(err.code)
            status = "400"
        except httplib.BadStatusLine, err:
            sys.stderr.write("HTTP BadStatusLine: %(err)s\n%(url)s\n" % {"err": str(err), "url": url})
            status = "400"
        except httplib.IncompleteRead, err:
            sys.stderr.write("HTTP IncompleteRead: %(err)s\n%(url)s\n" % {"err": str(err), "url": url})
            status = "400"
        except urllib2.HTTPError, err:
            sys.stderr.write("HTTPError: %(err)s\n%(url)s\n" % {"err": str(err), "url": url})
            status = str(err.code)
        except urllib2.URLError, err:
            sys.stderr.write("URLError: %(err)s\n%(url)s\n" % {"err": str(err), "url": url})
            status = str(err.code)
        except IOError, err:
            sys.stderr.write("IOError: %(err)s\n%(url)s\n" % {"err": str(err), "url": url})
            status = str(err.code)
        except ValueError, err:
            # unknown url type: http
            sys.stderr.write("ValueError: %(err)s\n%(url)s\n" % {"err": str(err), "url": url})
            status = "400"
        else:
            if debug:
                print "SUCCESS:", url

        return status, norm_uri, content_type, date, checksum, encoded, page


    def run (self):
        while True:
            # grab next url from queue and attempt to fetch its HTML content
            [uuid, url] = self.queue.get()
            status, norm_uri, content_type, date, checksum, encoded, page = self.fetch(url)
            norm_uuid, norm_uri = getUUID(norm_uri)

            if debug:
                print norm_uri, json.dumps([status, content_type, date, str(len(page)), checksum])

            # update the Page Store

            red_cli.srem("pend", uuid)

            if status.startswith("3"):
                # redirect / don't mark as "visited"
                pass
            else:
                red_cli.setnx(norm_uuid, norm_uri)

            red_cli.hset(norm_uri, "status", status)
            red_cli.hset(norm_uri, "content_type", content_type)
            red_cli.hset(norm_uri, "date", date)
            red_cli.hset(norm_uri, "page_len", str(len(page)))
            red_cli.hset(norm_uri, "checksum", checksum)
            red_cli.hset(norm_uri, "html", encoded)

            # after a "throttle" wait period, signal to queue that job is done
            time.sleep(REQUEST_SLEEP)
            self.queue.task_done()
          

def spawnThreads (queue, num_threads):
    ## spawn a pool of threads, passing them the queue instance

    for i in range(num_threads):
        t = ThreadUrl(queue)
        t.setDaemon(True)
        t.start()


def getUUID (url):
    url = url.replace(" ", "+")

    m = hashlib.md5()
    m.update(url)
    uuid = m.hexdigest()

    return uuid, url


def loadQueue (queue):
    ## populate queue with data (potentially, input from mapper+shuffle)

    for line in sys.stdin:
        try:
            [url] = line.strip().split("\t")
            uuid, url = getUUID(url)

            if debug:
                print "UUID", uuid, url

            queue.put([uuid, url])

        except ValueError:
            print "ERROR", line
            # ignore these (for now)
            pass


def main ():
    queue = Queue.Queue()

    spawnThreads(queue, NUM_WORKER_THREADS)
    loadQueue(queue)

    ## wait on the queue until everything has been processed
    queue.join()


if __name__ == "__main__":
    main()
