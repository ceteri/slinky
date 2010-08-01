#!/usr/bin/python
# encoding: utf-8

from BeautifulSoup import BeautifulSoup, SoupStrainer
import Queue
import base64
import hashlib
import httplib
import robotparser
import redis
import sys
import threading
import time
import urllib2
          

## static definitions
      
TODO_QUEUE = "todo"
PEND_QUEUE = "pend"

NUM_WORKER_THREADS = 100
OVER_BOOK = 2
REQUEST_SLEEP = 10

USER_AGENT = "KwizineLoc/1.0.0"
HTTP_TIMEOUT = 5
MAX_PAGE_LEN = 500000


## global variables

debug_level = 4 # 0

red_cli = None

opener = urllib2.build_opener()
opener.addheaders = [('User-agent', USER_AGENT), ('Accept-encoding', 'gzip')]


## class definitions

class ThreadUrl (threading.Thread):
    """Threaded URL Fetcher"""

    def __init__ (self, local_queue):
        ## initialize this Thread

        threading.Thread.__init__(self)
        self.local_queue = local_queue


    def getPage (self, url_handle):
        ## get HTTP headers and HTML content from a URL handle

        # keep track of the HTTP status and normalized URI
        status = str(url_handle.getcode())
        norm_uri = url_handle.geturl()

        # GET the first MAX_PAGE_LEN bytes of HTML from url
        raw_html = url_handle.read(MAX_PAGE_LEN)

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
        m.update(raw_html)
        checksum = m.hexdigest()
        b64_html = base64.b64encode(raw_html)

        return status, norm_uri, content_type, date, checksum, b64_html, raw_html


    def fetch (self, orig_url):
        ## attempt to fetch the given URL, collecting the status code

        status = "0"
        norm_uri = orig_url
        content_type = ""
        date = ""
        checksum = ""
        b64_html = ""
        raw_html = ""

        # status codes based on HTTP/1.1 spec in RFC 2616:
        # http://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html

        try:
            url_handle = urllib2.urlopen(orig_url, None, HTTP_TIMEOUT)
            status, norm_uri, content_type, date, checksum, b64_html, raw_html  = self.getPage(url_handle)

        except httplib.InvalidURL, err:
            sys.stderr.write("HTTP InvalidURL: %(err)s\n%(data)s\n" % {"err": str(err), "data": orig_url})
            print str(err.code)
            status = "400"
        except httplib.BadStatusLine, err:
            sys.stderr.write("HTTP BadStatusLine: %(err)s\n%(data)s\n" % {"err": str(err), "data": orig_url})
            status = "400"
        except httplib.IncompleteRead, err:
            sys.stderr.write("HTTP IncompleteRead: %(err)s\n%(data)s\n" % {"err": str(err), "data": orig_url})
            status = "400"
        except urllib2.HTTPError, err:
            sys.stderr.write("HTTPError: %(err)s\n%(data)s\n" % {"err": str(err), "data": orig_url})
            status = str(err.code)
        except urllib2.URLError, err:
            sys.stderr.write("URLError: %(err)s\n%(data)s\n" % {"err": str(err), "data": orig_url})
            status = str(err.code)
        except IOError, err:
            sys.stderr.write("IOError: %(err)s\n%(data)s\n" % {"err": str(err), "data": orig_url})
            status = str(err.code)
        except ValueError, err:
            # unknown url type: http
            sys.stderr.write("ValueError: %(err)s\n%(data)s\n" % {"err": str(err), "data": orig_url})
            status = "400"
        else:
            if debug_level > 0:
                print "SUCCESS:", orig_url

        return status, norm_uri, content_type, date, checksum, b64_html, raw_html


    def getOutLinks (self, raw_html):
        ## scan for outbound web links in the fetched HTML content

        out_links = set([])

        for link in BeautifulSoup(raw_html, parseOnlyThese=SoupStrainer("a")):
            if link.has_key("href"):
                l = link["href"]

                if len(l) > 0 and not l.startswith("javascript") and not l.startswith("#"):
                    out_links.add(l)

        if debug_level > 4:
            print out_links

        return out_links


    def dequeueTask (self):
            # grab next URL from Queue and attempt to fetch its HTML content

            [uuid, orig_url] = self.local_queue.get()

            # TODO: determine domain from the orig_url
            # TODO: fetch/apply "robots.txt" restrictions

            status, norm_uri, content_type, date, checksum, b64_html, raw_html = self.fetch(orig_url)
            norm_uuid, norm_uri = getUUID(norm_uri)
            out_links = self.getOutLinks(raw_html)

            if debug_level > 0:
                print norm_uri, status, content_type, date, str(len(raw_html)), checksum

            # update the Page Store with fetched/analyzed data

            red_cli.srem(PEND_QUEUE, uuid)

            if status.startswith("3"):
                # TODO: redirect / push onto Queue
                pass
            else:
                # mark as "visited"
                red_cli.setnx(norm_uuid, norm_uri)

            red_cli.hset(norm_uri, "uuid", norm_uuid)
            red_cli.hset(norm_uri, "status", status)
            red_cli.hset(norm_uri, "date", date)
            red_cli.hset(norm_uri, "content_type", content_type)
            red_cli.hset(norm_uri, "page_len", str(len(raw_html)))
            red_cli.hset(norm_uri, "checksum", checksum)
            red_cli.hset(norm_uri, "html", b64_html)

            # TODO: update the orig_url -> norm_uri mapping

            # TODO: outbound links require another lookup to resolve orig_url -> norm_uri mapping

            red_cli.hset(norm_uri, "out_links", "\t".join(out_links))

            return out_links


    def run (self):
        while True:
            # TODO: enqueue outbound links which fit white-listed domain rules
            out_links = self.dequeueTask()

            # TODO: adjust "throttle" based on aggregate server load (?? Yan, must define)
            # after "throttled" wait period, signal to queue that the task completed

            time.sleep(REQUEST_SLEEP)
            self.local_queue.task_done()
          

######################################################################

def initRedis (host_port_db):
    ## set up the Redis client

    host, port, db = host_port_db.split(":")
    return redis.Redis(host=host, port=int(port), db=int(db))


def flush (all):
    ## flush either all of the key/value store, or just the Queue

    if all:
        red_cli.flushdb()
    else:
        red_cli.delete(TODO_QUEUE)
        red_cli.delete(PEND_QUEUE)


def seed ():
    ## seed the Queue with root URLs as starting points

    for line in sys.stdin:
        try:
            line = line.strip()
            [url] = line.split("\t")
            uuid, url = getUUID(url)

            if debug_level > 0:
                print "UUID", uuid, url

            if red_cli.setnx(uuid, url):
                red_cli.sadd(TODO_QUEUE, uuid)

        except ValueError, err:
            sys.stderr.write("ValueError: %(err)s\n%(data)s\n" % {"err": str(err), "data": line})


def spawnThreads (local_queue, num_threads):
    ## spawn a pool of threads, passing them the local queue instance

    for i in range(num_threads):
        t = ThreadUrl(local_queue)
        t.setDaemon(True)
        t.start()


def getUUID (url):
    ## clean URL so it can be used as a unique key, then make an MD5 has as a UUID

    url = url.replace(" ", "+")

    m = hashlib.md5()
    m.update(url)
    uuid = m.hexdigest()

    return uuid, url


def drawQueue (local_queue, queue_book_len):
    ## draw from the Queue to populate local queue with URL fetch tasks

    for n in range(1, queue_book_len):
        uuid = red_cli.srandmember(TODO_QUEUE)

        if not uuid:
            # queue is empty; reached our DONE conditions, so we exit

            if debug_level > 0:
                print "DONE: Queue is now empty"

            return False
        elif red_cli.smove(TODO_QUEUE, PEND_QUEUE, uuid):
            # attempt to fetch URL

            url = red_cli.get(uuid)

            if debug_level > 0:
                print "UUID", uuid, url

            local_queue.put([uuid, url])

    # could be more iterations; not DONE
    return True


def crawl ():
    ## draw URL fetch tasks from Queue, populate local queue, run tasks in parallel

    local_queue = Queue.Queue()
    spawnThreads(local_queue, NUM_WORKER_THREADS)

    while True:
        has_more = drawQueue(local_queue, NUM_WORKER_THREADS * OVER_BOOK)

        # wait on local queue until everything has been processed
        local_queue.join()

        if not has_more:
            break


if __name__ == "__main__":
    # verify command line usage

    if len(sys.argv) != 3:
        print "Usage: slinky.py host:port:db [ 'flush' | 'seed' | 'whitelist' | 'crawl' ] < input.txt"
    else:
        # parse command line options

        red_cli = initRedis(sys.argv[1])
        mode = sys.argv[2]

        if mode == "flush":
            # flush data from the key/value store
            flush(True) # False
        elif mode == "seed":
            # seed the Queue with root URLs as starting points
            seed()
        elif mode == "whitelist":
            # TODO: implement white-listed domain rules as a Redis set
            pass
        elif mode == "crawl":
            # populate local queue and crawl those URLs
            crawl()
