#!/usr/bin/python
# encoding: utf-8

## Copyright (C) 2010, Paco Nathan. This work is licensed under
## the BSD License. To view a copy of this license, visit:
##    http://creativecommons.org/licenses/BSD/
## or send a letter to:
##    Creative Commons, 171 Second Street, Suite 300
##    San Francisco, California, 94105, USA
##
## @author Paco Nathan <ceteri@gmail.com>


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
          

## crawler parameters, override via CLI config

DEBUG_LEVEL = 4 # 0

TODO_QUEUE_KEY = "todo"
PEND_QUEUE_KEY = "pend"
NORM_URI_KEY = "norm"
WHITE_LIST_KEY = "white"

NUM_THREADS = 2 # 100
OVER_BOOK = 1
REQUEST_SLEEP = 2

USER_AGENT = "KwizineLoc/1.0.0"
HTTP_TIMEOUT = 5
MAX_PAGE_LEN = 500000


## global variables

red_cli = None
opener = None
domain_dict = {}
white_list = set([])


## class definitions

class ThreadUri (threading.Thread):
    """Threaded URI Fetcher"""

    def __init__ (self, local_queue):
        ## initialize this Thread

        threading.Thread.__init__(self)
        self.local_queue = local_queue


    def checkRobots (self, domain, uri):
        ## check "robots.txt" permission for this domain

        if domain in domain_dict:
            rp = domain_dict[domain]
        else:
            rp = robotparser.RobotFileParser()
            rp.set_url("http://" + domain + "/robots.txt")
            rp.read()

            domain_dict[domain] = rp

        is_allowed = rp.can_fetch(USER_AGENT, uri)

        if DEBUG_LEVEL > 0:
            print "ROBOTS", is_allowed, uri

        return is_allowed


    def getPage (self, url_handle):
        ## get HTTP headers and HTML content from a URI handle

        # keep track of the HTTP status and normalized URI
        status = str(url_handle.getcode())
        norm_uri = url_handle.geturl()

        # GET the first MAX_PAGE_LEN bytes of HTML
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


    def fetch (self, domain, orig_url):
        ## attempt to fetch the given URI, collecting the status code

        status = "403"
        norm_uri = orig_url
        content_type = ""
        date = ""
        checksum = ""
        b64_html = ""
        raw_html = ""

        try:
            # apply "robots.txt" restrictions, fetch if allowed

            if self.checkRobots(domain, orig_url):
                url_handle = urllib2.urlopen(orig_url, None, HTTP_TIMEOUT)
                status, norm_uri, content_type, date, checksum, b64_html, raw_html  = self.getPage(url_handle)

            # status codes based on HTTP/1.1 spec in RFC 2616:
            # http://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html

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
            if DEBUG_LEVEL > 0:
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

        if DEBUG_LEVEL > 4:
            print out_links

        return out_links


    def dequeueTask (self):
            # pop random/next from URI Queue and attempt to fetch HTML content

            [domain, uuid, orig_url] = self.local_queue.get()

            status, norm_uri, content_type, date, checksum, b64_html, raw_html = self.fetch(domain, orig_url)
            norm_uuid, norm_uri = getUUID(norm_uri)
            out_links = self.getOutLinks(raw_html)

            if DEBUG_LEVEL > 0:
                print domain, norm_uri, status, content_type, date, str(len(raw_html)), checksum

            # update the Page Store with fetched/analyzed data

            red_cli.srem(PEND_QUEUE_KEY, uuid)

            if status.startswith("3"):
                # push redirected link onto URI Queue
                self.enqueueLink(norm_uuid, norm_uri)
            else:
                # mark as "visited"
                red_cli.setnx(norm_uuid, norm_uri)

            red_cli.hset(norm_uri, "uuid", norm_uuid)
            red_cli.hset(norm_uri, "domain", domain)
            red_cli.hset(norm_uri, "status", status)
            red_cli.hset(norm_uri, "date", date)
            red_cli.hset(norm_uri, "content_type", content_type)
            red_cli.hset(norm_uri, "page_len", str(len(raw_html)))
            red_cli.hset(norm_uri, "checksum", checksum)
            red_cli.hset(norm_uri, "html", b64_html)

            # update the "orig_url" -> "norm_uri" mapping, to resolve redirects later
            red_cli.hset(NORM_URI_KEY, orig_url, norm_uri)

            # TODO: outbound links require another lookup to resolve orig_url -> norm_uri mapping

            red_cli.hset(norm_uri, "out_links", "\t".join(out_links))

            return out_links


    def enqueueLink (self, uuid, uri):
        ## enqueue outbound links which satisfy white-listed domain rules

        domain = getDomain(uri)

        if len(white_list) < 1 or domain in white_list:
            testSet(uuid, uri)

            if DEBUG_LEVEL > 0:
                print "ENQUEUE", domain, uuid, uri


    def run (self):
        while True:
            for link_uri in self.dequeueTask():
                link_uuid, link_uri = getUUID(link_uri)
                self.enqueueLink(link_uuid, link_uri)

            # TODO: adjust "throttle" based on aggregate server load (?? Yan, let's define)
            # after "throttled" wait period, signal to queue that the task completed

            time.sleep(REQUEST_SLEEP)
            self.local_queue.task_done()
          

######################################################################

def init (host_port_db):
    ## set up the Redis client

    host, port, db = host_port_db.split(":")
    red_cli = redis.Redis(host=host, port=int(port), db=int(db))

    return red_cli


def flush (all):
    ## flush either all of the key/value store, or just the URI Queue

    if all:
        red_cli.flushdb()
    else:
        red_cli.delete(TODO_QUEUE_KEY)
        red_cli.delete(PEND_QUEUE_KEY)


def testSet (uuid, uri):
    ## test whether URI has been visited before, then add to URI Queue

    if red_cli.setnx(uuid, uri):
        red_cli.sadd(TODO_QUEUE_KEY, uuid)


def seed ():
    ## seed the Queue with root URIs as starting points

    for line in sys.stdin:
        try:
            line = line.strip()
            [uri] = line.split("\t")
            uuid, uri = getUUID(uri)

            if DEBUG_LEVEL > 0:
                print "UUID", uuid, uri

            testSet(uuid, uri)

        except ValueError, err:
            sys.stderr.write("ValueError: %(err)s\n%(data)s\n" % {"err": str(err), "data": line})


def whitelist ():
    ## push white-listed domain rules into key/value store

    for line in sys.stdin:
        try:
            line = line.strip()
            [domain] = line.split("\t")

            if DEBUG_LEVEL > 0:
                print "WHITELISTED", domain

            red_cli.sadd(WHITE_LIST_KEY, domain)

        except ValueError, err:
            sys.stderr.write("ValueError: %(err)s\n%(data)s\n" % {"err": str(err), "data": line})


def spawnThreads (local_queue, num_threads):
    ## spawn a pool of threads, passing them the local queue instance

    for i in range(num_threads):
        t = ThreadUri(local_queue)
        t.setDaemon(True)
        t.start()


def getUUID (uri):
    ## clean URI so it can be used as a unique key, then make an MD5 has as a UUID

    uri = uri.replace(" ", "+")
    m = hashlib.md5()
    m.update(uri)
    uuid = m.hexdigest()

    return uuid, uri


def getDomain (uri):
    ## extract the domain from the URI

    domain = "foo.com"

    try:
        domain = uri.split("/")[2]
    except IndexError, err:
        pass

    return domain


def drawQueue (local_queue, queue_book_len):
    ## draw from the URI Queue to populate local queue with URI fetch tasks

    for n in range(1, queue_book_len):
        uuid = red_cli.srandmember(TODO_QUEUE_KEY)

        if not uuid:
            # URI Queue is empty; have we reached DONE condition?
            pend_len = red_cli.scard(PEND_QUEUE_KEY)

            if DEBUG_LEVEL > 0:
                print "DONE: URI Queue is now empty"
                print "PEND", pend_len

            return pend_len > 0

        elif red_cli.smove(TODO_QUEUE_KEY, PEND_QUEUE_KEY, uuid):
            # get URI, determine domain

            uri = red_cli.get(uuid)
            domain = getDomain(uri)

            if DEBUG_LEVEL > 0:
                print "UUID", domain, uuid, uri

            # enqueue URI fetch task in local queue
            local_queue.put([domain, uuid, uri])

    # there may be more iterations; not DONE
    return True


def crawl ():
    ## draw URI fetch tasks from URI Queue, populate local queue, run tasks in parallel

    local_queue = Queue.Queue()
    spawnThreads(local_queue, NUM_THREADS)

    opener = urllib2.build_opener()
    opener.addheaders = [('User-agent', USER_AGENT), ('Accept-encoding', 'gzip')]

    white_list = red_cli.smembers(WHITE_LIST_KEY)
    has_more = True
    iter = 0

    drawQueue(local_queue, NUM_THREADS * OVER_BOOK)

    while has_more:
        local_queue.join()
        has_more = drawQueue(local_queue, NUM_THREADS * OVER_BOOK)

        if DEBUG_LEVEL > 0:
            print "ITER", iter, has_more

        time.sleep(REQUEST_SLEEP)

        if iter > 2:
            break
        else:
            iter += 1


if __name__ == "__main__":
    # verify command line usage

    if len(sys.argv) != 3:
        print "Usage: slinky.py host:port:db [ 'config' | 'flush' | 'seed' | 'whitelist' | 'crawl' ] < input.txt"
    else:
        # parse command line options

        red_cli = init(sys.argv[1])
        mode = sys.argv[2]

        if mode == "config":
            # TODO: load config params into key/value store
            pass
        elif mode == "flush":
            # flush data from key/value store
            flush(True) # False
        elif mode == "seed":
            # seed the URI Queue with root URIs as starting points
            seed()
        elif mode == "whitelist":
            # push white-listed domain rules into key/value store
            whitelist()
        elif mode == "crawl":
            # populate local queue and crawl those URIs
            crawl()
