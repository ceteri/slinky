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
import numpy
import redis
import robotparser
import sys
import threading
import time
import urllib2
          

######################################################################
## global variables, debugging, dependency injection.. oh my!

red_cli = None
opener = None

domain_rules = {}
conf_param = {}

CONF_PARAM_KEY = "conf"
conf_param["debug_level"] = "0"


######################################################################
## class definitions

class ThreadUri (threading.Thread):
    """Threaded URI Fetcher"""

    def __init__ (self, local_queue, white_list):
        ## initialize this Thread

        threading.Thread.__init__(self)
        self.local_queue = local_queue
        self.white_list = white_list


    def checkRobots (self, protocol, domain, uri):
        ## check "robots.txt" permission for this domain

        if domain in domain_rules:
            rp = domain_rules[domain]
        else:
            rp = robotparser.RobotFileParser()
            rp.set_url("/".join([protocol, "", domain, "robots.txt"]))
            rp.read()
            domain_rules[domain] = rp

        is_allowed = rp.can_fetch(conf_param["user_agent"], uri)

        if debug(0):
            print "ROBOTS", is_allowed, uri

        return is_allowed


    def getPage (self, url_handle):
        ## get HTTP headers and HTML content from a URI handle

        # keep track of the HTTP status and normalized URI
        status = str(url_handle.getcode())
        norm_uri = url_handle.geturl()

        # GET thresholded byte count of HTML content
        raw_html = url_handle.read(int(conf_param["max_page_len"]))

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


    def fetch (self, protocol, domain, orig_url):
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

            if self.checkRobots(protocol, domain, orig_url):
                url_handle = urllib2.urlopen(orig_url, None, int(conf_param["http_timeout"]))
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
            status = "400"
        except ValueError, err:
            # unknown url type: http
            sys.stderr.write("ValueError: %(err)s\n%(data)s\n" % {"err": str(err), "data": orig_url})
            status = "400"
        else:
            if debug(0):
                print "SUCCESS:", orig_url

        return status, norm_uri, content_type, date, checksum, b64_html, raw_html


    def getOutLinks (self, protocol, domain, raw_html):
        ## scan for outbound web links in the fetched HTML content

        out_links = set([])

        for link in BeautifulSoup(raw_html, parseOnlyThese=SoupStrainer("a")):
            if link.has_key("href"):
                l = link["href"]

                if len(l) < 1 or l.startswith("javascript") or l.startswith("#"):
                    # ignore non-links
                    pass
                elif l.startswith("/"):
                    # reconstruct absolute URI from relative URI
                    out_links.add("/".join([protocol, "", domain]) + l)
                else:
                    # add the absolute URI
                    out_links.add(l)

        if debug(4):
            print out_links

        return out_links


    def dequeueTask (self):
            # pop random/next from URI Queue and attempt to fetch HTML content

            [protocol, domain, uuid, orig_url] = self.local_queue.get()

            status, norm_uri, content_type, date, checksum, b64_html, raw_html = self.fetch(protocol, domain, orig_url)
            norm_uuid, norm_uri = getUUID(norm_uri)
            out_links = self.getOutLinks(protocol, domain, raw_html)

            if debug(0):
                print domain, norm_uri, status, content_type, date, str(len(raw_html)), checksum

            # update the Page Store with fetched/analyzed data

            red_cli.srem(conf_param["pend_queue_key"], uuid)

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
            red_cli.hset(conf_param["norm_uri_key"], orig_url, norm_uri)

            out_links = resolveLinks(out_links)
            red_cli.hset(norm_uri, "out_links", "\t".join(out_links))

            return out_links


    def enqueueLink (self, uuid, uri):
        ## enqueue outbound links which satisfy white-listed domain rules

        protocol, domain = getDomain(uri)

        if debug(0):
            print "CHECK", protocol, domain, uri

        if len(self.white_list) < 1 or domain in self.white_list:
            testSet(uuid, uri)

            if debug(0):
                print "ENQUEUE", domain, uuid, uri


    def run (self):
        while True:
            for link_uri in self.dequeueTask():
                link_uuid, link_uri = getUUID(link_uri)
                self.enqueueLink(link_uuid, link_uri)

            # after "throttled" wait period, signal to queue that the task completed
            # TODO: adjust "throttle" based on aggregate server load (?? Yan, let's define)

            time.sleep(int(conf_param["request_sleep"]) * (1.0 + numpy.random.random()))
            self.local_queue.task_done()
          

######################################################################
## lifecycle methods

def init (host_port_db):
    ## set up the Redis client

    host, port, db = host_port_db.split(":")
    red_cli = redis.Redis(host=host, port=int(port), db=int(db))

    return red_cli


def debug (level):
    return int(conf_param["debug_level"]) > level


def config ():
    ## put config params in key/value store, as a distrib cache

    for line in sys.stdin:
        try:
            line = line.strip()

            if len(line) > 0 and not line.startswith("#"):
                [param, value] = line.split("\t")

                print "CONFIG", param, value
                red_cli.hset(CONF_PARAM_KEY, param, value);

        except ValueError, err:
            sys.stderr.write("ValueError: %(err)s\n%(data)s\n" % {"err": str(err), "data": line})
        except IndexError, err:
            sys.stderr.write("IndexError: %(err)s\n%(data)s\n" % {"err": str(err), "data": line})


def flush (all):
    ## flush either all of the key/value store, or just the URI Queue

    if all:
        red_cli.flushdb()
    else:
        red_cli.delete(conf_param["todo_queue_key"])
        red_cli.delete(conf_param["pend_queue_key"])


def testSet (uuid, uri):
    ## test whether URI has been visited before, then add to URI Queue

    if red_cli.setnx(uuid, uri):
        red_cli.sadd(conf_param["todo_queue_key"], uuid)


def seed ():
    ## seed the Queue with root URIs as starting points

    for line in sys.stdin:
        try:
            line = line.strip()
            [uri] = line.split("\t")
            uuid, uri = getUUID(uri)

            if debug(0):
                print "UUID", uuid, uri

            testSet(uuid, uri)

        except ValueError, err:
            sys.stderr.write("ValueError: %(err)s\n%(data)s\n" % {"err": str(err), "data": line})
        except IndexError, err:
            sys.stderr.write("IndexError: %(err)s\n%(data)s\n" % {"err": str(err), "data": line})


def whitelist ():
    ## push white-listed domain rules into key/value store

    for line in sys.stdin:
        try:
            line = line.strip()
            [domain] = line.split("\t")

            if debug(0):
                print "WHITELISTED", domain

            red_cli.sadd(conf_param["white_list_key"], domain)

        except ValueError, err:
            sys.stderr.write("ValueError: %(err)s\n%(data)s\n" % {"err": str(err), "data": line})
        except IndexError, err:
            sys.stderr.write("IndexError: %(err)s\n%(data)s\n" % {"err": str(err), "data": line})


def spawnThreads (local_queue, white_list, num_threads):
    ## spawn a pool of threads, passing them the local queue instance

    for i in range(num_threads):
        t = ThreadUri(local_queue, white_list)
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

    protocol = "http:"
    domain = "foo.com"

    try:
        l = uri.split("/")
        protocol = l[0]
        domain = l[2]
    except IndexError, err:
        # may ignore these, defaults get used
        pass

    return protocol, domain


def resolveLinks (out_links):
    ## perform lookup on outbound links to resolve orig_url -> norm_uri mapping

    resolved_links = set([])

    for link_uri in out_links:
        norm_uri = red_cli.hget(conf_param["norm_uri_key"], link_uri)

        if norm_uri:
            resolved_links.add(norm_uri)
        else:
            resolved_links.add(link_uri)

    return resolved_links


def drawQueue (local_queue, draw_limit):
    ## draw from the URI Queue to populate local queue with URI fetch tasks

    for n in range(1, draw_limit):
        uuid = red_cli.srandmember(conf_param["todo_queue_key"])

        if not uuid:
            # URI Queue is empty; have we reached DONE condition?
            pend_len = red_cli.scard(conf_param["pend_queue_key"])

            if debug(0):
                print "DONE: URI Queue is now empty"
                print "PEND", pend_len

            return pend_len > 0

        elif red_cli.smove(conf_param["todo_queue_key"], conf_param["pend_queue_key"], uuid):
            # get URI, determine domain

            uri = red_cli.get(uuid)
            protocol, domain = getDomain(uri)

            if debug(0):
                print "UUID", domain, uuid, uri

            # enqueue URI fetch task in local queue
            local_queue.put([protocol, domain, uuid, uri])

    # there may be more iterations; not DONE
    return True


def crawl ():
    ## draw URI fetch tasks from URI Queue, populate local queue, run tasks in parallel

    local_queue = Queue.Queue()
    white_list = red_cli.smembers(conf_param["white_list_key"])

    spawnThreads(local_queue, white_list, int(conf_param["num_threads"]))

    opener = urllib2.build_opener()
    opener.addheaders = [("User-agent", conf_param["user_agent"]), ("Accept-encoding", "gzip")]

    draw_limit = int(conf_param["num_threads"]) * int(conf_param["over_book"])
    drawQueue(local_queue, draw_limit)

    has_more = True
    iter = 0

    while has_more:
        local_queue.join()
        has_more = drawQueue(local_queue, draw_limit)

        if debug(0):
            print "ITER", iter, has_more

        if iter >= int(conf_param["max_iterations"]):
            has_more = False
        else:
            iter += 1
            time.sleep(int(conf_param["request_sleep"]))


if __name__ == "__main__":
    # verify command line usage

    if len(sys.argv) != 3:
        print "Usage: slinky.py host:port:db [ 'config' | 'flush' | 'seed' | 'whitelist' | 'crawl' ] < input.txt"
    else:
        # parse command line options

        red_cli = init(sys.argv[1])
        conf_param = red_cli.hgetall(CONF_PARAM_KEY)
        mode = sys.argv[2]

        if mode == "config":
            # put config params in key/value store, as a distrib cache
            config()
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
