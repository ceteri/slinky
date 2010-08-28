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

from BeautifulSoup import BeautifulSoup, BeautifulStoneSoup, Comment, SoupStrainer
from base64 import b64encode
from collections import deque
from datetime import datetime
from hashlib import md5
from random import random
from redis import Redis
from rfc3339 import rfc3339
from robotparser import RobotFileParser
from threading import Condition, Lock, RLock, Thread
from time import sleep

import httplib
import re
import sqlite3
import string
import sys
import urllib2
import zlib


######################################################################
## global variables, debugging, dependency injection.. oh my!

epoch_time = datetime.strptime("20010101", "%Y%m%d")
start_time = datetime.now()

red_cli = None
red_lock = None

CONF_PARAM_KEY = "conf"
conf_param = {}
conf_param["debug_level"] = "0"


######################################################################
## utility methods

def init (host_port_db):
    ## set up the Redis client

    [host, port, db] = host_port_db.split(":")

    return Redis(host=host, port=int(port), db=int(db))


def printLog (kind, args):
    ## print a log trace out to stderr, after handling potential UTF8 issues

    l = []

    for a in args:
        try:
            l.append(str(a))
        except UnicodeEncodeError:
            pass

    sys.stderr.write(kind + ": " + " ".join(l) + "\n")


def debug (level):
    ## check the log level

    if "debug_level" not in conf_param:
        return True
    elif int(conf_param["debug_level"]) >= level:
        return True
    else:
        return False


def calcPeriod (start):
    ## calculate a time period (millisec)

    td = (datetime.now() - start)

    return (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10**6) / 10**3


def getEpoch ():
    ## calculate the epoch (seconds) since 2001-01-01T00:00:00.000Z

    return calcPeriod(epoch_time) / 10**3


######################################################################
## lifecycle methods

def config ():
    ## put config params in key/value store, as a distrib cache

    for line in sys.stdin:
        try:
            line = line.strip()

            if len(line) > 0 and not line.startswith("#"):
                [param, value] = line.split("\t")

                if debug(0):
                    printLog("CONFIG", [param, value])

                red_cli.hset(CONF_PARAM_KEY, param, value);

        except ValueError, err:
            sys.stderr.write("ValueError: %(err)s\n%(data)s\n" % {"err": str(err), "data": line})
        except IndexError, err:
            sys.stderr.write("IndexError: %(err)s\n%(data)s\n" % {"err": str(err), "data": line})


def flush (all):
    ## flush either all of the key/value store, or just the CrawlQueue

    if all:
        red_cli.flushdb()
    else:
        red_cli.delete(conf_param["perform_todo_q"])
        red_cli.delete(conf_param["perform_pend_q"])


def putWhitelist ():
    ## push white-listed domain rules into key/value store

    for line in sys.stdin:
        try:
            line = line.strip()
            [domain] = line.split("\t")

            if debug(0):
                printLog("WHITELIST", [domain])

            red_cli.sadd(conf_param["white_list_key"], domain)

        except ValueError, err:
            sys.stderr.write("ValueError: %(err)s\n%(data)s\n" % {"err": str(err), "data": line})
        except IndexError, err:
            sys.stderr.write("IndexError: %(err)s\n%(data)s\n" % {"err": str(err), "data": line})


def seed (crawler):
    ## seed the Queue with root URIs as starting points

    for line in sys.stdin:
        try:
            line = line.strip()
            [hops, uri] = line.split("\t", 1)

            page = WebPage(uri, int(hops))
            page.hydrate()

            if debug(0):
                printLog("SEED", [page.uuid, page.uri, page.protocol, page.domain, page.hops])

	    crawler.scheduleURI(page)

        except ValueError, err:
            sys.stderr.write("ValueError: %(err)s\n%(data)s\n" % {"err": str(err), "data": line})
        except IndexError, err:
            sys.stderr.write("IndexError: %(err)s\n%(data)s\n" % {"err": str(err), "data": line})


def perform (crawler):
    ## draw fetch tasks from CrawlQueue, populate tasks into WorkerPool, run tasks in parallel

    if debug(0):
        printLog("START", [start_time])

    wp = WorkerPool(queue_len = 1, num_cons = int(conf_param["num_threads"]))
    wp.run(crawler.runWorkerTask)

    # set up producer

    producer = ProducerThread(wp.bounded_queue, crawler.feedWorkerPool)
    producer.name = "WorkerPool"
    producer.start()

    # we can haz joinz?

    wp.join()
    producer.join()


def persist ():
    ## drain content from PageStore and persist it on disk
    ## NB: single-threaded

    # create SQL table to store deflated HTML content

    db = PersistanceLayer()
    db.connect(conf_param["persist_db_uri"])
    db.sqlWrapper("CREATE TABLE page (uuid TEXT, b64_html TEXT)", silent=True)

    while True:
        zpop = red_cli.zrange(conf_param["persist_todo_q"], 0, 0)

        if len(zpop) < 1:
            # PersistQueue is empty... so sleep now, then poll again later
            sleep(float(conf_param["persist_sleep"]))
            continue
        elif not red_cli.zrem(conf_param["persist_todo_q"], zpop[0]):
            # some other Redis client won a race condition
            continue
        else:
            # got it! move URI to the "persist pended" phased queue
            uuid = zpop[0]

            if uuid:
                red_cli.zadd(conf_param["persist_pend_q"], uuid, getEpoch())
                b64_html = red_cli.hget(uuid, "b64_html")

                if b64_html:
                    print "\t".join([uuid, b64_html])
                    db.sqlWrapper("INSERT INTO page VALUES(?, ?)", (uuid, b64_html))
                    db.commit()

                pipe = red_cli.pipeline()
                pipe.zadd(conf_param["analyze_todo_q"], uuid, getEpoch())
                pipe.hdel(uuid, "b64_html")
                pipe.zrem(conf_param["persist_pend_q"], uuid)
                pipe.execute()


######################################################################
## PersistenceLayer class definition

class PersistanceLayer ():
    """Relational Persistance Layer"""

    def __init__ (self):
        ## initialize
        self.uri = None
        self.conn = None
        self.curs = None


    def connect (self, uri):
        self.uri = uri
        self.conn = sqlite3.connect(self.uri)
        self.conn.isolation_level = None
        self.curs = self.conn.cursor()


    def sqlWrapper (self, query, args=(), silent=False):
        ## wrapper to execute SQL statement
        success = False

        try:
            self.curs.execute(query, args)
            success = True
        except sqlite3.Error, err:
            if not silent:
                sys.stderr.write("SQLite3 Error: %(err)s\n" % {"err": str(err.args[0])})

        return success


    def commit (self):
        ## save changes / commit
        self.conn.commit()


    def close (self):
        ## close the cursor when we are done with it
        self.curs.close()
        self.conn.close()


######################################################################
## WebPage class definition

class WebPage ():
    """Web Page"""

    def __init__ (self, uri, hops):
        ## initialize

        self.uri = uri
        self.hops = hops
        self.out_links = set([])

	self.uuid = None
        self.protocol = None
        self.domain = None
        self.status = None
        self.reason = None
        self.crawl_time = None
        self.norm_uri = None
        self.norm_uuid = None
        self.content_type = None
        self.date = None
        self.raw_html = None
        self.b64_html = None
        self.checksum = None


    def hydrate (self):
        self.uuid, self.uri, self.protocol, self.domain = self.getUUID(self.uri)


    def getUUID (self, uri):
        ## clean URI so it can be used as a unique key, then make an MD5 has as a UUID

        protocol = "http:"
        domain = "foo.com"

        try:
            l = uri.split("/")
	    protocol = l[0].strip(":")
	    domain = l[2]
	except IndexError, err:
            # may ignore these, defaults get used
            pass

	uri = uri.replace(" ", "+")

	m = md5()
	m.update(filter(lambda x: x in string.printable, uri))
	uuid = m.hexdigest()

        return uuid, uri, protocol, domain


    def getPathQuery (self, uri):
        path_list = []
        local_page = ""
        query_text = ""

        http_proto_pat = re.compile("^(https?)\:\/\/([\w\-\.\_]+)(\/.*)$")
        m = re.match(http_proto_pat, uri)

        if m:
            path_text = m.group(3)
            query_split = string.find(path_text, "?")

            if query_split > 0:
                query_text = path_text[(query_split + 1):-1]
                path_text = path_text[0:query_split]

            path_list = path_text.split("/")
            path_list.pop(0)
            local_page = path_list.pop(-1)

        return path_list, local_page, query_text


    def parseOutLinks (self):
        ## scan for outbound web links in the fetched HTML content

        out_links = set([])
        results = []

        try:
            # TODO: include <img/> "src" links as well
            # TODO: any others?

            results = BeautifulSoup(self.raw_html, parseOnlyThese=SoupStrainer("a"))
        except UnicodeEncodeError, err:
            sys.stderr.write("UnicodeEncodeError: %(err)s uuid: %(uuid)s\n" % {"err": str(err), "uuid": self.uuid})

        for link in results:
            if link.has_key("href"):
                l = link["href"]
                out_uri = None

                if len(l) < 1 or l.startswith("javascript") or l.startswith("#"):
                    # ignore non-links
                    continue
                elif l.startswith("/"):
                    # reconstruct absolute URI from relative URI
                    out_uri = "/".join([self.protocol + ":", "", self.domain]) + l
                elif not l.startswith(self.protocol):
                    # reconstruct absolute URI from local URI
                    path_list, local_page, query_text = self.getPathQuery(self.norm_uri)
                    out_uri = "/".join([self.protocol + ":", "", self.domain] + path_list + [""]) + l
                else:
                    # add the absolute URI
                    out_uri = l

                if out_uri:
                    out_links.add(out_uri)

                    if debug(5):
                        printLog("OUT", [self.uuid, out_uri])

        return out_links


    def attemptFetch (self):
        ## attempt to fetch+store the URI metadata+content

        crawl_start = datetime.now()
        self.fetch()
        self.crawl_time = calcPeriod(crawl_start)

        if debug(2):
            printLog("attemptFetch", [self.uuid, self.norm_uri])

        # collect Redis commands in a MULTI/EXEC pipeline() batch

        pipe = red_cli.pipeline()

        # update derived metdata for the normalized URI

        if self.norm_uri:
            self.norm_uuid, self.norm_uri, self.protocol, self.domain = self.getUUID(self.norm_uri)

            pipe.hset(self.uuid, "uri", self.norm_uri)
            pipe.hset(self.norm_uuid, "uri", self.norm_uri)

            if self.uuid != self.norm_uuid:
                pipe.hset(self.uuid, "norm_uuid", self.norm_uuid)

        # update metadata for URI in Page Store

        if self.hops:
            pipe.hset(self.norm_uuid, "hops", self.hops)

        if self.domain:
            pipe.hset(self.norm_uuid, "domain", self.domain)

        if self.status:
            pipe.hset(self.norm_uuid, "status", self.status)

        if self.reason:
            pipe.hset(self.norm_uuid, "reason", self.reason)

        if self.content_type:
            pipe.hset(self.norm_uuid, "content_type", self.content_type)

        if self.date:
            pipe.hset(self.norm_uuid, "date", self.date)

        if self.crawl_time:
            pipe.hset(self.norm_uuid, "crawl_time", self.crawl_time)

        # update content for URI in Page Store

        if self.raw_html:
            if debug(6):
                printLog("HTML", [self.raw_html])

            self.deflate()

            pipe.hset(self.norm_uuid, "checksum", self.checksum)
            pipe.hset(self.norm_uuid, "b64_html", self.b64_html)

            # parse outbound links within URI content

            self.out_links = map(self.getUUID, self.parseOutLinks())

            if self.out_links:
                pipe.hset(self.norm_uuid, "out_links", map(lambda l: l[0], self.out_links))

        # mark URI as "visited" and now needing to be persisted,
        # so move it to the "persist todo" phased queue

        if self.norm_uuid:
            pipe.zadd(conf_param["persist_todo_q"], self.norm_uuid, getEpoch())

        pipe.zrem(conf_param["perform_pend_q"], self.uuid)
        pipe.execute()

        return map(lambda l: l[1], self.out_links), self.hops + 1


    def fetch (self):
        ## attempt to fetch the given URI, collecting the status code

        try:
            url_handle = urllib2.urlopen(self.uri, None, int(conf_param["http_timeout"]))
            self.getPage(url_handle)

            # status codes based on HTTP/1.1 spec in RFC 2616:
            # http://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html

            # TODO: capture explanation text for status code

        except httplib.InvalidURL, err:
            sys.stderr.write("HTTP InvalidURL: %(err)s\n%(data)s\n" % {"err": str(err), "data": str([self.uuid, self.uri])})
            self.status = "400"
            self.reason = "invalid URL"
        except httplib.BadStatusLine, err:
            sys.stderr.write("HTTP BadStatusLine: %(err)s\n%(data)s\n" % {"err": str(err), "data": str([self.uuid, self.uri])})
            self.status = "400"
            self.reason = "bad status line"
        except httplib.IncompleteRead, err:
            sys.stderr.write("HTTP IncompleteRead: %(err)s\n%(data)s\n" % {"err": str(err), "data": str([self.uuid, self.uri])})
            self.status = "400"
            self.reason = "imcomplete read"
        except urllib2.HTTPError, err:
            sys.stderr.write("HTTPError: %(err)s\n%(data)s\n" % {"err": str(err), "data": str([self.uuid, self.uri])})
            self.status = str(err.code)
            self.reason = "http error"
        except urllib2.URLError, err:
            sys.stderr.write("URLError: %(err)s\n%(data)s\n" % {"err": str(err), "data": str([self.uuid, self.uri])})
            self.status = "408"
            self.reason = err.reason
        except IOError, err:
            sys.stderr.write("IOError: %(err)s\n%(data)s\n" % {"err": str(err), "data": str([self.uuid, self.uri])})
            self.status = "400"
            self.reason = "IO error"
        except ValueError, err:
            # unknown url type: http
            sys.stderr.write("ValueError: %(err)s\n%(data)s\n" % {"err": str(err), "data": str([self.uuid, self.uri])})
            self.status = "400"
            self.reason = "value error"
        else:
            self.reason = "okay"

        if debug(1):
            printLog("CRAWL", [self.hops, self.uuid, self.domain, self.norm_uri, self.date, self.content_type, self.status, self.reason])


    def getPage (self, url_handle):
        ## get HTTP headers and HTML content from a URI handle

        # keep track of the HTTP status and normalized URI

        self.status = str(url_handle.getcode())
        self.norm_uri = url_handle.geturl()

        # GET the thresholded byte count of HTML content

        self.raw_html = url_handle.read(int(conf_param["max_page_len"]))

        # determine the content type

        url_info = url_handle.info()
        self.content_type = "text/html"

        if "content-type" in url_info:
            self.content_type = url_info["content-type"].split(";", 1)[0].strip()

        # determine a date

        if "last-modified" in url_info:
            self.date = self.formatRFC3339(url_info["last-modified"])
        else:
            self.date = self.formatRFC3339(url_info["date"])


    def formatRFC3339 (self, date):
        ## convert date into RFC 3339 format

        dt = datetime.strptime(date, "%a, %d %b %Y %H:%M:%S %Z")

        return rfc3339(dt, utc=True, use_system_timezone=False)


    def deflate (self):
        ## compute CRC of the content, deflate with zlib then Base64 encode

        self.checksum = zlib.crc32(self.raw_html)
        self.b64_html = b64encode(zlib.compress(self.raw_html, 9))


######################################################################
## Crawler class definition

class Crawler ():
    """Site Crawler"""

    def __init__ (self):
        ## initialize

	self.domain_rules = {}
        self.white_list = red_cli.smembers(conf_param["white_list_key"])

        self.opener = urllib2.build_opener()
        self.opener.addheaders = [("User-agent", conf_param["user_agent"]), ("Accept-encoding", "gzip")]


    def passWhitelist (self, page):
        ## is the URI domain whitelisted?

        return (len(self.white_list) < 1) or (page.domain in self.white_list)


    def passRobots (self, page):
        ## check "robots.txt" permission for this URI

        is_allowed = False

        try:
            if page.domain in self.domain_rules:
                rp = self.domain_rules[page.domain]
            else:
                rp = RobotFileParser()
                rp.set_url("/".join([page.protocol + ":", "", page.domain, "robots.txt"]))
                rp.read()
                self.domain_rules[page.domain] = rp

            is_allowed = rp.can_fetch(conf_param["user_agent"], page.uri)

	except IOError, err:
            sys.stderr.write("IOError: %(err)s\n%(data)s\n" % {"err": str(err), "data": str([page.domain, page.uri])})

        if debug(4):
            printLog("ROBOTS", [is_allowed, page.uri])

        return is_allowed


    def scheduleURI (self, page):
        ## consider incoming URI from "seed" file or from parsed outbound
        ## links, with "hops" as a known distance from a seed URI

        page.status = "0"
	page.reason = "todo"

        # test/set UUID in the "uuid->best_uri" map

        if not red_cli.hget(page.uuid, "uri"):
            red_cli.hset(page.uuid, "uri", page.uri)

            if debug(1):
                printLog("QUEUE", [page.hops, page.uuid, page.domain, page.uri])

            # check URI against the domain whitelist rules
            if not self.passWhitelist(page):
                page.reason = "failed whitelist"

            # check URI against "robots.txt" rules for its domain
	    elif not self.passRobots(page):
	        page.reason = "failed robots"

            # have not seen this URI before, so enqueue it on CrawlQueue
            else:
                pipe = red_cli.pipeline()
                pipe.zadd(conf_param["perform_todo_q"], page.uuid, page.hops)

		# initialize URI metadata in Page Store

		pipe.hset(page.uuid, "hops", page.hops)
		pipe.hset(page.uuid, "domain", page.domain)
		pipe.hset(page.uuid, "status", page.status)
		pipe.hset(page.uuid, "reason", page.reason)

                pipe.execute()


    def consumeQueue (self):
        ## consume from CrawlQueue
	
        uuid = None

        if not red_lock.acquire(True):
            # ... failed to lock the Redis client
            pass
        else:
            # acquired lock w/ wait, to avoid thread contention for Redis client

            try:
                # attempt to pop head element from CrawlQueue
                # NB: creates a fail-fast race condition to consume UUID
                zpop = red_cli.zrange(conf_param["perform_todo_q"], 0, 0)

                if len(zpop) < 1:
                    # CrawlQueue is empty... so sleep now, then poll again later
                    pass
                elif not red_cli.zrem(conf_param["perform_todo_q"], zpop[0]):
                    # some other Redis client won the race condition
                    pass
                else:
                    # got it! move URI to the "perform pended" phased queue
                    uuid = zpop[0]
                    red_cli.zadd(conf_param["perform_pend_q"], uuid, getEpoch())
	    finally:
                red_lock.release()

        return uuid


    def feedWorkerPool (self, worker_name, counter):
        ## poll CrawlQueue for another task, blocking until obtained

        task = None

        while task is None:
            sleep(float(conf_param["perform_sleep"]))
            uuid = self.consumeQueue()
            task = uuid

        if debug(1):
            printLog("FEED", ["%s TASK# %d, %d sec" % (worker_name, counter, calcPeriod(start_time) / 10**3)])

        return task


    def findPage (self, uuid):
        ## get URI from the "uuid->best_uri" map

        page = None

        uri = red_cli.hget(uuid, "uri")
        hops = red_cli.hget(uuid, "hops")

        if uri and hops:
            page = WebPage(uri, int(hops))
            page.hydrate()

            if debug(3):
                printLog("findPage", [page.uuid, page.domain, page.hops, page.uri])

        return page


    def runWorkerTask (self, worker_name, task):
        ## fetch+parser thread consumes a unit of work from WorkerPool

        if debug(2):
            printLog("runWorkerTask", ["%s TASK %s" % (worker_name, task)])

        uuid = task
        page = self.findPage(uuid)
        sleep(0.01)

        if page:
            out_links, hops = page.attemptFetch()

            if debug(1):
                printLog("TIME", [page.crawl_time, uuid])

            for out_uri in out_links:
                p_out = WebPage(out_uri, hops)
                p_out.hydrate()

                if debug(4):
                    printLog("SCHED", [p_out.hops, p_out.uuid, p_out.uri])

                self.scheduleURI(p_out)


######################################################################
## WorkerPool class definition

class BoundedQueue ():
    def __init__ (self, limit):
        self.mon = RLock()
        self.rc = Condition(self.mon)
        self.wc = Condition(self.mon)
        self.limit = limit
        self.queue = deque()

    def put (self, task):
        self.mon.acquire()

        while len(self.queue) >= self.limit:
            if debug(6):
                printLog("put", ["queue full", task])

            self.wc.wait()

        self.queue.append(task)

        if debug(6):
            printLog("put", ["appended", task, "length now", len(self.queue)])

        self.rc.notify()
        self.mon.release()


    def get (self):
        self.mon.acquire()

        while not self.queue:
            if debug(6):
                printLog("get", ["queue empty"])

            self.rc.wait()

        task = self.queue.popleft()

        if debug(6):
            printLog("get", ["got", task, "remaining", len(self.queue)])

        self.wc.notify()
        self.mon.release()

        return task


class ConsumerThread (Thread):
    def __init__ (self, queue, task_func):
        Thread.__init__(self, name="Consumer")
        self.queue = queue
        self.task_func = task_func


    def run (self):
        while True:
            # consume a unit of work
            self.task_func(self.name, self.queue.get())


class ProducerThread (Thread):
    def __init__ (self, queue, prod_func):
        Thread.__init__(self, name="Producer")
        self.queue = queue
        self.prod_func = prod_func


    def run (self):
        counter = 0

        while True:
            # produce a unit of work
            counter = counter + 1
            self.queue.put(self.prod_func(self.name, counter))


class WorkerPool ():
    def __init__ (self, queue_len, num_cons):
        # set up queue + consumers

        self.queue_len = queue_len
        self.num_cons = num_cons
        self.bounded_queue = BoundedQueue(self.queue_len)
        self.consumer_thread_pool = []


    def run (self, task_func):
        # start consumers, which will block

        for i in range(self.num_cons):
            consumer = ConsumerThread(self.bounded_queue, task_func)
            consumer.name = ("Consumer-%d" % (i + 1))
            self.consumer_thread_pool.append(consumer)

        for consumer in self.consumer_thread_pool:
            consumer.start()


    def join (self):
        # join consumers - after producer set up / before producer join

        for consumer in self.consumer_thread_pool:
            consumer.join()


def _test_prod_func (worker_name, counter):
    # produce a unit of work
    sleep(random() * 0.1)
    task = "%s task %d" % (worker_name, counter)
    return task                           


def _test_task_func (worker_name, task):
    # consume a unit of work
    sleep(random() * 0.3)
    printLog("test", ["%s DONE %s" % (worker_name, task)])


def _test ():
    queue_len = 3
    num_cons = 2

    wp = WorkerPool(queue_len, num_cons)
    wp.run(_test_task_func)

    # set up producer

    producer = ProducerThread(wp.bounded_queue, _test_prod_func)
    producer.name = "Producer"
    producer.start()

    # i can haz joinz?

    wp.join()
    producer.join()


######################################################################
## command line interface

if __name__ == "__main__":
    # verify command line usage

    if len(sys.argv) != 3:
        print "Usage: slinky.py host:port:db [ 'config' | 'flush' | 'whitelist' | 'seed' | 'perform' | 'persist' | 'analyze' ] < input.txt"
    else:
        # most basic configuration

        red_cli = init(sys.argv[1])
        red_lock = Lock()
        conf_param = red_cli.hgetall(CONF_PARAM_KEY)

        # parse command line options

        mode = sys.argv[2]

        if mode == "flush":
            # flush data from key/value store
            flush(True) # False
        elif mode == "config":
            # put config params in key/value store, as a distrib cache
            config()
        elif mode == "whitelist":
            # push white-listed domain rules into key/value store
            putWhitelist()
        elif mode == "seed":
            # seed the CrawlQueue with root URIs as starting points
            c = Crawler()
            seed(c)
        elif mode == "perform":
            # consume from CrawlQueue via WorkerPool producer
            c = Crawler()
            perform(c)
        elif mode == "persist":
            # drain content from PageStore and persist it on disk
            persist()
