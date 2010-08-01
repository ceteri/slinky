#!/usr/bin/python
# encoding: utf-8

import Queue
import base64
import hashlib
import httplib
import robotparser
import sys
import threading
import time
import urllib2
          

## static definitions
      
BOT_NAME = "KwizineLoc/1.0.0"
HTTP_TIMEOUT = 5
REQUEST_SLEEP = 10
MAX_PAGE_LEN = 80000
NUM_WORKER_THREADS = 100


## global variables

debug = True # False
start = time.time()

opener = urllib2.build_opener()
opener.addheaders = [('User-agent', BOT_NAME), ('Accept-encoding', 'gzip')]


## class definitions

class ThreadUrl (threading.Thread):
    """Threaded URL Fetcher"""

    def __init__ (self, queue):
        threading.Thread.__init__(self)
        self.queue = queue


    def getPage (self, url_handle):
        status = str(url_handle.getcode())
        norm_url = url_handle.geturl()

        # get first MAX_PAGE_LEN bytes of HTML from url
        page = url_handle.read(MAX_PAGE_LEN)

        m = hashlib.md5()
        m.update(page)
        checksum = m.hexdigest()

        encoded = base64.b64encode(page)

        if debug:
            print base64.b64decode(encoded)

        url_info = url_handle.info()
        content_type = "text/html"

        if "content-type" in url_info:
            content_type = url_info["content-type"].split(";", 1)[0].strip()

        return status, norm_url, page, checksum, encoded, url_info, content_type


    def fetch (self, oid, url):
        status = "400"
        norm_url = url
        page = ""
        content_type = "text/html"
        checksum = ""
        encoded = ""

        try:
            url_handle = urllib2.urlopen(url, None, HTTP_TIMEOUT)
            status, norm_url, page, checksum, encoded, content_type = self.getPage(url_handle)

        except httplib.InvalidURL:
            status = "400"
        except httplib.BadStatusLine:
            status = "400"
        except httplib.IncompleteRead:
            status = "400"
        except IOError:
            status = "400"
        except ValueError:
            # unknown url type: http
            status = "400"
        else:
            if debug:
                print "SUCCESS:", url

            pass

        return "\t".join([oid, status, content_type, str(len(page)), checksum, norm_url, encoded])


    def run (self):
        while True:
            # grab next url from queue
            [oid, url] = self.queue.get()

            # attempt to fetch its HTML content
            result = self.fetch(oid, url)

            if debug:
                print result

            time.sleep(REQUEST_SLEEP)

            # signal to queue that job is done
            self.queue.task_done()
          

def spawnThreads (queue, num_threads):
    ## spawn a pool of threads, passing them the queue instance

    for i in range(num_threads):
        t = ThreadUrl(queue)
        t.setDaemon(True)
        t.start()


def loadQueue (queue):
    ## populate queue with data (potentially, input from mapper+shuffle)

    for line in sys.stdin:
        try:
            l = line.strip().split('\t')

            if len(l) == 2:
                oid, url = l

                if debug:
                    print oid, url

                queue.put([oid, url])

        except ValueError:
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
