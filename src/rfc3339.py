#!/usr/bin/env python
#
# Copyright (c) 2009, 2010, Henry Precheur <henry@precheur.org>
#
# Permission to use, copy, modify, and/or distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT,
# INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM
# LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR
# OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR
# PERFORMANCE OF THIS SOFTWARE.
#
'''Formats dates according to the :RFC:`3339`.'''

__author__ = 'Henry Precheur <henry@precheur.org>'
__license__ = 'ISCL'
__version__ = '3'
__all__ = ('rfc3339', )

import datetime
import time
import unittest

def _timezone(utcoffset):
    '''
    Return a string representing the timezone offset.

    >>> _timezone(0)
    '+00:00'
    >>> _timezone(3600)
    '+01:00'
    >>> _timezone(-28800)
    '-08:00'
    >>> _timezone(-1800)
    '-00:30'
    '''
    # Python's division uses floor(), not round() like in other languages:
    #   -1 / 2 == -1 and not -1 / 2 == 0
    # That's why we use abs(utcoffset).
    hours = abs(utcoffset) // 3600
    minutes = abs(utcoffset) % 3600 // 60
    return '%c%02d:%02d' % ('-' if utcoffset < 0 else '+', hours, minutes)

def _timedelta_to_seconds(timedelta):
    '''
    >>> _timedelta_to_seconds(datetime.timedelta(hours=3))
    10800
    >>> _timedelta_to_seconds(datetime.timedelta(hours=3, minutes=15))
    11700
    '''
    return (timedelta.days * 86400 + timedelta.seconds +
            timedelta.microseconds // 1000)

def _utc_offset(date, use_system_timezone):
    '''
    Return the UTC offset of `date`. If `date` does not have any `tzinfo`, use
    the timezone informations stored locally on the system.

    >>> if time.localtime().tm_isdst:
    ...     system_timezone = -time.altzone
    ... else:
    ...     system_timezone = -time.timezone
    >>> _utc_offset(datetime.datetime.now(), True) == system_timezone
    True
    >>> _utc_offset(datetime.datetime.now(), False)
    0
    '''
    if isinstance(date, datetime.datetime) and date.tzinfo is not None:
        return _timedelta_to_seconds(date.dst() or date.utcoffset())
    elif use_system_timezone:
        t = time.mktime(date.timetuple())
        if time.localtime(t).tm_isdst: # pragma: no cover
            return -time.altzone
        else:
            return -time.timezone
    else:
        return 0

def _utc_string(d):
    return d.strftime('%Y-%m-%dT%H:%M:%SZ')

def rfc3339(date, utc=False, use_system_timezone=True):
    '''
    Return a string formatted according to the :RFC:`3339`. If called with
    `utc=True`, it normalizes `date` to the UTC date. If `date` does not have
    any timezone information, uses the local timezone::

        >>> d = datetime.datetime(2008, 4, 2, 20)
        >>> rfc3339(d, utc=True, use_system_timezone=False)
        '2008-04-02T20:00:00Z'
        >>> rfc3339(d) # doctest: +ELLIPSIS
        '2008-04-02T20:00:00...'

    If called with `user_system_time=False` don't use the local timezone if
    `date` does not have timezone informations and consider the offset to UTC
    to be zero::

        >>> rfc3339(d, use_system_timezone=False)
        '2008-04-02T20:00:00+00:00'

    `date` must be a `datetime.datetime`, `datetime.date` or a timestamp as
    returned by `time.time()`::

        >>> rfc3339(0, utc=True, use_system_timezone=False)
        '1970-01-01T00:00:00Z'
        >>> rfc3339(datetime.date(2008, 9, 6), utc=True,
        ...         use_system_timezone=False)
        '2008-09-06T00:00:00Z'
        >>> rfc3339(datetime.date(2008, 9, 6),
        ...         use_system_timezone=False)
        '2008-09-06T00:00:00+00:00'
        >>> rfc3339('foo bar')
        Traceback (most recent call last):
        ...
        TypeError: excepted datetime, got str instead
    '''
    # Check if `date` is a timestamp.
    try:
        if utc:
            return _utc_string(datetime.datetime.utcfromtimestamp(date))
        else:
            date = datetime.datetime.fromtimestamp(date)
    except TypeError:
        pass
    if isinstance(date, datetime.date):
        utcoffset = _utc_offset(date, use_system_timezone)
        if utc:
            if not isinstance(date, datetime.datetime):
                date = datetime.datetime(*date.timetuple()[:3])
            return _utc_string(date + datetime.timedelta(seconds=utcoffset))
        else:
            return date.strftime('%Y-%m-%dT%H:%M:%S') + _timezone(utcoffset)
    else:
        raise TypeError('excepted %s, got %s instead' %
                        (datetime.datetime.__name__, date.__class__.__name__))


class LocalTimeTestCase(unittest.TestCase):
    '''
    Test the use of the timezone saved locally. Since it is hard to test using
    doctest.
    '''

    def setUp(self):
        local_utcoffset = _utc_offset(datetime.datetime.now(), True)
        self.local_utcoffset = datetime.timedelta(seconds=local_utcoffset)
        self.local_timezone = _timezone(local_utcoffset)

    def test_datetime(self):
        d = datetime.datetime.now()
        self.assertEqual(rfc3339(d),
                         d.strftime('%Y-%m-%dT%H:%M:%S') + self.local_timezone)

    def test_datetime_timezone(self):

        class FixedNoDst(datetime.tzinfo):
            'A timezone info with fixed offset, not DST'

            def utcoffset(self, dt):
                return datetime.timedelta(hours=2, minutes=30)

            def dst(self, dt):
                return None

        fixed_no_dst = FixedNoDst()

        class Fixed(FixedNoDst):
            'A timezone info with DST'

            def dst(self, dt):
                return datetime.timedelta(hours=3, minutes=15)

        fixed = Fixed()

        d = datetime.datetime.now().replace(tzinfo=fixed_no_dst)
        timezone = _timezone(_timedelta_to_seconds(fixed_no_dst.\
                                                   utcoffset(None)))
        self.assertEqual(rfc3339(d),
                         d.strftime('%Y-%m-%dT%H:%M:%S') + timezone)

        d = datetime.datetime.now().replace(tzinfo=fixed)
        timezone = _timezone(_timedelta_to_seconds(fixed.dst(None)))
        self.assertEqual(rfc3339(d),
                         d.strftime('%Y-%m-%dT%H:%M:%S') + timezone)

    def test_datetime_utc(self):
        d = datetime.datetime.now()
        d_utc = d + self.local_utcoffset
        self.assertEqual(rfc3339(d, utc=True),
                         d_utc.strftime('%Y-%m-%dT%H:%M:%SZ'))

    def test_date(self):
        d = datetime.date.today()
        self.assertEqual(rfc3339(d),
                         d.strftime('%Y-%m-%dT%H:%M:%S') + self.local_timezone)

    def test_date_utc(self):
        d = datetime.date.today()
        # Convert `date` to `datetime`, since `date` ignores seconds and hours
        # in timedeltas:
        # >>> datetime.date(2008, 9, 7) + datetime.timedelta(hours=23)
        # datetime.date(2008, 9, 7)
        d_utc = datetime.datetime(*d.timetuple()[:3]) + self.local_utcoffset
        self.assertEqual(rfc3339(d, utc=True),
                         d_utc.strftime('%Y-%m-%dT%H:%M:%SZ'))

    def test_timestamp(self):
        d = time.time()
        self.assertEqual(rfc3339(d),
                         datetime.datetime.fromtimestamp(d).\
                         strftime('%Y-%m-%dT%H:%M:%S') + self.local_timezone)

    def test_timestamp_utc(self):
        d = time.time()
        d_utc = datetime.datetime.utcfromtimestamp(d) + self.local_utcoffset
        self.assertEqual(rfc3339(d),
                         (d_utc.strftime('%Y-%m-%dT%H:%M:%S') +
                          self.local_timezone))

    # If these tests start failing it probably means there was a policy change
    # for the Pacific time zone.
    # See http://en.wikipedia.org/wiki/Pacific_Time_Zone.
    if 'PST' in time.tzname:
        def testPDTChange(self):
            '''Test Daylight saving change'''
            # PDT switch happens at 2AM on March 14, 2010

            # 1:59AM PST
            self.assertEqual(rfc3339(datetime.datetime(2010, 3, 14, 1, 59)),
                             '2010-03-14T01:59:00-08:00')
            # 3AM PDT
            self.assertEqual(rfc3339(datetime.datetime(2010, 3, 14, 3, 0)),
                             '2010-03-14T03:00:00-07:00')

        def testPSTChange(self):
            '''Test Standard time change'''
            # PST switch happens at 2AM on November 6, 2010

            # 0:59AM PDT
            self.assertEqual(rfc3339(datetime.datetime(2010, 11, 7, 0, 59)),
                             '2010-11-07T00:59:00-07:00')

            # 1:00AM PST
            # There's no way to have 1:00AM PST without a proper tzinfo
            self.assertEqual(rfc3339(datetime.datetime(2010, 11, 7, 1, 0)),
                             '2010-11-07T01:00:00-07:00')


if __name__ == '__main__': # pragma: no cover
    import doctest
    doctest.testmod()
    unittest.main()
