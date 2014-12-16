#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
  Produce a report of daily activity on Wikimedia blog and e-mail it

  Copyright (C) 2013 Wikimedia Foundation
  Licensed under the GNU Public License, version 2

"""
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import argparse
import collections
import operator
import os
import re
import socket
import subprocess
import urlparse

from cStringIO import StringIO
from datetime import datetime, timedelta
from email.mime.text import MIMEText

from sqlalchemy import create_engine, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


def parse_string_to_date(date_str):
    """Parse a string into a datetime.date.

    If the string cannot get parsed to a date, a ValueError is raised.

    :param date_str: String to parse to a datetime.date
    """
    if date_str == 'yesterday':
        return datetime.utcnow().date() - timedelta(days=1)

    # Try to parse ISO date
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').date()
    except ValueError:
        raise ValueError("Could not parse '%s' as date" % (date_str))


def send_email(sender, recipient, subject, text, cc=None):
    """Send an e-mail by shelling out to 'sendmail'."""
    message = MIMEText(text)
    message['From'] = sender
    message['To'] = recipient
    if cc is not None:
        message['Cc'] = cc
    message['Subject'] = subject
    p = subprocess.Popen(('/usr/sbin/sendmail', '-t'), stdin=subprocess.PIPE)
    p.communicate(message.as_string().encode('utf8'))


if __name__ == "__main__":
    argparser = argparse.ArgumentParser(
        description='Report daily activity on Wikimedia blog')
    argparser.add_argument('--date', default='yesterday',
                           help='Date to compute the report for. Either '
                           '\'yesterday\' or given as YYYY-MM-DD. Has to be a '
                           'past date. (default: yesterday)')

    args = argparser.parse_args()

    # FIXME: Load configs from a file.
    db_url = os.environ['BLOGREPORT_DB']
    email_sender = os.environ['BLOGREPORT_FROM']
    email_recipient = os.environ['BLOGREPORT_TO']
    email_cc = os.environ['BLOGREPORT_CC']

    date_of_interest = parse_string_to_date(args.date)
    if date_of_interest >= datetime.utcnow().date():
        raise ValueError("Given date is not before today")

    Base = declarative_base()
    Base.metadata.bind = create_engine(db_url)
    Session = sessionmaker(bind=Base.metadata.bind, autocommit=True)

    class BlogVisit(Base):
        __table__ = Table('WikimediaBlogVisit_5308166', Base.metadata,
                          autoload=True)

    session = Session()
    q = session.query(BlogVisit).filter(
        BlogVisit.webHost == 'blog.wikimedia.org')
    q = q.filter(BlogVisit.timestamp.startswith(
        date_of_interest.strftime('%Y%m%d')))

    uniques = set()
    visits = 0
    referrers = collections.Counter()
    searches = collections.Counter()
    urls = collections.Counter()
    ref_domains = collections.Counter()

    for visit in q:
        # Exclude previews, testblogs, and WP admin pages
        if re.search(r'[&?]preview=|testblog|\/wp-',
                     visit.event_requestUrl):
            continue
        # Transform all searches into '(search)'
        if re.search(r'[&?]s=', visit.event_requestUrl):
            try:
                visit_path = visit.event_requestUrl.rsplit('?', 1)[1]
                search = dict(urlparse.parse_qsl(visit_path)).pop('s', '')
                searches[search] += 1
            except:
                pass
            visit.event_requestUrl = '(search)'
        urls[visit.event_requestUrl] += 1
        visits += 1
        uniques.add(visit.clientIp)
        ref = visit.event_referrerUrl
        if ref is not None:
            if ref.startswith('https://blog.wikimedia.org'):
                ref = ref[26:]
            domain = urlparse.urlparse(visit.event_referrerUrl).hostname
            if domain:
                if domain.startswith('www.'):
                    domain = domain[4:]
                ref_domains[domain] += 1
        referrers[ref] += 1

    body = StringIO()

    body.write('Total visits: %d\n' % visits)
    body.write('Unique visitors: %d\n' % len(uniques))
    body.write('\n')

    body.write('\n')
    body.write('Pages / hits (ordered by number of hits):\n')
    body.write('=========================================\n')
    for url, count in sorted(urls.iteritems(), key=operator.itemgetter(1),
                             reverse=True):
        body.write('%s\t%s\n' % (url, count))

    body.seek(0)

    send_email(
        'eventlogging@stat1.eqiad.wmnet',
        'tbayer@wikimedia.org',
        'Wikimedia blog stats for %s: pageviews'
        % date_of_interest.strftime('%Y-%m-%d'),
        body.read(),
        'ori@wikimedia.org'
        )

    body.close()
    body = StringIO()

    body.write('Search queries / count (sorted by number of queries):\n')
    body.write('=====================================================\n')
    for search, count in sorted(searches.iteritems(),
                                key=operator.itemgetter(1), reverse=True):
        body.write('"%s"\t%s\n' % (search, count))

    body.write('\n')
    body.write('Referring domain names / referrals (sorted by number of '
               'referrals):\n')
    body.write('========================================================'
               '===========\n')
    for hostname, count in sorted(ref_domains.iteritems(),
                                  key=operator.itemgetter(1), reverse=True):
        body.write('%s\t%s\n' % (hostname, count))

    body.write('\n')
    body.write('Referrers / count (sorted alphabetically):\n')
    body.write('==========================================\n')
    for url, count in sorted(
            sorted(referrers.iteritems(), key=operator.itemgetter(0)),
            reverse=True):
        if url is None:
            url = '(no referrer)'
        body.write('%s\t%s\n' % (url, count))

    body.seek(0)

    send_email(
        'blogreport@' + socket.getfqdn(),
        email_recipient,
        'Wikimedia blog stats for %s: referrers & searches'
        % date_of_interest.strftime('%Y-%m-%d'),
        body.read(),
        email_cc,
    )
