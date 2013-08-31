#!/usr/bin/env python
"""Test for regressions in the search engine."""

from imdb import IMDb
import sys

errors = 0

imdb = IMDb(dbfile='imdb.zip')
for line in sys.stdin:
    line = line.decode('utf-8').strip()
    if not line or line[0] == '#':
        continue
    title, year, match = line.split('|')
    print '%s: ' % match,
    results = imdb.search(title, year=(int(year) if year else None))
    result = results[0][0].title if results else None
    if result and result == match:
        print 'OK'
    else:
        print 'NOT OK; got %s' % result
        errors += 1

print "Tests complete; %d errors." % errors
if errors > 0:
    sys.exit(1)
