#!/usr/bin/env python
"""A simple example to query the database."""

from imdb import IMDb

imdb = IMDb(dbfile='imdb.zip')
# imdb.rebuild_index('/path/to/imdb')
results = imdb.search('War Games (1983)')
titles = [title for title, score in results]
imdb.populate_rating(titles)
for title in titles:
    r = title.rating
    print (u'%s has rating %s/10 (%d ratings)' %
           (title, str(r.score), r.nratings)).encode('utf-8')
