"""imdb - Read and search IMDb plain text data files.

http://www.imdb.com/interfaces
ftp://ftp.fu-berlin.de/pub/misc/movies/database/
ftp://ftp.funet.fi/pub/mirrors/ftp.imdb.com/pub/
ftp://ftp.sunet.se/pub/tv+movies/imdb/
"""

import heapq
import re
import os

from chunkedfile import ChunkedFile
from utils import Timer
import parsers
import search

# Notes on handling plot summaries:
# - HTML entities
# - qv links http://www.imdb.com/updates/guide/tgq_qv
# - MovieGuide should use Markdown-escaper (but leave HTML entities)
# Note title formats: http://www.imdb.com/updates/guide/title_formats
# FAQ: http://www.imdb.com/updates/guide/
# Linking to IMDb: http://www.imdb.com/Title?The+Bourne+Ultimatum+(2007)
#   Old-style URL; degrades to search results if not found.
#   URLencode with + not %20, iso-8859-1 not utf-8)

class IMDbTitle(object):
    """An object representing a title entry in IMDb.
    If a backend IMDb object is provided, undefined attributes (e.g. rating)
    will be populated from the backend on-demand. Note that if populating
    multiple IMDbTitles is desired, it will be much faster to use
    IMDb.populate_rating or equivalent."""

    def __init__(self, title, backend=None):
        self.title = title
        self.backend = backend
        self.name, self.year, self.unique, self.cat = self.parse(title)[1:]
        self.aka = None

    def __repr__(self):
        return 'IMDbTitle(%s)' % repr(self.title)

    def __unicode__(self):
        return self.title
    def __str__(self):
        return self.__unicode__().encode('utf-8')

    # For getters/setters for movie data see _install_parsers, below.

    parse = staticmethod(parsers.parse_title)

class IMDb(object):
    """Main interface to IMDb."""

    def __init__(self, dbfile, debug=False):
        self.dbfile = dbfile
        self.debug = debug

    def rebuild_index(self, dbdir):
        """Convert and index data files for random access.
           Index movie list for searching."""
        # Import and index data files
        if os.path.exists(self.dbfile):
            raise Exception('%s exists' % self.dbfile)
        for parsername, parser in parsers.parsers():
            obj = parser(dbfile=self.dbfile, dbdir=dbdir, debug=self.debug)
            if self.debug:
                print "Indexing %s..." % parsername
            with Timer(indent=2, quiet=not self.debug):
                obj.rebuild_index(do_copy=True)

        # Create index of movie titles
        if self.debug:
            print "Creating search index..."
        with Timer(indent=2, quiet=not self.debug):
            search.create_index(self.dbfile, dbdir, debug=self.debug)

    def search(self, query, year=None, timeout=None):
        """Search the database for query, optionally with an estimated year."""
        scores, akascores = search.search(self.dbfile, query, year,
                                          debug=self.debug, timeout=timeout)

        # Return the top-scoring results
        numret = 30
        topscores = heapq.nlargest(numret, scores, scores.get)
        titles = dict((title, IMDbTitle(title, backend=self)) \
                        for title in topscores)
        for title, obj in titles.items():
            if title in akascores:
                obj.aka = akascores[title]
        return [(titles[title], scores[title]) for title in topscores]

# For each parser, add a corresponding property to the IMDbTitle class and a
# populator (to load data into one or more IMDBTitles) to the IMDb class.

def imdbtitle_property(name):
    """Create and return an IMDbTitle property for a type of movie data.
    Uses self.backend.populate_whatever to load the data from the database."""
    populater = 'populate_'+name
    data_val = '_'+name

    def getter(self):
        """Auto-generted getter for this property."""
        if not hasattr(self, data_val):
            populate_func = getattr(self.backend, populater)
            populate_func((self,))
        return getattr(self, data_val)

    def setter(self, value):
        """Auto-generated setter for this property."""
        setattr(self, data_val, value)

    return (getter, setter)

def imdb_populator(parserclass, prop, default):
    """Create and return an IMDb method to populate (from the database) some
    property for multiple IMDbTitle objects."""
    def populate(self, titles):
        """Auto-generated function to populate (from the database) this
        property for multiple IMDbTitle objects."""
        titles = tuple(title for title in titles)
        # FIXME: Optimize if title._rating is None)
        parser = parserclass(dbfile=self.dbfile, debug=self.debug)
        results = parser.search(title.title for title in titles)
        for title in titles:
            if title.title in results:
                setattr(title, prop, results[title.title])
            else:               # No data available
                setattr(title, prop, default)
    return populate

def _install_parsers():
    """Install support for each parser into the IMDb and IMDbTitle classes."""
    property_name = re.compile(r'(?<=[a-z])([A-Z])')
    for name, parser in parsers.parsers():
        name = property_name.sub(r'_\1', name).lower()
        populator = imdb_populator(parser, name, default=parser.default)
        setattr(IMDb, 'populate_'+name, populator)
        prop = property(*imdbtitle_property(name),
                        doc="""IMDb """+name+""" autogenerated property.""")
        setattr(IMDbTitle, name, prop)

_install_parsers()

