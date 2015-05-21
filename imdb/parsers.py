"""parsers - Parsers for IMDB data files."""

from collections import Counter, namedtuple, defaultdict
import os.path
import re

from chunkedfile import ChunkedFile
from utils import Timer, open_compressed

# Data types
IMDbRating = namedtuple('IMDbRating',
                        ('distribution', 'nratings', 'score'))
IMDbPlot = namedtuple('IMDbPlot', ('summary, byline'))

# Parser enumeration
def parsers():
    """Return a list of available parsers in the form (parsername, parser)."""
    for parsername, parser in sorted(globals().items()):
        if parsername[0] == '_' or not parsername.endswith('Parser'):
            continue
        if hasattr(parser, 'is_property'):
            yield (parsername[4:-6], parser)

# Parser for titles and names
IMDbParsedTitle = namedtuple('IMDbParsedTitle',
                             ('title', 'name', 'year', 'unique', 'cat'))
IMDbParsedName = namedtuple('IMDbParsedName',
                            ('name', 'first', 'last', 'unique'))
# Regular expressions for IMDb title and name suffixes
TITLERE = re.compile(r'^(?P<title>(?P<name>.+?)(?: \((?:'
                     +r'(?P<TV>TV)|(?P<V>V)|(?P<VG>VG)|(?P<mini>mini)|'
                     +r'(?P<year>\d{4}|\?{4})(?P<unique>/[IVXLCDM]+)?)\))+)'
                     +r'(?P<trailing>(?:  .*)?)$',
                     re.UNICODE)
NAMERE = re.compile(r'^(?P<name>(?P<last>.+?)(?:, (?P<first>.+?))?(?: \((?:'
                    +r'(?P<unique>[IVXLCDM]+))\))*)$',
                    #+r'(?P<trailing>.*?)$',
                    re.UNICODE)

# Data attached to entries in names databases e.g. "(uncredited)", "[Driver]",
# etc.
CASTRE = re.compile(r'(?P<notes>  \(.+?\))*'
                    +r'(?:  \[(?P<character>.+?)\])?'
                    +r'(?:  <(?P<order>\d+)>)?'
                    +r'(?P<trailing>.*?)$',
                    re.UNICODE)

def parse_title(title):
    """Parse a title string into its components and return an
    IMDbParsedTitle object."""
    match = TITLERE.match(title)
    if not match or match.group('trailing'):
        raise ValueError('Cannot parse "%s" as an IMDb title' % (title,))
    name = match.group('name')
    year = match.group('year')
    if year == u'????':
        year = None
    unique = match.group('unique')

    # Handle movie type (TV, etc.)
    cat = None
    #for i in ('TV','V','VG','mini'):
    #    if match.group(i):
    #        cat = match.group(i)
    ## Do not index video games
    #if cat == "VG":
    #    continue

    # Detect TV show, strip surrounding quotes.
    if name[0] == '"' and name[-1] == '"':
        name = name[1:-1]
        assert(cat is None)
        cat = "TV Show"

    return IMDbParsedTitle(title, name, year, unique, cat)

def parse_name(name):
    """Parse a name string into its components and return an
    IMDbParsedName object."""
    match = NAMERE.match(name)
    if not match:# or match.group('trailing'):
        raise ValueError('Cannot parse "%s" as an IMDb name' % (name,))
    first = match.group('first') or None
    last = match.group('last')
    unique = match.group('unique')
    return IMDbParsedName(name, first, last, unique)

# Helper functions
def _skip_to(fileobj, indicator, additional_skips):
    """Iterate fileobj until a value matching indicator is reached, then
    skip additional_skips more values. Returns the number of lines read.
    Used for skipping headers in IMDb files.
    """
    i = 1
    for line in fileobj:
        if line.strip() == indicator:
            break
        i += 1
    else:
        raise SyntaxError("File did not contain expected indicator")
    # Skip additional lines as required
    for i in xrange(additional_skips):
        next(fileobj)
    return i + additional_skips

# File seeking
def _find_seeks_index(dbfile, indexname, queries, debug=False):
    """Use the index file to find exact seek positions for relevant
    records. End locations are not necessary since we are guaranteed that
    the data will be present, so a number of occurances is sufficient for
    prompt termination."""
    timer = Timer(rl_min_dur=1)
    locs = Counter()
    if debug:
        print "  Searching index..."
    indexfh = ChunkedFile(dbfile, indexname, mode='r')
    last_bookmark = 0
    for query in sorted(queries):
        # Use bookmarks to rapidly search the index!
        bookmark = indexfh.find_bookmark(query.encode('utf-8'))
        if bookmark != last_bookmark:
            indexfh.seek(bookmark)
            #print "  Seek to", bookmark
            last_bookmark = bookmark
        for i, line in enumerate(indexfh):
            title, nums = line.decode('utf-8').split('\t')
            if i % 100 == 0:
                timer.step()
            if title in queries:
                locs.update(int(x) for x in nums.split(' '))
            elif title > query:
                break   # This works because the index is sorted.
    indexfh.close()
    for start, nresults in sorted(locs.items()):
        yield (start, None, nresults)
    if debug:
        print '  Completed in', timer, 'seconds.'

def _find_seeks_bookmarks(fileobj, queries, debug=False):
    """Use bookmarks to find sections of the file that *may* contain
    the data we're looking for. End locations are required to ensure
    prompt termination, since we have no idea if the file contains the
    information we're looking for."""
    timer = Timer()
    locs = Counter()
    endlocs = {}
    if debug:
        print "  Searching bookmarks..."
    for query in sorted(queries):
        start, end = fileobj.find_bookmark(query.encode('utf-8'),
                                           give_range=True)
        #print "Got",start,end,"for",query.encode('utf-8')
        if not end:
            endlocs[start] = None   # None = EOF
        elif start not in endlocs or endlocs[start] < end:
            endlocs[start] = end
        #print '  ',endlocs[start]
        locs.update((start,))

    # Because of the inexact nature of bookmarks, we need to normalize
    # the locations to a non-overlapping set of ranges, some of which
    # may be infinite.
    start = 0
    end = 0
    nresults = 0
    for nextstart, nextnresults in sorted(locs.items()):
        nextend = endlocs[nextstart] if nextstart in endlocs else None
        assert(nextnresults > 0)
        #print "Got",startloc,endloc
        if end is None or nextstart <= end:
            # Previous range ends after start of this range (or the
            # previous range is infinite). Extend the previous range
            # to include this one.
            if nextend is None or (end is not None and nextend > end):
                end = nextend
            nresults += nextnresults
        else:
            # The ranges do not overlap. Return the previous range and
            # then save the current range.
            if nresults > 0:
                #assert(end is None or end > start)
                yield (start, end, nresults)
            start = nextstart
            end = nextend
            nresults = nextnresults
    if nresults > 0:
        #assert(end is None or end > start)
        yield (start, end, nresults)
    if debug:
        print '  Completed in', timer, 'seconds.'


# Parser objects
class _IMDbParser(object):
    """A generic parser for IMDb plain text data files."""

    filenames = []
    default = None

    def __init__(self, dbfile, dbdir=None, debug=False):
        self.dbfile = dbfile
        self.listname = self.__class__.__name__[4:-6].lower()
        self.indexname = self.listname + '.index'
        if dbdir:
            self.origfiles = [os.path.join(dbdir, fn + '.list.gz') \
                for fn in self.filenames]
        else:
            self.listorig = None
        self.skip_tvvg = False
        self.debug = debug

    def rebuild_index(self, do_copy=True):
        """Create an index for this file, to allow rapid seeking to information
        about a given title."""
        if do_copy:
            copy_to = ChunkedFile(self.dbfile, self.listname, mode='a',
                                  autoflush=True if self.indexname else False)
            tellobj = copy_to
            filenames = self.origfiles
        else:
            #filenames = ???
            copy_to = None
            raise NotImplementedError

        indexobj = defaultdict(list)

        for filename in filenames:
            if do_copy:
                try:
                    fileobj = open_compressed(filename)
                except IOError as e:
                    print "  Skipping %s: %s" % (filename, e.strerror)
                    continue
            else:
                fileobj = ChunkedFile(self.dbfile, self.listname, mode='r')
                tellobj = fileobj

            self._skip_header(fileobj)
            # Get location of this line
            loc = tellobj.tell()
            for line in fileobj:
                # Do not index video games or individual TV episodes
                # (Not applicable for all file types)
                if self.skip_tvvg and ('(VG)' in line or '{' in line):
                    continue
                if copy_to:
                    copy_to.write(line)
                # Decode database (IMDb databases use ISO-8859-1)
                line = line.rstrip().decode('iso-8859-1')

                data = self._parse_line(line, loc)
                loc = tellobj.tell()
                if data is None:
                    break           # End of database
                if not data:
                    continue        # Skip this line

                # Add to the index
                title, idxline = data[0:2] #self._make_locator(data)
                title = title.encode('utf-8')
                if self.indexname:
                    indexobj[title].append(idxline)
                elif copy_to:
                    copy_to.bookmark(title)
            fileobj.close()
        if copy_to:
            copy_to.close()

        if self.indexname:
            # Write out a separate index, if required (e.g. names databases)
            indexfh = ChunkedFile(self.dbfile, self.indexname, mode='a',
                                  autoflush=False)
            for title, linenos in sorted(indexobj.items()):
                indexfh.write(title)
                indexfh.write("\t")
                indexfh.write(' '.join(str(i) for i in linenos))
                indexfh.write("\n")
                indexfh.bookmark(title)
            indexfh.close()
        else:
            # An index is required to use more than one file, since the
            # resulting combination will not be sorted
            assert(len(filenames) == 1)

    def _run_search(self, queries):
        """Return items from the data file matching any item in queries."""
        if queries is not None:
            queries = set(queries)
            # Don't do anything if an empty set is provided
            if not queries:
                return

        # Open the compressed database, either copied version or original file.
        if self.dbfile:
            fileobj = ChunkedFile(self.dbfile, self.listname, mode='r')
        else:
            assert(len(self.origfiles) == 1)
            try:
                fileobj = open_compressed(self.origfiles[0])
            except IOError as e:
                print "Skipping %s: %s" % (self.origfiles[0], e.strerror)
                return
            self._skip_header(fileobj)
        if self.debug:
            print "Reading %s..." % self.listname

        # Locate seek positions for all queries
        if queries and self.indexname:  # Use index
            locs = list(_find_seeks_index(self.dbfile, self.indexname, queries,
                                          debug=self.debug))
        elif queries:                   # Use bookmarks
            locs = list(_find_seeks_bookmarks(fileobj, queries,
                                              debug=self.debug))
        else:
            locs = [(None, None, 1)]     # Dummy values to start loop

        # Read selected lines from the file
        timer = Timer()
        loc = 0
        for startloc, endloc, nresults in locs:
            # Skip to the correct position in the file
            if queries:
                if startloc > loc:
                    #print "  Seek to", startloc
                    fileobj.seek(startloc)
                    loc = fileobj.tell()
                elif startloc < loc:
                    #print "  Skipping", startloc, "already at", loc
                    continue
                #else:
                #    print "  Skipping", startloc, "already there"
                #print "    Finish at", endloc, "after", nresults, "results"
            for _ in xrange(nresults):
                # Parse the file until we get a result
                for i, line in enumerate(fileobj):
                    # Determine if we have reached the end location for this
                    # section
                    if endloc and loc == endloc:
                        break
                    #assert(not endloc or loc < endloc)

                    # Do not index video games or individual TV episodes
                    # (Not applicable for all file types)
                    if not self.dbfile and self.skip_tvvg and \
                            ('(VG)' in line or '{' in line):
                        #loc = fileobj.tell() # Don't seek/tell in gzip
                        continue
                    # Decode database (IMDb databases use ISO-8859-1)
                    line = line.rstrip().decode('iso-8859-1')

                    if queries and i % 100 == 0:
                        timer.step()

                    data = self._parse_line(line, loc)
                    if self.dbfile:
                        loc = fileobj.tell()

                    if data is None:
                        break           # End of database
                    if not data:
                        continue        # Skip this line

                    # Check if one of our queries matches
                    if queries is None or data[0] in queries:
                        yield self._make_result(data)
                        if queries is not None:
                            # queries.remove(data[0])
                            break

        if self.debug:
            print 'Completed in', timer, 'seconds.'
        fileobj.close()

    def search(self, queries=None):
        """Perform a search, returning results after optional subclass-specific
        postprocessing.
        """
        return self._run_search(queries)

    def _skip_header(self, fileobj):
        """Skip header lines in fileobj (as an iterator)"""
        raise NotImplementedError

    def _parse_line(self, line, loc):
        """Parse a line of data.
        Return the parsed content as a tuple.
        First element must be title (to match against query)
        """
        raise NotImplementedError

    def _make_result(self, data):
        """Format the data from parse_line for return by run_search.
        For example, convert data-types, reorder elements, etc.
        Only called for data that matches a query.
        """
        return (data[0],) + tuple(data[2:])

    #def _make_locator(self, data):
    #    """Format the data from parse_line for return by run_search.
    #    For example, convert data-types, reorder elements, etc.
    #    Only called for data that matches a query.
    #    """
    #    return data[0:2]
    # Removed because nobody overrides it.

class IMDbMoviesParser(_IMDbParser):
    """Parser for IMDb data file movies.list.gz."""

    filenames = ['movies']

    def __init__(self, dbfile=None, dbdir=None, debug=False):
        super(IMDbMoviesParser, self).__init__(dbfile, dbdir, debug)
        self.skip_tvvg = True

    def _skip_header(self, fileobj):
        return _skip_to(fileobj, '===========', 1)

    def _parse_line(self, line, loc):
        try:
            fullname, _ = line.split("\t", 1)
        except (ValueError, IndexError):
            if line == '-'*80:
                return None
            raise
        return (fullname, loc)

    # def _make_result(self, data)
    # def _make_locator(self, data)

    def search(self, queries=None):
        # Return the iterator
        return self._run_search(None)

class IMDbAkaParser(_IMDbParser):
    """Parser for IMDb data file aka-titles."""

    filenames = ['aka-titles']

    def __init__(self, dbfile=None, dbdir=None, debug=False):
        self.last_title = None  # FIXME: not thread-safe
        super(IMDbAkaParser, self).__init__(dbfile, dbdir, debug)
        self.skip_tvvg = True

    def _skip_header(self, fileobj):
        return _skip_to(fileobj, '===============', 2)

    def _parse_line(self, line, loc):
        if not line:
            self.last_title = None
            return ()   # Blank lines between entries
        if line.startswith('   (aka '):
            # An alternate name. Example: '   (aka Die Hard 4.0 (2007))\t(UK)'
            assert(self.last_title)
            info = line[8:].split("\t")
            return (self.last_title, loc, info[0][:-1],
                    info[1] if len(info) > 1 else None)
        elif line[0] != ' ':
            # A title; alternate names will follow
            self.last_title = line
            return ()
        else:
            raise Exception

    # def _make_result(self, data)
    # def _make_locator(self, data)
    # def search(self, queries=None)

class IMDbRatingParser(_IMDbParser):
    """Parser for IMDb data file ratings."""

    # Note that this file also includes Top 250 and Bottom 10 ratings.

    filenames = ['ratings']
    default = IMDbRating('..........', 0, '0')
    is_property = True

    def __init__(self, dbfile=None, dbdir=None, debug=False):
        super(IMDbRatingParser, self).__init__(dbfile, dbdir, debug)
        self.skip_tvvg = True
        self.indexname = None

    def _skip_header(self, fileobj):
        return _skip_to(fileobj, 'MOVIE RATINGS REPORT', 2)

    def _parse_line(self, line, loc):
        try:
            distribution, nratings, score, title = line[6:].split(None, 3)
            return (title, loc, (distribution, nratings, score))
        except (ValueError, IndexError):
            if line == '':
                return None
            else:
                raise

    def _make_result(self, (title, _, (distribution, nratings, score))):
        return (title, IMDbRating(distribution, int(nratings, 10), score))

    #def _make_locator(self, data)

    def search(self, queries=None):
        # Return a dictionary
        return dict(self._run_search(queries))

class IMDbPlotParser(_IMDbParser):
    """Parser for IMDb data file plot.."""

    filenames = ['plot']
    default = IMDbPlot(None, None)
    is_property = True

    def __init__(self, dbfile=None, dbdir=None, debug=False):
        super(IMDbPlotParser, self).__init__(dbfile, dbdir, debug)
        self.last_title = None
        self.title_begin = None
        self.last_plot = []
        # INDEX REQUIRED for this file, as it is not correctly sorted:
        # "10 Things I Hate About You" (2009) follows "10 om te zien" (1989)

    def _skip_header(self, fileobj):
        return _skip_to(fileobj, '===================', 1)

    def _parse_line(self, line, loc):
        if line:
            tag, data = line[0:2], line[4:]
        else:
            tag, data = '--', None
        if tag == 'MV':
            if '(VG)' in data or '{' in data:
                self.last_title = None
                # FIXME: Do not output plots for video games and TV episodes.
            else:
                self.last_title = data
                self.title_begin = loc
                assert(not self.last_plot)
        elif not self.last_title:
            return ()
            ## Skip to the next title
            #try:
            #    _skip_to(self.fileobj, '-'*79, 0) # FIXME
            #except SyntaxError:
            #    # End-of-file
            #    return None
        elif tag == 'PL':
            self.last_plot.append(data)
        elif self.last_plot:
            # Return the plot summary for this title
            assert(self.last_title)
            title = self.last_title
            plot = self.last_plot
            self.last_plot = []

            return (title, self.title_begin, (plot, None))
            #data if tag == 'BY' else None) # FIXME: bylines
        return ()

    def _make_result(self, (title, _, (plot, byline))):
        return (title, IMDbPlot(' '.join(plot), byline))

    # def _make_locator(self, data)

    def search(self, queries=None):
        # Return a dictionary that contains the shortest plot summary.
        # Test with, e.g. [Rec] (2007) and [Rec] 2 (2009).
        data = defaultdict(list)
        for title, value in self._run_search(queries):
            data[title].append(value)
        for title in data.keys():
            data[title] = sorted(data[title], key=lambda x: len(x[0]))[0]
        return data

class _IMDbBasicParser(_IMDbParser):
    """Parser for IMDb data files formatted as basic lists
       (color-info, running-times, etc.)."""

    default = None

    def __init__(self, dbfile=None, dbdir=None, debug=False):
        super(_IMDbBasicParser, self).__init__(dbfile, dbdir, debug)
        self.skip_tvvg = True
        # Genres and running times may have duplicate entries.
        # So we have to have an index.
        # Ick.

    def _skip_header(self, fileobj):
        return _skip_to(fileobj, '-'*77, 3)

    def _parse_line(self, line, loc):
        try:
            # Note: There may be multiple consecutive delimters
            data = [i for i in line.split("\t") if i]
            if len(data) < 2:
                # Some entries in running-times have no actual running time.
                return ()
            return (data[0], loc, data[1])
        except IndexError:
            if line == '-'*80:
                return None
            else:
                print line
                raise

    # def _make_result(self, data)
    # def _make_locator(self, data)
    # def search(self, queries=None)

class IMDbColorInfoParser(_IMDbBasicParser):
    """Parser for IMDb data file color-info."""

    filenames = ['color-info']
    is_property = True

    def __init__(self, dbfile=None, dbdir=None, debug=False):
        super(IMDbColorInfoParser, self).__init__(dbfile, dbdir, debug)
        self.indexname = None

    def search(self, queries=None):
        # Just return a dictionary, since there shouldn't be any duplicate
        # entries.
        return dict(self._run_search(queries))

class IMDbGenresParser(_IMDbBasicParser):
    """Parser for IMDb data file genres."""

    filenames = ['genres']
    default = []
    is_property = True

    def _skip_header(self, fileobj):
        return _skip_to(fileobj, '8: THE GENRES LIST', 2)

    def search(self, queries=None):
        # Return a dictionary that contains a sorted list of genres
        data = defaultdict(list)
        for title, value in self._run_search(queries):
            data[title].append(value)
        for datalist in data.values():
            datalist.sort()
        return data

class IMDbRunningTimeParser(_IMDbBasicParser):
    """Parser for IMDb data file running-times."""

    filenames = ['running-times']
    is_property = True

    def _make_result(self, (title, _, duration)):
        # Durations are of the form "[COUNTRY:]DURATION[:NUMBERS]"
        # For example: "USA:30" or "27 min." or "1:10:43". We return
        # the first colon-separated number in the string as the
        # running time, ignoring all trailing garbage. (This leads to
        # some running times being erroneously listed as 1 minute, but
        # the IMDb website also does this.)
        if duration[0].isdigit():
            # No leading country
            country = None
        else:
            country, duration = duration.split(':', 1)
        duration = duration.strip()
        # running-times.gz has 7 films with a running time like "54 min."
        # instead of "54". There's also "1o7", "2 1/2", "2 x 90", ....
        try:
            duration = int(duration, 10)
        except ValueError:
            # Could not parse as integer. Extract leading digits.
            for i, c in enumerate(duration):
                if not c.isdigit():
                    duration = int(duration[:i], 10)
                    break
            else:
                duration = None
        return (title, (duration, country))

    def search(self, queries=None):
        # Return a dictionary that contains the average running time
        data = defaultdict(list)
        for title, value in self._run_search(queries):
            data[title].append(value[0])
        for title in data.keys():
            data[title] = sorted(data[title])[int(len(data[title])/2)] # Median
            #data[title] = int(sum(data[title])/len(data[title])) # Mean
        return data

class IMDbCertificatesParser(_IMDbBasicParser):
    """Parser for IMDb data file certificates."""

    filenames = ['certificates']
    is_property = True
    countries = ['USA']

    def __init__(self, dbfile=None, dbdir=None, debug=False):
        super(IMDbCertificatesParser, self).__init__(dbfile, dbdir, debug)
        self.indexname = None

    def _parse_line(self, line, loc):
        try:
            # Note: There may be multiple consecutive delimters
            data = [i for i in line.split("\t") if i]
            # Check for approved country
            if ':' not in data[1]:
                return ()
            country, certificate = data[1].split(':', 1)
            if country not in self.countries:
                return ()
            else:
                return (data[0], loc, (certificate, country))
        except IndexError:
            if line == '-'*80:
                return None
            else:
                print line
                raise

    # def _make_result(self, data)

    def search(self, queries=None):
        # Just return a dictionary, since there shouldn't be any duplicate
        # entries. (FIXME: multiple supported countries)
        return dict(self._run_search(queries))


class _IMDbNamesParser(_IMDbParser):
    """Parser for IMDb data files formatted as lists of names
       (actors, directors, etc.)"""

    default = []

    def __init__(self, dbfile=None, dbdir=None, debug=False):
        super(_IMDbNamesParser, self).__init__(dbfile, dbdir, debug)
        self.last_person = (None, None)  # FIXME: not thread-safe

    def _skip_header(self, fileobj):
        return _skip_to(fileobj, "----\t\t\t------", 0)

    def _parse_line(self, line, loc):
        if not line:
            self.last_person = (None, None)
            return ()   # Blank lines between entries
        try:
            if line[0] != "\t":
                newperson, line = line.split("\t", 1)
                self.last_person = (newperson, loc)
            line = line.strip()
            # Skip video games and TV episodes
            if '(VG)' in line or '{' in line:
                return ()
            # Another credit for the person
            assert(self.last_person[0])
            # Separate character, cast order information
            match = TITLERE.match(line)
            if not match:
                raise ValueError('Cannot extract title from %s' % (line,))
            title = match.group('title')
            match = CASTRE.match(match.group('trailing'))
            if not match:
                raise ValueError('Cannot extract casting from %s' % (line,))
            #if match.group('trailing'):
            #    print '"%s" has trailing garbage "%s"' \
            #        % (line, match.group('trailing'))
            character = match.group('character')
            #character = None
            order = match.group('order')
            notes = match.group('notes')
            if order:
                order = int(order, 10)
            return (title, self.last_person[1],
                    (self.last_person[0], character, order, notes))
        except ValueError:
            if line.strip('-') == '' and len(line) > 60:
                return None
            print line
            raise

    # def _make_result(self, data)
    # def _make_locator(self, data)

    def search(self, queries=None):
        # Return a dictionary that contains a sorted list of names
        data = defaultdict(list)
        for title, value in self._run_search(queries):
            data[title].append(value)
        for datalist in data.values():
            datalist.sort(key=lambda x: 9999 if x[2] is None else x[2])
        return data

class IMDbCastParser(_IMDbNamesParser):
    """Parser for IMDb data files actors, actresses."""
    filenames = ['actors', 'actresses']
    is_property = True

class IMDbDirectorsParser(_IMDbNamesParser):
    """Parser for IMDb data file directors."""
    filenames = ['directors']
    is_property = True

class IMDbWritersParser(_IMDbNamesParser):
    """Parser for IMDb data files writers."""
    filenames = ['writers']
    is_property = True

