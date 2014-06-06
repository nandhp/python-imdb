"""search - Search capability for movie list."""

from difflib import SequenceMatcher
from collections import Counter
import re

from chunkedfile import ChunkedFile
from utils import Timer, open_compressed
import parsers
from datetime import date
from math import exp

# Helper functions for search

def _subwords(words, size):
    """Explode each string in words into all substrings of length size.
    (Shorter words will not be exploded).

    Used by search_index to turn (for example) ['Les','Miserables'] into
    ['Les','Miser','isera','serab','erabl','rable','ables'] (with size=5).
    """
    for word in words:
        for i in xrange(len(word)-size+1) or (0,):
            yield word[i:i+size]

_STRIPRE = re.compile('[^a-z0-9 ]+', re.UNICODE)
def _clean_word(word):
    """Clean the string word by lower-casing and removing characters that are
    not alphanumeric or space (including accented characters)."""
    return _STRIPRE.sub('', word.lower()).encode('ascii')

# The four most common three-letter words (in movie titles)
_STEMS = ('the', 'and', 'der', 'for')
def _clean_words(words, strip_stems=True):
    """Normalize a list of words using _clean_word, optionally removing
    very short/common words.
    """
    normed = list(_clean_word(word) for word in words)
    normed = list(word for word in normed if word)
    if not strip_stems or len(words) == 1:
        return normed
    # Will we still have something useful after eliminating short/common words?
    limited = []
    for word in normed:
        if len(word) <= 2 or word in _STEMS:
            continue
        # Do not include small numbers
        if len(word) <= 4:
            try:
                num = int(word)
            except ValueError:
                pass
            else:
                if num < 2100:
                    continue
        limited.append(word)
    return limited if limited else normed

# Search implementation
def create_index(dbfile, dbdir, debug=False):
    """Index the movie list for searching."""
    # Load ratings; number of ratings included in index for score weighting
    ratings = parsers.IMDbRatingParser(dbfile=dbfile, debug=debug).search()

    # Count word frequencies while outputting searchable list
    frequencies = Counter()
    #indexfh = ChunkedFile(dbfile, 'index', mode='a')
    indexfh = open_compressed(dbfile+'.idx', mode='w')

    # Index all IMDb titles
    skipped = 0
    for iterator in \
            (parsers.IMDbMoviesParser(dbfile=None, dbdir=dbdir).search(),
             parsers.IMDbAkaParser(dbfile=None, dbdir=dbdir).search()):
        last_time = None
        for obj in iterator:
            if len(obj) == 1:   # movies.list.gz
                data = parsers.parse_title(obj[0])
                akafor = ''
            else:               # aka-titles.list.gz
                data = parsers.parse_title(obj[1])  # AKA name of the title
                akafor = obj[0]             # Real name of the title
                # If it's a duplicate AKA (for indexing purposes), skip it.
                # The same AKA title may be repeated. For example:
                #     (aka Die Hard 4.0 (2007)) (UK)
                #     (aka Die Hard 4.0 (2007)) (Germany)
                if last_time and last_time[0:2] == obj[0:2]:
                    skipped += 1
                    continue
                last_time = obj
            searchable = _clean_word(data.name).split(' ')
            # Save word frequencies
            frequencies.update(searchable)
            # Determine rating for result ranking
            nratings = 0
            if akafor and akafor in ratings:
                nratings = ratings[akafor].nratings
            elif not akafor and data.title in ratings:
                nratings = ratings[data.title].nratings
            # Write movie to output
            indexfh.write("\t".join((''.join(searchable),
                                     data.year.encode('ascii')
                                     if data.year else '',
                                     data.title.encode('utf-8'),
                                     akafor.encode('utf-8'),
                                     str(nratings))))
            indexfh.write("\n")
    indexfh.close()
    #print "Skipped %d duplicate AKA titles" % skipped

    # Write frequencies to stopwords file
    if False:
        swf = ChunkedFile(dbfile, 'stopwords', mode='a')
        for word, numtimes in frequencies.most_common():
            swf.write("%s %d\n" % (word, numtimes))
        swf.close()

def _search_index(timer, dbfile, words, size, strip_stems=True,
                  year=None, deltayear=8, debug=False):
    """Yield a subset of the database that somewhat matches words.
    Returns any movies that contains a subword of any of words.
    (See the _subwords function.) Shorter subwords means more results, but
    slower performance.

    words -- List of words.
    size -- Length of subwords to use for search. (See _subwords function.)
    strip_stems -- Omit really common subwords. (See _subwords function.)
    year -- A guess of the year. Only returns movies dated near year.
    deltayear -- Only return movies with year [year-deltayear,year+deltayear].
    """
    # Extract a plausible-looking subset of the database so we don't
    # have to run SequenceMatcher on everything. This works pretty
    # well, except for movies like O (2001).

    # A list of plain-text strings that we expect to find in the
    # SEARCHABLE field of the data. We will require at least one of
    # these to be present.
    wordlist = tuple(_subwords(_clean_words(words, strip_stems), size))
    # If we are provided with an estimated year, compose a list of
    # acceptable years.
    validyears = range(year-deltayear, year+deltayear) if year else ()
    if debug:
        print wordlist
        print "Searching..."

    # Reading lines out of a GzipFile is very slow; using gzip(1) is ~6.5x
    # faster. For further speedup, we could use zgrep(1) to extract our
    # subset using grep(1).
    #indexfh = ChunkedFile(dbfile, 'index')
    indexfh = open_compressed(dbfile+'.idx')
    #indexfh = open('idx.tmp')

    for i, line in enumerate(indexfh):
        # Quick check to determine if the entry matches any of our words
        # (grep -F is faster; grep -E might be faster still)
        for word in wordlist:
            if word in line:
                break
        else:
            continue

        # Get SEARCHABLE\tYEAR\tTITLE
        ryear, title, akafor, nratings = line.decode('utf-8').split('\t')[1:]

        # Check that the year is within tolerances
        if validyears and ryear and int(ryear) not in validyears:
            continue

        yield title, ryear, akafor, nratings
        if i % 100 == 0:
            timer.step()
    indexfh.close()
    if debug:
        print 'Completed search in', timer, 'seconds.'

def search(dbfile, query, year=None, size=5, debug=False, timeout=None):
    """Search the database for query, optionally with an estimated year."""
    this_year = date.today().year
    if year:
        year = int(year)
    words = query.split()
    timer = Timer(timeout=timeout)
    results = _search_index(timer, dbfile, words, size, year=year, debug=debug)

    # Similar to diffutils.get_close_matches, but ignores capitalization
    # and IMDb suffixes.
    scores = {}
    akascores = {}
    cutoff = 0.6
    lcquery = query.lower()
    matchers = [(1.0, SequenceMatcher(b=lcquery))]
    if year:
        yearstr = ' ('+str(year)
        if yearstr not in lcquery:
            matchers.append((1.0, SequenceMatcher(b=lcquery+yearstr+')')))

    for title, ryear, akafor, nratings in results:
        titles = [(1.0, title.lower()),
                  (1.0, parsers.TITLERE.match(title).group('name').lower())]
        # Try matching without the subtitle. But only do this if the query
        # included a year, since otherwise "ABC (1991)" and "ABC: Revenge
        # of the DEF (1999)" rank the same. We exact a 95% penalty against
        # matches that occur this way.
        if year and ':' in titles[-1][1]:
            titles.append((0.95, titles[-1][1][0:titles[-1][1].find(':')]))
        # Take highest score from all matches checked
        score = 0
        mycutoff = cutoff
        # Match against query with and without year
        for matcherpenalty, matcher in matchers:
            # Check titile both with and without the suffix
            for titlepenalty, mystr in titles:
                matcher.set_seq1(mystr)
                if matcher.real_quick_ratio() > mycutoff and \
                    matcher.quick_ratio() > mycutoff:
                    myratio = matcher.ratio()*matcherpenalty*titlepenalty
                    if myratio > mycutoff:
                        score = max(score, myratio)
                        mycutoff = score

        # If the movie scored at all, add it to the result list
        if score > 0:
            timer.check_expired()
            nratings = int(nratings)
            stored_title = akafor if akafor else title
            # Weight score by the number of ratings
            factor = (0.0205376)*nratings**(0.167496)+(0.9226)
            # Slightly discourage TV shows in favor of movies. This
            # makes it more difficult to match mini-series, but that's
            # just too bad.
            if stored_title[0] == '"':
                factor *= 0.95
            # Movies without a known year are extremely unlikely to be the
            # correct result.
            if not ryear:
                factor *= 0.90
            elif year: # and ryear
                ryear = int(ryear)
                if year == this_year and ryear == this_year:
                    # Extend the benefit of the doubt to prerelease movies
                    # (and others from this year) that have not had many
                    # votes on IMDb.
                    factor = max(factor, 1)
                # Adjust weight to disambiguate results by year-similarity
                factor *= exp(-(year-ryear)**2/160.0)
            score *= factor
            if stored_title not in scores or scores[stored_title] < score:
                scores[stored_title] = score
                if akafor:
                    akascores[stored_title] = title
                elif stored_title in akascores:
                    del akascores[stored_title]
    return scores, akascores

