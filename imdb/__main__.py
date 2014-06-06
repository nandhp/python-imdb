"""__main__ - Sample program to search the IMDb from the command line."""

from argparse import ArgumentParser
import sys, os.path
sys.path.append(os.path.dirname(__file__))
from __init__ import IMDb, IMDbTitle

SUPPORTED_ARGS = 'rating', 'plot', 'color_info', 'genres', 'running_time', \
    'certificates', 'cast', 'directors', 'writers'

def _main(argv):
    """Command-line interface."""
    parser = ArgumentParser()
    parser.add_argument('--quiet', action='store_const', default=False,
                        const=True,
                        help='Do not display debugging messages')
    parser.add_argument('--dbfile', nargs=1, default='imdb.zip',
                        help='Database file')
    parser.add_argument('--rebuild-db', nargs=1, metavar='DIR',
                        help='Rebuild the database file from IMDb data files')
    parser.add_argument('--search', nargs='*',
                        help='Search the database')
    for argname in SUPPORTED_ARGS:
        parser.add_argument('--' + argname.replace('_', '-'), nargs='*',
                            metavar='TITLE',
                            help='Display ' + argname.replace('_', ' '))
    parser.add_argument('--all', nargs='*', metavar='TITLE',
                        help='Display all information')

    if len(argv) == 0:
        parser.error('nothing to do.')
    args = parser.parse_args(argv)

    iface = IMDb(dbfile=args.dbfile,    # Database filename
                 debug=not args.quiet)

    if args.rebuild_db:
        iface.rebuild_index(args.rebuild_db[0])

    titles = []
    if args.search:
        queries = []
        check_for_year = False
        for query in args.search:
            if check_for_year:
                try:
                    iquery = int(query)
                except ValueError:
                    pass
                else:
                    if iquery > 1850 and iquery < 2100:
                        queries[-1][1] = iquery
                        check_for_year = False
                        continue
            queries.append([query, None])
            check_for_year = True

        print "Search results:"
        for query, year in queries:
            results = iface.search(query, year=year)
            for title, score in results:
                print "  %s (%s)" % (title, str(score))
            if len(results) > 0:
                titles.append(results[0][0])
        print ''

    for argname in SUPPORTED_ARGS:
        argval = args.all if args.all is not None else getattr(args, argname)
        if argval is None:
            continue
        my_titles = [IMDbTitle(i) for i in argval]
        if not my_titles:
            my_titles = titles
        # Populate the requested information
        populator = getattr(iface, 'populate_' + argname)
        populator(my_titles)
        # Print the information
        for title in my_titles:
            print u"%s for %s:" % (argname.title().replace('_', ' '), title)
            val = getattr(title, argname)
            if val is None:
                val = u'(None)'
            elif argname == 'rating':
                val = u"%s/10, %7s votes" % (val.score, val.nratings)
            elif argname == 'plot':
                val = val.summary
                # if val.byline: val += u" (by %s)" % (val.byline,)
            elif argname == 'genres':
                val = u", ".join(val)
            elif argname == 'running_time':
                val = u'%3d minutes' % val
            elif argname == 'cast' or argname == 'writers' or \
                    argname == 'directors':
                val = u"\n  ".join(str(i) for i in val)
            print u"  %s" % (val,)
        print ''

if __name__ == '__main__':
    _main([arg.decode('utf-8') for arg in sys.argv[1:]])
    #print search('texas chainsaw massacre', year=1974)
    #print search('war games', year=1983)
    #print search('dark city - 1998')
    #print search('Evangelion 3.0 Q: You Can (Not) Redo (2012)')
    #print search('Evangelion Shin Gekijoban: Kyu', year=2012)
    #print search('Up')
    #print search('R.E.M.')
    #print search('secret', year=2007)
    #print search('secret (2007)')
    #print search('die hard')
    #build_index()
    #print IMDbRatingsParser().search((u'Not Existing', u'Up (2009)',
    #                                  u'Live Free or Die Hard (2007)',
    #                                  u'zNotExist'))
    #for i in IMDbAkaParser().search():
    #    pass
    #print i

