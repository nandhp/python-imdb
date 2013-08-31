#!/usr/bin/env python

"""wsgi.py - Simple WSGI service to provide a JSON API for the database.
As a reminder, making IMDb data publically available through this API may
be a violation of the IMDb terms of use.
"""

import os
from cgi import parse_qs
import imdb
import json
from imdb.parsers import parse_name
from functools import wraps
import signal

SUPPORTED_ARGS = ('title', 'rating', 'plot', 'color_info', 'genres',
    'running_time', 'certificates', 'cast', 'directors', 'writers', 'aka')

imdbfile = 'imdb.zip'
if 'IMDB' in os.environ:
    imdbfile = os.environ['IMDB']
iface = imdb.IMDb(dbfile=imdbfile)

# From http://stackoverflow.com/a/2282656/462117
class TimeoutError(Exception):
    pass
def timeout(seconds=10):
    def decorator(func):
        def _handle_timeout(signum, frame):
            raise TimeoutError()
        def wrapper(*args, **kwargs):
            signal.signal(signal.SIGALRM, _handle_timeout)
            signal.alarm(seconds)
            try:
                result = func(*args, **kwargs)
            finally:
                signal.alarm(0)
            return result
        return wraps(func)(wrapper)
    return decorator

# Timeout searches after seven minutes. This allows excessively slow
# searches to be rejected entirely.
@timeout(7*60)
def run_search(query, year):
    return iface.search(query, year=year)

def application(environ, start_response):
    path = environ.get('PATH_INFO', '')
    params = parse_qs(environ.get('QUERY_STRING',''))
    if path == '/imdb':
        ctype = 'application/json'
        obj = {}
        if 'q' not in params or not params['q'] or not params['q'][0]:
            obj['_error'] = 'No query provided'
        else:
            try:
                year = int(params['y'][0])
            except:
                year = None
            try:
                results = run_search(params['q'][0], year)
            except TimeoutError:
                results = None
                # Return no results. The query is too complicated to
                # complete in a reasonable amount of time. It probably
                # would not produce any result, even with additional time.
                # Retrying the request will result in the same error.
                obj['_error'] = 'Timeout with no results'
            if '_error' in obj:
                pass
            elif not results:
                obj['_error'] = 'No results'
            else:
                obj['_score'] = results[0][1]
                for key in SUPPORTED_ARGS:
                    out = getattr(results[0][0], key)
                    # Convert names to "First Last"
                    if key in ('cast', 'directors', 'writers'):
                        for i, name in enumerate(out):
                            parsed_name = parse_name(name[0])
                            if parsed_name[1]:
                                out[i] = (u' '.join(parsed_name[1:3]),) + \
                                    name[1:]
                            else:
                                out[i] = (parsed_name[2],)+name[1:]
                    obj[key] = out
        response_body = json.dumps(obj)
    else:
        ctype = 'text/html'
        status = '404 Not Found'
        response_body = '''<!DOCTYPE html>
<html>
<head>
<meta charset=utf-8>
<title>404 Not Found</title>
</head>
<body>
<h1>404 Not Found</h1>
</body>
</html>
'''

    status = '200 OK'
    response_headers = [('Content-Type', ctype),
                        ('Content-Length', str(len(response_body)))]
    start_response(status, response_headers)
    return [response_body]

# For testing
if __name__ == '__main__':
    from wsgiref.simple_server import make_server
    make_server('localhost', 8051, application).serve_forever()

