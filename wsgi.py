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

SUPPORTED_ARGS = ('title', 'rating', 'plot', 'color_info', 'genres',
    'running_time', 'certificates', 'cast', 'directors', 'writers', 'aka')

imdbfile = 'imdb.zip'
if 'IMDB' in os.environ:
    imdbfile = os.environ['IMDB']
iface = imdb.IMDb(dbfile=imdbfile)

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
            results = iface.search(params['q'][0], year=year)
            if not results:
                obj['_error'] = 'No results'
            else:
                obj['_score'] = results[0][1]
                for key in SUPPORTED_ARGS:
                    out = getattr(results[0][0], key)
                    # Convert names to "First Last"
                    if key in ('cast', 'directors', 'writers'):
                        for i, nm in enumerate(out):
                            #out[i] = u' '.join(parse_name(nm[0])[1:3])
                            out[i] = (u' '.join(parse_name(nm[0])[1:3]),)+nm[1:]
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
    httpd = make_server('localhost', 8051, application)
    # httpd.handle_request() # Serve a single request
    httpd.serve_forever()

