This package implements a Python interface to [IMDb plain text data files][1].

[1]: http://www.imdb.com/interfaces

At this time, the API should not be considered stable.

Note that IMDb uses iso-8859-1 encoding (in data files and URLs);
this package uses Unicode in most places.

`python-imdb` supports the following data files (to greater or lesser degree)

* movies
* aka-titles
* ratings
* plot
* genres
* running-times
* color-info
* certificates
* directors
* writers
* actors
* actresses

Download these files into `/some/directory` and then run `python imdb --rebuild-db /some/directory` to convert the data files (necessary to support seeking within the data files) and build a search index.
This will result in files `imdb.zip` and `imdb.zip.idx`.

For search, `movies.list` is required and `aka-titles.list` and `ratings.list` are strongly recommended. However, each file is optional, with associated data and/or features simply being unavailable.

The module includes examples of a simple program (`example.py`)
and a WSGI-based JSON API endpoint (`wsgi.py`).
