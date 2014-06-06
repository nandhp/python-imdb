"""chunkedfile - Chunked storage of compressed data"""

from collections import namedtuple
from zipfile import ZipFile, BadZipfile, ZIP_DEFLATED
from gzip import GzipFile
from base64 import urlsafe_b64encode, urlsafe_b64decode

ChunkInfo = namedtuple('ChunkInfo', ('name', 'pos', 'bookmark'))

class ChunkedFile(object):
    """Compressed file writer/reader that stores data in chunks in a zip file.
    Transparently supports reading gzip files.
    """
    def __init__(self, filename, subfile='', mode='r', chunksize=131072,
                 autoflush=True):
        """Create a ChunkedFile object with given filename, I/O mode (r,w,a),
        and preferred chunk size. If you wish to manually control the chunk
        boundaries using bookmark() or flush(), set autoflush=False."""
        if mode not in 'rwa':
            raise ValueError('Mode must be r or w or a')
        try:
            self.zip = ZipFile(filename, mode, ZIP_DEFLATED)
            self._is_gzip = False
        except BadZipfile:
            assert(mode == 'r')
            # Transparent reading of gzip files
            # (relatively fast, pure-python, some limitations)
            self.zip = GzipFile(filename, mode)
            self._is_gzip = True
        self.prefix = '%s/c.' % str(subfile) if subfile else 'c.'
        self.mode = mode
        self.chunksize = chunksize
        self.autoflush = autoflush

        # List of available chunks
        if not self._is_gzip:
            self.chunks = self._chunks()

        # Determine current position
        if mode == 'r':
            self.eof = False
            self.chunkidx = -1
        else:
            self.eof = True
            self.chunkidx = len(self.chunks)-1
        if self.chunkidx >= 0:
            info = self.zip.getinfo(self.chunks[self.chunkidx].name)
            self.pos = self.chunks[self.chunkidx].pos + info.file_size
        else:
            self.pos = 0

        # Buffers
        self.nextbuf = []
        self.readbuf = ''
        self.writebuf = ''
        self._last_bookmark = None

    def _chunks(self):
        """Return a list of ChunkInfos, one for each chunk in the file."""
        offset = len(self.prefix)
        chunks = []
        for name in self.zip.namelist():
            # Check multifiles
            if not name[0:].startswith(self.prefix):
                continue
            nameinfo = name[offset:].split(',')
            pos = int(nameinfo[0], 16)
            bookmark = None
            if len(nameinfo) > 1:
                bookmark = urlsafe_b64decode(nameinfo[1])
            chunks.append(ChunkInfo(name=name,
                                    pos=pos,
                                    bookmark=bookmark))
        return sorted(chunks, key=lambda chunk: chunk.pos)

    def _next_chunk(self):
        """Read the next chunk into the read buffer."""
        if self._is_gzip:
            chunk = self.zip.read(self.chunksize)
            if not chunk:
                self.eof = True
                raise EOFError
            else:
                self.readbuf += chunk
            return
        self.chunkidx += 1
        if self.chunkidx >= len(self.chunks):
            self.eof = True
            raise EOFError
        else:
            self.readbuf += self.zip.read(self.chunks[self.chunkidx].name)

    def _flush(self, auto=True, bookmark=None):
        """Flush complete chunks from the write buffer. An incomplete chunk
        may be created (and the write buffer completely emptied) if
        auto=False"""
        if auto and not self.autoflush:
            return
        while self.writebuf and \
                (len(self.writebuf) >= self.chunksize or not auto):
            self.chunkidx += 1
            assert(self.chunkidx == len(self.chunks))
            chunkpos = self.pos-len(self.writebuf)
            chunkname = '%s%08x' % (self.prefix, chunkpos)
            chunkbookmark = None
            if bookmark and len(self.writebuf) <= self.chunksize:
                chunkname += ','+urlsafe_b64encode(bookmark)
                chunkbookmark = bookmark
            self.zip.writestr(chunkname, self.writebuf[:self.chunksize])
            self.writebuf = self.writebuf[self.chunksize:]
            self.chunks.append(ChunkInfo(name=chunkname,
                                         pos=chunkpos,
                                         bookmark=chunkbookmark))

    def close(self):
        """Close the file. Must be called to avoid data loss."""
        self.flush()
        self.zip.close()

    def flush(self):
        """Flush all output to the file."""
        self._flush(auto=False)

    def bookmark(self, bookmark):
        """Possibly flush the file, writing a bookmark if doing so."""
        assert(not self._last_bookmark or bookmark >= self._last_bookmark)
        self._last_bookmark = bookmark
        if len(self.writebuf) >= (self.chunksize-self.chunksize/8):
            # Use 7/8 of a chunksize to avoid creating too many tiny overflow
            # chunks.
            self._flush(auto=False, bookmark=bookmark)

    def write(self, data):
        """Write data to be stored in the file."""
        assert(not self._is_gzip)
        self.writebuf += data
        self.pos += len(data)
        self._flush(auto=True)

    def read(self, size=-1):
        """Read data from the file."""
        try:
            while size < 0 or len(self.readbuf) < size:
                self._next_chunk()
        except EOFError:
            pass
        if size > 0:
            ret = self.readbuf[:size]
            self.readbuf = self.readbuf[size:]
        elif size < 0:
            ret = self.readbuf
            self.readbuf = ''
        elif size == 0:
            ret = ''
        self.pos += len(ret)
        return ret

    def next(self):
        """Return the next line from the file or raise StopIteration."""
        if self.nextbuf:
            self.pos += len(self.nextbuf[0])
            return self.nextbuf.pop(0)
        if self.eof and not self.readbuf:
            raise StopIteration
        # Find next line ending
        try:
            while '\n' not in self.readbuf:
                self._next_chunk()
        except EOFError:
            if '\n' not in self.readbuf:
                if self.readbuf:
                    return self.read(-1)
                else:
                    raise StopIteration

        # Split lines into separate buffer
        self.nextbuf = self.readbuf.splitlines(True)
        if self.readbuf[-1] != '\n':
            self.readbuf = self.nextbuf.pop()
        else:
            self.readbuf = ''
        return self.next()

    def seek(self, offset, whence=0):
        """Seek to a given byte position in the file. Currently limited to
        files opened for mode=r and whence current location or beginning of
        the file."""
        # Only simple writing is supported
        assert(self.mode == 'r')
        if whence == 0:
            pass
        elif whence == 1:
            offset = self.pos+offset
        elif whence == 2:
            raise NotImplementedError
        else:
            raise ValueError
        if self._is_gzip:
            assert(offset >= self.pos)
        else:
            # Find the correct chunk
            self.flush()
            self.nextbuf = []
            self.readbuf = ''
            self.chunkidx = -1
            self.pos = 0
            for idx, data in enumerate(self.chunks):
                if data.pos <= offset:
                    self.chunkidx = idx-1
                    self.pos = data.pos
        delta = offset-self.pos
        assert(delta >= 0)
        self.read(delta)
        assert(delta <= self.chunksize or self.eof or self._is_gzip)
        assert(self.pos == offset)

    def find_bookmark(self, bookmark, give_range=False):
        """Determine an appropriate seek position near bookmark."""
        pos = 0
        for chunk in self.chunks:
            if chunk.bookmark and chunk.bookmark < bookmark:
                pos = chunk.pos
        if give_range:
            ret_next = 0
            for chunk in self.chunks:
                if ret_next == 1:
                    assert(chunk.pos > pos)
                    return pos, chunk.pos
                elif chunk.bookmark and chunk.bookmark > bookmark:
                    ret_next = 1
            return pos, None
        else:
            return pos

    def tell(self):
        """Return the current byte position in the file."""
        return self.pos

    # def __enter__(...): return self
    # def __exit__(...): self.close()

    def __iter__(self):
        return self

def _main(argv):
    """Simple program to read/write ChunkedFiles."""
    parser = ArgumentParser()
    parser.add_argument('--read',
                        action='store_const', const='r', dest='read',
                        help='Read data from file')
    parser.add_argument('--seek', nargs=1, type=int,
                        help='Seek position before reading')
    parser.add_argument('--write',
                        action='store_const', const='w', dest='mode',
                        help='Write data to file')
    parser.add_argument('--append',
                        action='store_const', const='a', dest='mode',
                        help='Append data to file')
    parser.add_argument('file', nargs=1,
                        help='Container to read/write')
    parser.add_argument('subfile', nargs='?',
                        help='Subfile to read/write')
    args = parser.parse_args(argv[1:])

    def move_data(readfh, writefh):
        """Move data from readfh to writefh until EOF."""
        while True:
            buf = readfh.read(1024*1024)
            if not buf:
                break
            writefh.write(buf)

    if args.mode and args.mode in 'aw':
        cfh = ChunkedFile(args.file[0], subfile=args.subfile, mode=args.mode)
        move_data(sys.stdin, cfh)
        cfh.close()
    if args.read or not args.mode:
        cfh = ChunkedFile(args.file[0], subfile=args.subfile, mode='r')
        if args.seek:
            cfh.seek(args.seek[0])
        move_data(cfh, sys.stdout)
        cfh.close()

if __name__ == '__main__':
    import sys
    from argparse import ArgumentParser
    _main(sys.argv)

