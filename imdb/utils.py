"""utils - Shared utility functions."""

from subprocess import Popen, PIPE, STDOUT
from time import time, sleep

# Rate-limit configuration, to avoid using 100% CPU time for long searches.
# Set RATELIMIT = (x,y) to have search_index sleep y seconds (default 0.1)
# every x seconds (default 1/6.0). If RATELIMIT or x is false, never sleep.
RATELIMIT = (1/6.0, 0.1)

class TimerTimeout(Exception):
    """A Timer has exceeded its timeout."""
    pass

class Timer(object):
    """Provide elapsed-time statistics.
       Also support rate-limiting to avoid excessive CPU usage during data
       processing, in cases where background operation is desired, and
       timeout support for terminating long-running activities."""

    def __init__(self, message='Completed in %s seconds.', rl_min_dur=0,
                 indent=0, timeout=None, quiet=False):
        """Initialize a RateLimit object. min_dur is the initial duration
        to run without rate-limiting (i.e. to only rate-limit long-running
        tasks). If quiet=True, will not print elapsed time when used as a
        context manager (with statement)."""
        self.start = time()
        self.last = self.start
        self.message = ' '*indent + message
        self.quiet = quiet
        self.min_dur = rl_min_dur
        self.timeout = timeout
        assert(RATELIMIT and RATELIMIT[0])

    def step(self):
        """This method should be called regularly during processing.
        It will sleep whenever necessary to maintain rate-limiting."""
        # Rate-limit by using only small slices of CPU time.
        # It's like nice, but better and slower.
        now = time()
        if self.timeout and now-self.start > self.timeout:
            raise TimerTimeout
        if (not self.min_dur or now-self.start > self.min_dur) and \
                now-self.last > RATELIMIT[0]:
            sleep(RATELIMIT[1])
            self.last = now

    def check_expired(self):
        """Check the timer duration for timeout; but do not rate-limit."""
        now = time()
        if self.timeout and now-self.start > self.timeout:
            raise TimerTimeout

    def time(self):
        """Return the elapsed duration."""
        return time() - self.start

    def __str__(self):
        return "%8.4f" % self.time()

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_value, traceback):
        if self.quiet:
            pass
        elif not exc_type and not exc_value and not traceback:
            print self.message % self
        else:
            print 'Exception occurred after %s seconds.' % self

def open_compressed(filename=None, fileobj=None, mode='r',
                    compressor=('gzip', '--quiet')):
    """Read or write a compressed file using an external (de)compressor.
    One of filename and fileobj must be specified.

    filename -- Filename to open.
    fileobj -- File object to wrap (de)compressor around.
    mode -- Must contain 'r' or 'w'. Passed to open if filename is provided.
    compressor -- Command-line of (de)compression program, for example,
                  ['gzip', '--quiet'].
                  Specified compressor should behave like gzip(1)
                  (with respect to behavior with no arguments and with '-d').
    """
    if filename and not fileobj:
        fileobj = open(filename, mode)
    elif filename or not fileobj:
        raise ValueError("Must specify exactly one of filename or fileobj")
    if 'r' in mode:
        return Popen(tuple(compressor) + ('-d',),
                     stdin=fileobj, stdout=PIPE, stderr=STDOUT).stdout
    elif 'w' in mode:
        return Popen(tuple(compressor), stdin=PIPE, stdout=fileobj).stdin
    else:
        raise ValueError("Must specify read or write")

