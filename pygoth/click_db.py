
import time
import gzip
import urllib
from datetime import datetime

import msgpack
import leveldb



class ClickLevelDB(object):
    def __init__(self, db_path):
        self._db = leveldb.LevelDB(db_path)

    def write_clicks(self, data):
        """Writes click data into the db.

        data is an iterable of (ts, uid, path, E) tuples. We write a
        (ts, tspec, uid, path, E) tuple into the database.

        """

        count = 0
        for ts, uid, path, E in data:
            if count == 0:
                batch = leveldb.WriteBatch()

            key = '%010d:%s' % (ts, uid)
            data = [ts, self._to_psql_timestamp(ts), uid, path, E]
            batch.Put(key, msgpack.packb(data))

            count += 1

            if count == 100000:
                self._db.Write(batch, sync=True)
                count = 0
                batch = None

    def iter_clicks(self, st_ts=None, end_ts=None):
        """Yields (ts, tspec, uid, path, et) tuples that fall in [st_ts, end_ts).

        ts - unix timestamp
        tspec - postgres formatted timestamp string
        uid - unique user id
        path - page path
        et - engaged time
        """

        if st_ts is not None:
            start_key = '%010d:' % st_ts
        else:
            start_key = None

        if end_ts is not None:
            end_key = '%010d:' % end_ts
        else:
            end_key = None

        for key, value in self._db.RangeIter(key_from=start_key, key_to=end_key):
            if end_key and key >= end_key:
                return

            data = msgpack.unpackb(value)
            yield data

    def _to_psql_timestamp(self, unix_ts):
        """Converts a Unix epoch timestamp to a postgresql formatted
        timestamp.
        
        """

        dt = datetime.utcfromtimestamp(unix_ts)
        return dt.strftime("%Y-%m-%d %H:%M:%S")


def generate_fake_data(num):
    for i in xrange(num):
        yield (i, i, '/some-path', 10)

def timing_test():
    db = ClickDB('/home/vagrant/temp/timing_test.db', 'w')

    num_items = 2 * 1000000

    st_time = time.time()
    db.write_clicks(generate_fake_data(num_items))
    elapsed = time.time() - st_time

    print "wrote %d items in %f sec, %f/sec" % \
        (num_items, elapsed, num_items/elapsed)

def timing_test2():
    db = leveldb.LevelDB('/home/vagrant/temp/timing.leveldb')
    num_items = 5 * 1000000

    st_time = time.time()
    batch = leveldb.WriteBatch()
    for d in generate_fake_data(num_items):
        batch.Put(str(d[0]), msgpack.packb(d))
    db.Write(batch, sync=True)
    elapsed = time.time() - st_time

    print "wrote %d items in %f sec, %f/sec" % \
        (num_items, elapsed, num_items/elapsed)


def load_clicks(gz_fname):
    """Loads up click events from compressed csv file."""

    with gzip.open(gz_fname, 'r') as finp:
        for line in finp:
            uid, ts, path, E = line.split()
            # just some simple cleanup on the path
            path = urllib.unquote(path).split('?')[0]

            yield (int(ts), uid, path, E)


if __name__ == '__main__':
    db = ClickLevelDB('/home/vagrant/temp/click.leveldb')

    # write a bunch of fake data in to see what pure write throughput is
    # timing_test()
    # timing_test2()

    # write_clicks with load_clicks actually loads up the gawker data
    db.write_clicks(load_clicks('/home/vagrant/gawker.csv.gz'))
