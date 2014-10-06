
import gzip
from datetime import datetime
from collections import defaultdict

from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres



def get_rows(path):
    """Iterates through compressed access log and yields dicts that
    contain ip, time, and error code info.

    """

    with gzip.open(path) as finp:
        for line in finp:
            try:
                parts = line.split()

                # clean time, chop off timezone info
                time = parts[4].replace('[', '')

                # clean path and truncate for demo purposes
                path = parts[7].split('?')[0].rstrip('/')
                path = path[:60]
                if not path:
                    continue

                yield {
                    'path': path,
                    'ip': parts[0],
                    'time': _log_time_to_psql(time),
                    'error': int(parts[-2]),
                    'elapsed': float(parts[-1]),
                }
            except Exception:
                # Some simple error handling because log file ain't
                # clean. Of *course* I would never do this in
                # production...
                pass

def _log_time_to_psql(timestr):
    """Converts time format from '01/Jul/2014:06:31:24' to '2014-07-01 06:31:24'."""

    dt = datetime.strptime(timestr, '%d/%b/%Y:%H:%M:%S')
    return dt.strftime("%Y-%m-%d %H:%M:%S")


class AccessLogFDW(ForeignDataWrapper):
    """
    CREATE SERVER access_log_srv foreign data wrapper multicorn options ( wrapper 'pygoth.access_log_fdw.AccessLogFDW' );

    create foreign table access_log (path text, ip text, time timestamp without time zone, error int, elapsed float) server access_log_srv;
    """

    def __init__(self, options, columns):
        super(AccessLogFDW, self).__init__(options, columns)
        self._access_log_path = '/home/vagrant/access_log.gz'

        log_to_postgres("caching row data")
        self._rows = list(get_rows(self._access_log_path))
        self._rows_by_error = defaultdict(list)
        for row in self._rows:
            self._rows_by_error[row['error']].append(row)

    def execute(self, quals, columns):
        # for row in get_rows(self._access_log_path):
        #     yield row

        results = self._rows
        for q in quals:
            if q.field_name == 'error' and q.operator == '=':
                log_to_postgres("filtering query on error to %d" % q.value)
                results = self._rows_by_error[q.value]

        return results


if __name__ == '__main__':
    for row in get_rows('/home/vagrant/access_log.gz'):
        print row
