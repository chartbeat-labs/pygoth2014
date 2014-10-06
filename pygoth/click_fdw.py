
from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres

import click_db


class ClickFDW(ForeignDataWrapper):
    """
    FDW that loads in click data from an leveldb database.

    CREATE SERVER click_srv foreign data wrapper multicorn options (wrapper 'pygoth.click_fdw.ClickFDW' );

    CREATE FOREIGN TABLE clicks (time timestamp without time zone, uid text, path text, et int) server click_srv;

    """

    def __init__(self, options, columns):
        super(ClickFDW, self).__init__(options, columns)
        self._db = click_db.ClickLevelDB('/home/vagrant/temp/click.leveldb')

    def execute(self, quals, cols):
        start_ts = self._get_min_time(quals)
        end_ts = self._get_max_time(quals)
        for ts, tspec, uid, path, et in self._db.iter_clicks(start_ts, end_ts):
            yield {
                'time': tspec,
                'uid': uid,
                'path': path,
                'et': et,
            }

    def _get_min_time(self, quals):
        """Extracts a 'time >= X' or 'time > X' qualifier."""

        for q in quals:
            if q.field_name == 'time' and q.operator in ['>', '>=']:
                return int(q.value.strftime('%s'))

        return None

    def _get_max_time(self, quals):
        """Extracts a 'time <= X' or 'time < X' qualifier."""

        for q in quals:
            if q.field_name == 'time' and q.operator in ['<', '<=']:
                return int(q.value.strftime('%s'))

        return None

if __name__ == '__main__':
    pass
