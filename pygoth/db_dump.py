import sys

import leveldb

import click_db


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print "usage: db_dump DB"
        sys.exit(1)

    db_path = sys.argv[1]
    db = click_db.ClickLevelDB(db_path)
    for data in db.iter_clicks():
        print data
