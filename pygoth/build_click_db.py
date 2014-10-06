
from click_db import *


if __name__ == '__main__':
    timing_test2()

    # db = ClickLevelDB('/home/vagrant/temp/click.leveldb', 'w')
    # db.write_clicks(load_clicks('/home/vagrant/gawker.csv.gz'))

    # db = ClickDB('/home/vagrant/temp/click.db', 'r')
    # count = 0
    # for d in db.iter_clicks():
    #     count += 1
    # print count

