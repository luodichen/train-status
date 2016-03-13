# -*- encoding: utf-8 -*-

'''
Created on Mar 13, 2016

@author: luodichen
'''

import os
import re
import sys
import pyquery
import urllib2
import sqlite3
import datetime

reload(sys)
sys.setdefaultencoding('utf-8')

class DataModal(object):
    def __init__(self, db_path):
        self.db_path = db_path

    def get_conn(self):
        return sqlite3.connect(self.db_path)

    def new_table(self):
        conn = self.get_conn()
        cur = conn.cursor()

        table_name = 'train_num_' + datetime.datetime.now().strftime('%Y%m%d%H%M%S')

        cur.execute('DROP TABLE IF EXISTS ' + table_name)
        sql_create = '''CREATE TABLE `%s` (
            `id` INTEGER PRIMARY KEY AUTOINCREMENT,
            `original_train_num` TEXT NOT NULL,
            `train_num` TEXT NOT NULL,
            `train_num2` TEXT DEFAULT NULL,
            `station_name` TEXT NOT NULL,
            `arrival_time` INTEGER NOT NULL,
            `departure_time` INTEGER NOT NULL,
            `remain_time` INTEGER NOT NULL
        )''' % (table_name, )

        cur.execute(sql_create)
        cur.execute('CREATE INDEX %s_index0 ON %s (arrival_time)' % (table_name, table_name, ))
        cur.execute('CREATE INDEX %s_index1 ON %s (departure_time)' % (table_name, table_name, ))
        cur.execute('CREATE INDEX %s_index2 ON %s (remain_time)' % (table_name, table_name, ))

        conn.commit()
        cur.close()
        conn.close()

        return table_name

    def train_exists(self, table, train_number):
        conn = self.get_conn()
        cur = conn.cursor()

        cur.execute('''SELECT COUNT(*)
                            FROM `%s`
                            WHERE `original_train_num` = ?''' % (table, ),
                          (train_number, ))

        ret = cur.fetchone()[0] > 0
        cur.close()
        conn.close()

        return ret

    def insert_train_info(self, table, info):
        conn = self.get_conn()
        cursor = conn.cursor()
        try:
            for item in info:
                cursor.execute('''
                    INSERT INTO %s (`original_train_num`,
                                    `train_num`,
                                    `train_num2`,
                                    `station_name`,
                                    `arrival_time`,
                                    `departure_time`,
                                    `remain_time`)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                ''' % (table, ), item)
            conn.commit()
        finally:
            cursor.close()
            conn.close()

def time_to_seconds(str_time):
    ret = 0
    for node in str_time.split(':'):
        ret = ret * 60 + int(node)

    return ret

def get_train_info(url):
    pq = pyquery.PyQuery(urllib2.urlopen(url, timeout=10).read())
    exp = r'^.*?(([A-Z]?\d{1,5})(?:/([A-Z]?\d{1,5}))?).*?$'
    match = re.compile(exp).match(pq("h1").text())

    original_train_num, train_num, train_num2 = match.group(1), match.group(2), match.group(3)

    rows = pq("#stationInfo tr:gt(0)")
    parser = [
        lambda x: None,
        lambda x: x,
        lambda x: time_to_seconds(x),
        lambda x: time_to_seconds(x),
        lambda x: time_to_seconds(x),
        lambda x: None,
        lambda x: None
    ]

    ret = []
    for i in xrange(len(rows)):
        item = rows.eq(i)("td")
        data = [original_train_num, train_num, train_num2] + [parser[i](item.eq(i).text()) for i in xrange(len(item))][1:5]
        ret.append(tuple(data))

    return ret

def get_station_train_list(url):
    base_url = 'http://qq.ip138.com'
    pq = pyquery.PyQuery(urllib2.urlopen(url, timeout=10).read())
    rows = pq("#checilist table tr:gt(0) a")

    ret = []
    for i in xrange(len(rows)):
        ret.append(tuple([rows.eq(i).text(), base_url + rows.eq(i).attr('href'), ]))

    return ret

def get_province_stations_list(url):
    base_url = 'http://qq.ip138.com'
    pq = pyquery.PyQuery(urllib2.urlopen(url, timeout=10).read())
    rows = pq("table:eq(3) td a")

    ret = []
    for i in xrange(len(rows)):
        ret.append(tuple([rows.eq(i).text(), base_url + rows.eq(i).attr('href'), ]))

    return ret

def get_province_list():
    url = 'http://qq.ip138.com/train/'
    base_url = 'http://qq.ip138.com'
    pq = pyquery.PyQuery(urllib2.urlopen(url, timeout=10).read())
    rows = pq("table:eq(4) td a")

    ret = []
    for i in xrange(len(rows)):
        ret.append(tuple([rows.eq(i).text(), base_url + rows.eq(i).attr('href'), ]))

    return ret

def main(argv):
    error_count = 0
    database = '.' + os.path.sep + 'data.sqlite3'

    if len(argv) > 1:
        database = argv[1]

    data_modal = DataModal(database)
    table = data_modal.new_table()

    for province in get_province_list():
        print province[0]
        province_retry = 0
        while province_retry < 3:
            try:
                station_list = get_province_stations_list(province[1])
            except Exception, e:
                print e
                print 'retry...'
                province_retry += 1
                continue
            break

        for station in station_list:
            print station[0]
            station_retry = 0
            while station_retry < 3:
                try:
                    train_list = get_station_train_list(station[1])
                except Exception, e:
                    print e
                    print 'retry...'
                    station_retry += 1
                    continue
                break

            for train in train_list:
                print train[0]
                if data_modal.train_exists(table, train[0]):
                    print 'exists, skip.'
                    continue

                train_retry = 0
                while train_retry < 3:
                    try:
                        train_info = get_train_info(train[1])
                    except Exception, e:
                        print e
                        print 'retry...'
                        train_retry += 1
                        continue
                    break

                try:
                    data_modal.insert_train_info(table, train_info)
                    for arrival in train_info:
                        print '%s at %02d:%02d' % (arrival[3], arrival[4] / 60, arrival[4] % 60)
                except Exception, e:
                    print e
                    error_count += 1
                    continue

    print '%d error(s)' % (error_count, )

if __name__ == '__main__':
    main(sys.argv)
