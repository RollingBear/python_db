# -*- coding: utf-8 -*-

#   2019/4/17 0017 下午 5:22     

__author__ = 'RollingBear'

import pymysql

db = pymysql.connect('localhost', 'root', '1120', 'test')

cursor = db.cursor()

sql = 'INSERT INTO user(name)' \
      'VALUES ("Mac")'

try:
    cursor.execute(sql)
    db.commit()
except:
    db.rollback()

db.close()
