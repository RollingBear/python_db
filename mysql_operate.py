# -*- coding: utf-8 -*-

#   2019/4/18 0018 上午 9:30     

__author__ = 'RollingBear'

import sys
import logging
from logging.handlers import RotatingFileHandler
import traceback
import pymysql

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    stream=sys.stdout)

logger = logging.getLogger()
Rthandler = RotatingFileHandler(sys.path[0] + '/mysql_connect_log.log', maxBytes=10, backupCount=5)
Rthandler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
Rthandler.setFormatter(formatter)
logger.addHandler(Rthandler)


class mysql():

    def __init__(self, ip='localhost', username='root', password='1120', db_name='test'):
        super().__init__()

        self.ip = ip
        self.username = username
        self.password = password
        self.db_name = db_name

        self.db = pymysql.connect(self.ip, self.username, self.password, self.db_name)

        self.cursor = self.db.cursor()

    def query(self, query_content='*', table_name='user'):
        sql = 'SELECT ' + query_content + ' FROM ' + table_name

        try:
            self.cursor.execute(sql)

            result = self.cursor.fetchall()
            for row in result:
                id = row[0]
                name = row[1]
                print('id=%s name=%s' % (id, name))
        except Exception as e:
            logger.error(traceback.format_exc())

    def insert(self, insert_content=None, table_name=None):

        if insert_content is None or table_name is None:
            logging.info('parameter can not be NULL')
            return
        else:
            sql = 'INSERT ' + insert_content + ' INTO ' + table_name

            try:
                self.cursor.execute(sql)
                self.db.commit()
            except Exception as e:
                logger.error(traceback.format_exc())
                self.db.rollback()

    def update(self, update_content=None, update_location=None, table_name=None):
        if update_content is None or update_location is None or table_name is None:
            logging.info('parameter can not be NULL')
            return
        else:
            sql = 'UPDATE ' + table_name + ' SET ' + update_content + ' WHERE ' + update_location

            try:
                self.cursor.execute(sql)
                self.db.commit()
            except Exception as e:
                logger.error(traceback.format_exc())
                self.db.rollback()

    def delete(self, delete_location=None, table_name=None):
        if delete_location is None or table_name is None:
            logging.info('parameter can not be NULL')
            return
        else:
            sql = 'DELETE FROM ' + table_name + ' WHERE ' + delete_location

            try:
                self.cursor.execute(sql)
                self.db.commit()
            except Exception as e:
                logger.error(traceback.format_exc())
                self.db.rollback()

    def close_db(self):
        self.cursor.close()
        self.db.close()


if __name__ == '__main__':
    db = mysql()

    db.query()

    db.close_db()
