# -*- coding: utf-8 -*-

#   2019/4/18 0018 下午 4:46     

__author__ = 'RollingBear'

import sys
import pymysql
import logging
from logging.handlers import RotatingFileHandler
import traceback

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s  %(threadName)s  %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    stream=sys.stdout)

Rthandler = RotatingFileHandler('sql_log.log', maxBytes=10 * 1024 * 1024, backupCount=10)
Rthandler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s  %(threadName)s  %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
Rthandler.setFormatter(formatter)
logger = logging.getLogger('sql_log')
logger.addHandler(Rthandler)


class mysql():
    """
        query dictionary:
            sql = {
                    'select'        : 'select list name',
                    'from'          : 'table name',
                    'where'         : {
                                            'list name': 'value',
                                            'list name': 'value',
                                            ...
                                        }
                    }
            sql = {
                    'select'        : 'select list name',
                    'from'          : 'table name',
                    'where'         : 'list name > value and list name < value'
                    }
        delete dictionary:
            sql = {
                    'delete'        : 'table name',
                    'where'         : 'list name > value and list name <value'
                    }
        insert dictionary:
            sql = {
                    'insert'        : 'table name',
                    'domain_array'  : [ 'list name', 'list name', ...]
                    'value_array'   : [ 'value', 'value', ...]

        'where' can be None
    """

    __db = None

    __config = {
        'host': 'localhost',
        'port': 3306,
        'username': 'root',
        'password': '1120',
        'database': 'test',
        'charset': 'utf8'
    }

    def __init__(self):
        self.__connect()

    def __del__(self):
        if self.__db is not None:
            self.__db.close()

    def __connect(self):
        if self.__db is None:
            self.__db = pymysql.connect(
                host=self.__config['host'],
                port=self.__config['port'],
                user=self.__config['username'],
                passwd=self.__config['password'],
                db=self.__config['database'],
                charset=self.__config['charset']
            )

        return self.__db

    def query(self, _sql):
        cursor = self.__connect().cursor()

        try:
            cursor.execute(_sql)
            data = cursor.fetchall()

            self.__connect().commit()
        except:
            self.__connect().rollback()
            logger.error(traceback.format_exc())
            return False

        return data

    def query_dict(self, _sql_dict):
        if 'select' in _sql_dict.keys():
            if _sql_dict['where'] is '' or _sql_dict['where'] is None:
                sql = 'SELECT ' + _sql_dict['select'] + ' FROM ' + _sql_dict['from']
            else:
                sql = 'SELECT ' + _sql_dict['select'] + ' FROM ' + _sql_dict['from'] + self.where(_sql_dict['where'])
            logger.info('SQL statement to be executed: {}'.format(sql))
            return self.query(sql)
        elif 'insert' in _sql_dict.keys():
            sql = "INSERT INTO " + _sql_dict['insert'] + self.quote(_sql_dict['domain_array'],
                                                                    type_filter=False) + " VALUES " + self.quote(
                _sql_dict['value_array'])
            logger.info('SQL statement to be executed: {}'.format(sql))
            return self.query(sql)
        elif 'delete' in _sql_dict.keys():
            if _sql_dict['where'] is '' or _sql_dict['where'] is None:
                sql = 'DELETE FROM ' + _sql_dict['delete']
            else:
                sql = 'DELETE FROM ' + _sql_dict['delete'] + self.where(_sql_dict['where'])
            logger.info('SQL statement to be executed: {}'.format(sql))
            return self.query(sql)

    @staticmethod
    def where(_sql):
        if isinstance(_sql, dict) is False:
            return ' WHERE ' + str(_sql)
        elif isinstance(_sql, dict):
            _sql_dict = _sql
            s = ' WHERE '
            index = 0
            for domain in _sql_dict:
                if index == 0:
                    s += domain + '=' + str(_sql_dict[domain]) + ' '
                    index += 1
                else:
                    s += ' AND ' + domain + '=' + str(_sql_dict[domain]) + ' '

            return s

    @staticmethod
    def quote(_data_array, type_filter=True):
        s = '('
        index = 0
        if type_filter:
            for domain in _data_array:
                if index == 0:
                    if isinstance(domain, int):
                        s += str(domain)
                    elif isinstance(domain, str):
                        s += '"' + domain + '"'
                    index += 1
                else:
                    if isinstance(domain, int):
                        s += ', ' + str(domain)
                    elif isinstance(domain, str):
                        s += ', ' + '"' + domain + '"'
        else:
            for domain in _data_array:
                if index == 0:
                    s += str(domain)
                    index += 1
                else:
                    s += ', ' + domain

        return s + ')'

    def format_query_dict(self, _list_name, _table_name, _condition):
        _sql = dict()

        _sql['select'] = _list_name
        _sql['from'] = _table_name
        _sql['where'] = _condition

        return self.query_dict(_sql)

    def format_delete_dict(self, _table_name, _condition):
        _sql = dict()

        _sql['delete'] = _table_name
        _sql['from'] = _condition

        return self.query_dict(_sql)

    def format_insert_dict(self, _table_name, _domain_array, _value_array):
        _sql = dict()

        _sql['insert'] = _table_name
        _sql['domain_array'] = _domain_array
        _sql['value_array'] = _value_array

        return self.query_dict(_sql)

    def sql_sentence(self, _sql):

        return self.query(_sql)