# -*- coding: utf-8 -*-

#   2019/4/18 0018 下午 4:46     

__author__ = 'RollingBear'

import pymysql


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
            return False

        return data

    def query_dict(self, _sql_dict):
        if 'select' in _sql_dict.keys():
            sql = 'SELECT ' + _sql_dict['select'] + ' FROM ' + _sql_dict['from'] + self.where(_sql_dict['where'])
            print(sql)
            return self.query(sql)
        elif 'insert' in _sql_dict.keys():
            sql = "INSERT INTO " + _sql_dict['insert'] + self.quote(_sql_dict['domain_array'],
                                                                    type_filter=False) + " VALUES " + self.quote(
                _sql_dict['value_array'])
            print(sql)
            return self.query(sql)
        elif 'delete' in _sql_dict.keys():
            sql = 'DELETE FROM ' + _sql_dict['delete'] + self.where(_sql_dict['where'])
            print(sql)
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

    def format_query_dict_1(self):
        pass

    def format_query_dict_2(self):
        pass

    def format_delete_dict(self, _table_name, _condition):
        _sql = dict()

        _sql['delete'] = _table_name
        _sql['from'] = _condition

        return self.query_dict(_sql)

    def format_insert_dict(self):
        pass

    def sql_sentence(self, _sql):

        return self.query(_sql)
