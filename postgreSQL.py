# -*- coding: utf-8 -*-

#   2019/6/27 0027 上午 9:59     

__author__ = 'RollingBear'

import psycopg2


class postgre():

    def __init__(self, host, port, db, username, password):
        self.host = host
        self.port = port
        self.db = db
        self.username = username
        self.password = password

        self._conn = self._connect()
        self._cursor = self._conn.cursor()

    def try_except(self):
        def wrapper(*args, **kwargs):
            try:
                self(*args, **kwargs)
            except Exception as e:
                print('Get Error: ', e)

        return wrapper

    @try_except
    def _connect(self):

        connect_result = psycopg2.connect(database=self.db, user=self.username, password=self.password, host=self.host,
                                          port=self.port)
        return connect_result

    @try_except
    def select(self, sql):
        self.execute(sql)
        return self.cursor.fetchall()

    def insert(self, sql):
        self.common(sql)

    def update(self, sql):
        self.common(sql)

    def delete(self, sql):
        self.common(sql)

    def close(self):
        self._cursor.close()
        self._conn.close()

    def insert_and_get_field(self, sql, field):
        try:
            self.cursor.execute(sql + 'RETURNING' + field)
        except Exception as e:
            print(e)
            self.conn.rollback()
            self.cursor.execute(sql + 'RETURNING' + field)

        self.conn.commit()

        return self.cursor.fetchall()

    def common(self, sql):
        try:
            self.cursor.execute(sql)
        except Exception as e:
            print(e)
            self.conn.rollback()
            self.cursor.execute(sql)

        self.conn.commit()

    def __del__(self):
        self.close()
