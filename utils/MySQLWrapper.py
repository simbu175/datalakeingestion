import configparser
# import sys
import time
import traceback

# os.environ['PYTHON_EGG_CACHE'] = "/public/gdp/scripts/python/"
import MySQLdb

# sys.path.append(f"data/codes/Lake_Ingestion_Codes/config")
# sys.path.append(f"C:\\Projects\\PyCharmRepo\\ArchivalSolution\\Lake_Ingestion_Codes\\")
from config import basic_config as conf


class MySQLWrapper:
    def __init__(self, db_config=conf.DB_CONFIG, section='DEFAULT'):

        if section not in ('LENDINGSTREAM', 'DRAFTY'):
            section = 'DEFAULT'
        config = configparser.ConfigParser()
        config.read(db_config)

        self.db = config.get(section, 'database')
        self.host = config.get(section, 'host')
        self.user = config.get(section, 'username')
        self.passwd = config.get(section, 'password')
        try:
            self.port = config.getint(section, 'port')
        except configparser.NoOptionError:
            self.port = 3306
        self.conn = self.connect()

    def connect(self):
        conn = None
        for i in range(conf.db_connect_retry_attempts):
            try:
                conn = MySQLdb.connect(host=self.host, user=self.user, passwd=self.passwd, db=self.db)
                conn.autocommit(True)
            except Exception as e:
                time.sleep(i * 5)
                print(e)
            return conn

    def getCursor(self, opt=''):
        cursor = None
        for i in range(2):
            try:
                cursor = ''
                if opt == 1:
                    cursor = self.conn.cursor()
                else:
                    cursor = self.conn.cursor(MySQLdb.cursors.SSDictCursor)
                return cursor
            except Exception as e:
                print(e)
        return cursor

    def processQuery(self, q, locals=None, opt='', copt=''):
        ret = None
        for i in range(2):
            try:
                cursor = self.getCursor(copt)
                cursor.execute(q, locals)
                if opt == 1:
                    records = cursor.fetchone()
                else:
                    records = cursor.fetchall()
                cursor.close()

                return records
            except Exception as e:
                print("Exception Query:", q)
                print("Exception:", e)
                print(traceback.format_exc())
                self.conn = self.connect()
        print(traceback.format_exc())
        raise RuntimeError("Execute failed despite our best attempts...Giving up :(")

    def replaceFromDict(self, d, table, colList=None):
        if not colList:
            colList = d.keys()
        query = "Replace into %s (%s) values (%s)" % (
            table, ",".join(colList), ",".join(map(lambda x: "%(" + x + ")s", colList)))
        ret = self.processQuery(query, d)
        return ret

    def insertFromDict(self, d, table, colList=None):
        if not colList:
            colList = d.keys()
        query = "insert into %s (%s) values (%s)" % (
            table, ",".join(colList), ",".join(map(lambda x: "%(" + x + ")s", colList)))
        ret = self.processQuery(query, d)
        return ret

    def updateFromDict(self, d, table, colList=None, **pkeys):
        if not colList:
            colList = d.keys()

        if pkeys:
            query = "update %s set %s where %s" % (table, ",".join(map(lambda x: x + " = %(" + x + ")s", colList)),
                                                   ' and '.join(map(lambda x: x + " = %(" + x + ")s", pkeys)))

        else:
            raise RuntimeError('Not a single key present where clause')

        ret = self.processQuery(query, dict(d, **pkeys))
        return ret

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()


if __name__ == "__main__":
    db = MySQLWrapper("localhost", "absuser")
    print(db.processQuery("select * from abc1;"))
    d = {'v1': 4, 'v2': 'dfdfd', }
    print("lastRowId = ", db.insertFromDict(d, 'abc1', ['v1', 'v2', ]))

    db.commit()
