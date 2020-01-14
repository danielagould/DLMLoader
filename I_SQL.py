import sqlalchemy
import pyodbc
import urllib.parse

class SQLconn:

    sqlalchemy_engine = sqlalchemy.engine
    # pyodbc_connection = pyodbc.Connection
    # pyodbc_cursor = pyodbc_connection.cursor()
    connString = ''

    def __init__(self,connString):
        self.connString = connString
        params = urllib.parse.quote_plus(connString)
        self.sqlalchemy_engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
        # self.pyodbc_connection = pyodbc.connect(connString)
        # self.pyodbc_cursor = self.pyodbc_connection.cursor

    def execSP_param_output(self,sql_str,parameters):
        connection = pyodbc.connect(self.connString)
        cursor = connection.cursor()
        cursor.execute(sql_str, parameters)
        proxy = cursor.fetchall()
        cursor.commit()
        return proxy

    def execSP_param(self,sql_str,parameters):
        connection = pyodbc.connect(self.connString)
        cursor = connection.cursor()
        cursor.execute(sql_str, parameters)
        cursor.commit()

