# -*- coding: utf-8 -*-
"""
Created on Mon May 23 15:33:26 2022

@author: gabri
"""

import pyodbc

# from SQL_Conections import Financas

class SQL_Conections:
    def __init__(self):
        self.__server = 'localhost'
        
    @property
    def server(self):
        return self.__server
    
    def __del__(self):
        print('conn {} encerrada'.format(self.conn))
        self.conn.close()
    
class Financas(SQL_Conections):
    def __init__(self):
        super().__init__()
        self.__database = 'Financas'
        conn_str = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER='+self.server+';DATABASE='+self.database+';Trusted_Connection=yes;'
        self.conn = pyodbc.connect(conn_str)
        print(self.conn)

    @property
    def database(self):
        return self.__database


