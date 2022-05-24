# -*- coding: utf-8 -*-
"""
Created on Mon May 23 15:33:26 2022

@author: gabri
"""

import pyodbc
from datetime import datetime as dt

# from Conn import Financas

class Conn:
    def __init__(self, debug=False):
        self.__server = 'localhost'
        self.__debug = debug
        
    @property
    def server(self):
        return self.__server
    
    @property
    def debug(self):
        return self.__debug
    
    def __del__(self):
        if self.debug:
            print('{} - conn {} encerrada'.format(dt.now().strftime('%d/%m/%Y %H:%M:%S'),
                                                  self.conn))
        self.cursor.close()
        self.conn.close()
    
class Financas(Conn):
    def __init__(self, debug=True):
        super().__init__(debug)
        self.__database = 'Financas'
        conn_str = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + self.server
        conn_str += ';DATABASE=' + self.database + ';Trusted_Connection=yes;'
        self.__conn = pyodbc.connect(conn_str)
        if self.debug:
            print('{} - conn {} iniciada'.format(dt.now().strftime('%d/%m/%Y %H:%M:%S'),
                                                 self.conn))
        self.cursor = self.conn.cursor()
        
    @property
    def database(self):
        return self.__database
    @property
    def conn(self):
        return self.__conn
    
    def novo_registro(self, classe, tex, vlr, dat):
        self.cursor.execute(self.__inset_fato(classe, tex, vlr, dat))
        self.cursor.commit()
        ID = self.cursor.execute("SELECT @@IDENTITY AS ID;").fetchone()[0]
        if bool(tex):
            self.cursor.execute(self.__inset_dimn('TEX', ID, tex))
            self.cursor.commit()
        if bool(vlr):
            self.cursor.execute(self.__inset_dimn('VLR', ID, vlr))
            self.cursor.commit()
        if bool(dat):
            self.cursor.execute(self.__inset_dimn('DAT', ID, dat))
            self.cursor.commit()

    def __inset_fato(self, classe, tex, vlr, dat):
        result =    'INSERT INTO F_FATO (CLASSE, TEX, VLR, DAT) '
        result +=   'VALUES '
        result +=   '({0}, {1}, {2}, {3})'.format(classe,
                                                  int(bool(tex)),
                                                  int(bool(vlr)),
                                                  int(bool(dat)))
        if self.debug:
            print('{} - QUERY F_FATO: {}'.format(dt.now().strftime('%d/%m/%Y %H:%M:%S'),
                                                 result))
        return result
    
    def __inset_dimn(self, param, ID, lista):
        result =    'INSERT INTO D_{} (FATO_ID, '.format(param)
        for i in range(len(lista)):
            result += '{}_{}, '.format(param, i)
        result = '{}) VALUES ({}, '.format(result[:-2], ID)
        for i in lista:
            if param == 'VLR':
                result += '{}, '.format(i)
            if param == 'TEX':
                result += "'{}', ".format(i)
            if param == 'DAT':
                result += "CONVERT(DATETIME, '{}', 103), ".format(i.strftime('%d/%m/%Y %H:%M:%S'))
                
        result = '{})'.format(result[:-2])
        if self.debug:
            print('{} - QUERY D_{}: {}'.format(dt.now().strftime('%d/%m/%Y %H:%M:%S'),
                                               param,
                                               result))
        return result
    
    
