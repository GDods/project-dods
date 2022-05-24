# -*- coding: utf-8 -*-
"""
Created on Mon May 23 15:33:26 2022

@author: gabri
"""

import pyodbc
import json
from datetime import datetime as dt

# from Conn import Financas

class Conn:
    def __init__(self, database, debug=False):
        self.__debug = debug
        self.__server = 'localhost'
        self.__database = database
        conn_str = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + self.server
        conn_str += ';DATABASE=' + self.database + ';Trusted_Connection=yes;'
        self.__conn = pyodbc.connect(conn_str)
        if self.debug:
            print('{} - conn {} iniciada'.format(dt.now().strftime('%d/%m/%Y %H:%M:%S'),
                                                 self.conn))
        self.cursor = self.conn.cursor()
        
    @property
    def debug(self):
        return self.__debug
    @property
    def server(self):
        return self.__server
    @property
    def database(self):
        return self.__database
    @property
    def conn(self):
        return self.__conn
    
    def __del__(self):
        if self.debug:
            print('{} - conn {} encerrada'.format(dt.now().strftime('%d/%m/%Y %H:%M:%S'),
                                                  self.conn))
        self.cursor.close()
        self.conn.close()
        
    def novo_registro(self, classe, tex, vlr, dat):
        self.cursor.execute(self.__insert_fato(classe, tex, vlr, dat))
        self.cursor.commit()
        ID = self.cursor.execute("SELECT @@IDENTITY AS ID;").fetchone()[0]
        if bool(tex):
            self.cursor.execute(self.__insert_dimn('TEX', ID, tex))
            self.cursor.commit()
        if bool(vlr):
            self.cursor.execute(self.__insert_dimn('VLR', ID, vlr))
            self.cursor.commit()
        if bool(dat):
            self.cursor.execute(self.__insert_dimn('DAT', ID, dat))
            self.cursor.commit()
            
    def ler_ID(self, ID):
        self.cursor.execute(self.__select_ID('F_FATO', ID))
        fato = self.cursor.fetchone()
        classe =  fato[2]
        result = Conn.__ler_registros(self, classe, ID)
        return result
            
    def __ler_registros(self, classe, IDs):
        result = {}
        IDs = [IDs] if type(IDs) == int else IDs
        self.cursor.execute(self.__select_ID('P_CLASSE', classe))
        param = json.loads(self.cursor.fetchone()[1])
        nome = param['NOME']
        
        if 'TEX_0' in nome:
            self.cursor.execute(self.__select_dimn('TEX', nome, IDs))
            tex = self.cursor.fetchall()
        if 'VLR_0' in nome:
            self.cursor.execute(self.__select_dimn('VLR', nome, IDs))
            vlr = self.cursor.fetchall()
        if 'DAT_0' in nome:
            self.cursor.execute(self.__select_dimn('DAT', nome, IDs))
            dat = self.cursor.fetchall()
        
        for ID in IDs:
            if 'ID' in result:
                result['ID'].append(ID)
            else:
                result['ID'] = [ID]
                
            result = Conn.__merge(result, ID, tex, 'TEX',
                                  Conn.__col_range(nome, 'TEX'))
            result = Conn.__merge(result, ID, vlr, 'VLR', 
                                  Conn.__col_range(nome, 'VLR'))
            result = Conn.__merge(result, ID, dat, 'DAT', 
                                  Conn.__col_range(nome, 'DAT'))
        
        for i in range(len(nome)):
            result[param['ALIAS'][i]] = result.pop(nome[i])
        
        return result
        
    def __col_range(nomes, dimn):
        result = 1
        
        for nome in nomes:
            if dimn in nome:
                result+=1
        
        return result
    
    def __merge(result, ID, arr, dimn, col_range):
            # i = 0
            n_e = True
            for r in arr:
                if ID == r[0]:
                    n_e = False
                    for i in range(col_range):
                        if '{}_{}'.format(dimn, i-1) in result:
                            result['{}_{}'.format(dimn, i-1)].append(r[i])
                        else:
                            result['{}_{}'.format(dimn, i-1)] = [r[i]]
                        # i+=1
                    break
            if n_e:
                for i in range(col_range):
                    if '{}_{}'.format(dimn, i-1) in result:
                        result['{}_{}'.format(dimn, i-1)].append(None)
                    else:
                        result['{}_{}'.format(dimn, i-1)] = [None]
                    # i+=1
            result.pop('TEX_-1', None)
            result.pop('VLR_-1', None)
            result.pop('DAT_-1', None)
            
            return result
    
    def __select_dimn(self, table, nome, IDs):
        result = 'SELECT [FATO_ID], '
        for n in nome:
            if n[:-2] in table:
                result += '[{}], '.format(n)
        result = '{} '.format(result[:-2])
        result += 'FROM D_{} '.format(table)
        result += 'WHERE [FATO_ID] in ('
        for ID in IDs:
            result += '{}, '.format(ID)
        result = '{})'.format(result[:-2])
        if self.debug:
            print(result)
        return result

    def __select_ID(self, table, ID):
        if ID == 0:
            result = 'SELECT * FROM {}'.format(table)
        else:
            result = 'SELECT * FROM {} WHERE ID = {}'.format(table, ID)
        if self.debug:
            print(result)
        return result

    def __insert_fato(self, classe, tex, vlr, dat):
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
    
    def __insert_dimn(self, param, ID, lista):
        result = 'INSERT INTO D_{} (FATO_ID, '.format(param)
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
    
class Financas(Conn):
    def __init__(self, debug=True):
        super().__init__('Financas', debug)

        

    
