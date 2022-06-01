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
        
    def novo_registro(self, classe, d):
        tex = {}
        vlr = {}
        dat = {}
        del_col = []
        self.cursor.execute(self.__select_ID('P_CLASSE', classe))
        d_classe = json.loads(self.cursor.fetchone()[1])
        for k in d:
            if k not in d_classe['ALIAS']:
                del_col.append(k)
        for dl in del_col:
            del d[dl]
        for i in range(len(d_classe['NOME'])):
            if d_classe['ALIAS'][i] not in d:
                continue
            d[d_classe['NOME'][i]] = d.pop(d_classe['ALIAS'][i])
        for k in d:
            if 'TEX' in k:
                tex[k] = d[k]
            if 'VLR' in k:
                vlr[k] = d[k]
            if 'DAT' in k:
                dat[k] = d[k]
        self.__insert_array(classe, tex, vlr, dat)
        
    def update_status_ID(self, ID, activate):
        self.cursor.execute(self.__update_array(ID, activate))
        
    def __update_array(self, ID, activate):
        result = 'UPDATE F_FATO SET '
        if activate == 1:
            result += "[DEL] = NULL "
        else:
            now = dt.now().strftime('%d/%m/%Y %H:%M:%S')
            result += "[DEL] = CONVERT(DATETIME, '{}', 103) ".format(now)
        result += 'WHERE ID = {}'.format(ID)
        if self.debug:
            print(result)
        return result
        
    def __insert_array(self, classe, tex, vlr, dat):
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
    
    def ler_tabela(self, classe, actives):
        self.cursor.execute(self.__select_classe(classe, actives))
        fato = self.cursor.fetchall()
        IDs = []
        for row in fato:
            IDs.append(row[0])
        result = Conn.__ler_registros(self, classe, IDs)
        return result
    
    def __select_classe(self, classe, actives):
        result = 'SELECT * FROM F_FATO WHERE CLASSE = {} '.format(classe)
        if actives:
            result += 'AND [DEL] IS NULL'
        else:
            result += 'AND [DEL] IS NOT NULL'
        if self.debug:
            print(result)
        return result
    
    def __ler_registros(self, classe, IDs):
        result = {}
        IDs = [IDs] if type(IDs) == int else IDs
        self.cursor.execute(self.__select_ID('P_CLASSE', classe))
        d_classe = json.loads(self.cursor.fetchone()[1])
        nome = d_classe['NOME']
        
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
            result[d_classe['ALIAS'][i]] = result.pop(nome[i])
        
        return result
        
    def __col_range(nomes, dimn):
        result = 1
        
        for nome in nomes:
            if dimn in nome:
                result+=1
        
        return result
    
    def __merge(result, ID, arr, dimn, col_range):
            n_e = True
            for r in arr:
                if ID == r[0]:
                    n_e = False
                    for i in range(col_range):
                        if '{}_{}'.format(dimn, i-1) in result:
                            result['{}_{}'.format(dimn, i-1)].append(r[i])
                        else:
                            result['{}_{}'.format(dimn, i-1)] = [r[i]]
                    break
            if n_e:
                for i in range(col_range):
                    if '{}_{}'.format(dimn, i-1) in result:
                        result['{}_{}'.format(dimn, i-1)].append(None)
                    else:
                        result['{}_{}'.format(dimn, i-1)] = [None]
            col = []    
            for k in result:
                if '-' in k:
                    col.append(k)
            for k in col:
                del result[k]
            return result
    
    def __select_dimn(self, table, nome, IDs):
        result = 'SELECT [FATO_ID], '
        for n in nome:
            if n[:-2] in table:
                result += '[{}], '.format(n)
        result = '{} '.format(result[:-2])
        result += 'FROM D_{} '.format(table)
        result += 'WHERE [ID] in (SELECT max([ID]) as [ID] '
        result += 'FROM D_{} '.format(table)
        result += 'WHERE [FATO_ID] in ('
        for ID in IDs:
            result += '{}, '.format(ID)
        result = '{}) '.format(result[:-2])
        result += 'GROUP BY [FATO_ID])'
        if self.debug:
            print(result)
        return result

    def __select_ID(self, table, ID):
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
    
    def __insert_dimn(self, dimn, ID, d):
        result = 'INSERT INTO D_{} (FATO_ID, '.format(dimn)
        for i in range(len(d)):
            result += '{}_{}, '.format(dimn, i)
        result = '{}) VALUES ({}, '.format(result[:-2], ID)
        for i in d:
            if dimn == 'VLR':
                result += '{}, '.format(d[i])
            if dimn == 'TEX':
                result += "'{}', ".format(d[i])
            if dimn == 'DAT':
                result += "CONVERT(DATETIME, '{}', 103), ".format(d[i].strftime('%d/%m/%Y %H:%M:%S'))
                
        result = '{})'.format(result[:-2])
        if self.debug:
            print('{} - QUERY D_{}: {}'.format(dt.now().strftime('%d/%m/%Y %H:%M:%S'),
                                               dimn,
                                               result))
        return result


from flask import Flask, jsonify, request
from flask_restful import Resource, Api

app = Flask(__name__)
api = Api(app)

class Financas(Conn):
    def __init__(self, debug=True):
        Conn.__init__(self, 'Financas', debug)

class simple_find(Resource, Financas):
    def get(self, ID):
        result = self.ler_ID(ID)
        return jsonify(result)
    
class mult_find(Resource, Financas):
    def get(self, ID, actives):
        if actives == 0:
            result = self.ler_tabela(ID, False)
        else:
            result = self.ler_tabela(ID, True)
        return jsonify(result)

class new_item(Resource, Financas):
    def post(self, classe):
        d = request.get_json()
        self.novo_registro(classe, d)
        return '', 201
    
api.add_resource(simple_find, '/simple_find/<int:ID>')
api.add_resource(mult_find, '/mult_find/<int:ID>/<int:actives>')
api.add_resource(new_item, '/new_item/<int:classe>')

if __name__ == '__main__':
    app.run(debug=True)
        

        

    
