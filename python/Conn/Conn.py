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
        '''
        Create the Conn class.
        It establish connection to the databank
        
        Parameters
        ----------
        database : string
            Name of the database to connect
        debug : bool, optional
            Mode of the 'internal' debug mode. The default is False.

        Returns
        -------
        None.

        '''
        self.__debug = debug
        self.__server = 'localhost'
        self.__database = database
        conn_str = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + self.server
        conn_str += ';DATABASE=' + self.database + ';Trusted_Connection=yes;'
        self.__conn = pyodbc.connect(conn_str)
        if self.debug:
            print('{} - conn {} started'.format(dt.now().strftime('%d/%m/%Y %H:%M:%S'),
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
        '''
        Force a close connection to the databank.
        Lower risk to fasten the connection to the server.

        Returns
        -------
        None.

        '''
        if self.debug:
            print('{} - conn {} closed'.format(dt.now().strftime('%d/%m/%Y %H:%M:%S'),
                                               self.conn))
        self.cursor.close()
        self.conn.close()

    def read_ID(self, ID):
        '''
        Excetute a query to read a specific row of the database

        Parameters
        ----------
        ID : int
            ID of the row.

        Returns
        -------
        result : dict
            Dict of the specific row.

        '''
        fato = self.__read_fato_ID(ID)
        classe = fato[2]
        result = Conn.__read_array(self, classe, ID)
        return result
    
    def read_table(self, classe, actives):
        '''
        Execute a query to read all rows of a specific table.

        Parameters
        ----------
        classe : int
            ID of the table.
        actives : bool
            Select the actived or deactived rows.

        Returns
        -------
        result : dict
            Dict of the table.

        '''
        self.cursor.execute(self.__select_table(classe, actives))
        fato = self.cursor.fetchall()
        IDs = []
        for row in fato:
            IDs.append(row[0])
        result = Conn.__read_array(self, classe, IDs)
        return result
            
    def new_array(self, classe, d):
        '''
        Insert new array to the specific table

        Parameters
        ----------
        classe : int
            ID of the table.
        d : dict
            Data to be insert.

        Returns
        -------
        None.

        '''
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
        
    def update_ID(self, ID):
        '''
        Change the status (activated/deactivated) of the selected row.

        Parameters
        ----------
        ID : int
            ID of the row.

        Returns
        -------
        None.

        '''
        fato = self.__read_fato_ID(ID)
        if fato[3] == None:
            activate = 0
        else:
            activate = 1
        self.cursor.execute(self.__update_array(ID, activate))
        self.cursor.commit()

    def __read_fato_ID(self, ID):
        '''
        Select a specific row of the fact table

        Parameters
        ----------
        ID : int
            ID of the row.

        Returns
        -------
        fato : dict
            Dict of the selected row.

        '''
        self.cursor.execute(self.__select_ID('F_FATO', ID))
        fato = self.cursor.fetchone()
        return fato
    
    def __read_array(self, classe, IDs):
        '''
        Select and translate all data of the sepecific row.

        Parameters
        ----------
        classe : int
            ID of the table.
        IDs : list
            list of IDs that are going to be select.

        Returns
        -------
        result : dict
            Dict of the selected table.

        '''
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
                
            result = Conn.__merge(result, ID, tex, 'TEX', nome)
            result = Conn.__merge(result, ID, vlr, 'VLR', nome)
            result = Conn.__merge(result, ID, dat, 'DAT', nome)
            
        for i in range(len(nome)):
            result[d_classe['ALIAS'][i]] = result.pop(nome[i])
        return result

    def __select_ID(self, table, ID):
        '''
        Write the SELECT query of a specific row for the F_FATO or P_CLASSE.

        Parameters
        ----------
        table : string
            Table name (F_FATO, P_CLASSE).
        ID : int
            ID of the row.

        Returns
        -------
        result : stirng
            Query string for the SELECT.

        '''
        result = 'SELECT * FROM {} WHERE ID = {}'.format(table, ID)
        if self.debug:
            print(result)
        return result

    def __select_dimn(self, dimn, nome, IDs):
        '''
        Write the SELECT query for the Dimn tables

        Parameters
        ----------
        dimn : string
            Name of the dimn to be select.
        nome : list
            list of column logical names.
        IDs : list
            list of ID of the rows to be select.

        Returns
        -------
        result : string
            Query string for the SELECT.

        '''
        result = 'SELECT [FATO_ID], '
        for n in nome:
            if n[:-2] in dimn:
                result += '[{}], '.format(n)
        result = '{} '.format(result[:-2])
        result += 'FROM D_{} '.format(dimn)
        result += 'WHERE [ID] in (SELECT max([ID]) as [ID] '
        result += 'FROM D_{} '.format(dimn)
        result += 'WHERE [FATO_ID] in ('
        for ID in IDs:
            result += '{}, '.format(ID)
        result = '{}) '.format(result[:-2])
        result += 'GROUP BY [FATO_ID])'
        if self.debug:
            print(result)
        return result

    def __select_table(self, classe, actives):
        '''
        Write the SELECT query for a specific table

        Parameters
        ----------
        classe : int
            ID of the table.
        actives : bool
            Select actives or deactived rows.

        Returns
        -------
        result : string
            Query string of the SELECT.

        '''
        result = 'SELECT * FROM F_FATO WHERE CLASSE = {} '.format(classe)
        if actives:
            result += 'AND [DEL] IS NULL'
        else:
            result += 'AND [DEL] IS NOT NULL'
        if self.debug:
            print(result)
        return result
    
    def __insert_array(self, classe, tex, vlr, dat):
        '''
        Insert the lists in the right dimn of the database

        Parameters
        ----------
        classe : int
            ID of the table.
        tex : list
            list of tex's dimn.
        vlr : list
            list of vlr's dimn.
        dat : list
            list of dat's dimn.

        Returns
        -------
        None.

        '''
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
            
    def __insert_fato(self, classe, tex, vlr, dat):
        '''
        Write the Insert query of the fato

        Parameters
        ----------
        classe : int
            ID of the table.
        tex : bool
            Contains data.
        vlr : bool
            Contains data.
        dat : bool
            Contains data.

        Returns
        -------
        result : string
            Query for insert data in the database.

        '''
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
        '''
        Write the Insert query of the dimn

        Parameters
        ----------
        dimn : string
            Selected dimn to be insert.
        ID : int
            ID of the row to indentify the dimn.
        d : dict
            Dict containing all data to be insert.

        Returns
        -------
        result : string
            Query for the insert data in the database.

        '''
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

    def __update_array(self, ID, activate):
        '''
        Write the Update query for the fato.

        Parameters
        ----------
        ID : int
            ID of the row.
        activate : int
            activate or deactivate a row.

        Returns
        -------
        result : string
            Qurey for the update row.

        '''
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
        
    def __col_range(nomes, dimn):
        '''
        Range of columns expected in the dimn of a table

        Parameters
        ----------
        nomes : list
            list of logical names of the table.
        dimn : list
            list of dimn column that arrived.

        Returns
        -------
        result : int
            Qtt of columns.

        '''
        result = 1
        for nome in nomes:
            if dimn in nome:
                result+=1
        return result
    
    def __merge(result, ID, arr, dimn, nomes):
        '''
        Merge the fato + result to the dimn

        Parameters
        ----------
        result : dict
            Dict of the fato or fato + past dimn.
        ID : int
            ID of the row.
        arr : list
            List of dimn to be merge.
        dimn : string
            Dimn to be treat.
        nomes : list
            List of logical names.

        Returns
        -------
        result : dict
            Dict of the merged table.

        '''
        col_range = Conn.__col_range(nomes, dimn)
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


        

    
