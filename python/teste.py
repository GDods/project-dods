# -*- coding: utf-8 -*-
"""
Created on Mon May 30 11:01:42 2022

@author: gabri
"""

import requests

BASE = 'http://127.0.0.1:5000/'

response = requests.get(BASE + '/simple_find/19').json()
response = requests.get(BASE + '/mult_find/1/1').json()
# response = requests.post(BASE + '/new_item/1', verify=False, 
#                          json={"Produto":"real3","Categoria":"real4","Valor":9999, "teste_de_limpeza":"Abacaxi"})


