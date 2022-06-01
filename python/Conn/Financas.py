# -*- coding: utf-8 -*-
"""
Created on Wed Jun  1 12:44:56 2022

@author: gabri
"""

from Conn import Conn

from flask import Flask, jsonify, request
from flask_restful import Resource, Api

app = Flask(__name__)
api = Api(app)

class Financas(Conn):
    def __init__(self, debug=True):
        Conn.__init__(self, 'Financas', debug)

class simple_array(Resource, Financas):
    def get(self, ID):
        result = self.read_ID(ID)
        return jsonify(result)
    
    def put(self, ID):
        self.update_ID(ID)
        return '', 201

    def post(self, ID):
        d = request.get_json()
        self.new_array(ID, d)
        return '', 201
    
class mult_array(Resource, Financas):
    def get(self, ID, actives):
        if actives == 0:
            result = self.read_table(ID, False)
        else:
            result = self.read_table(ID, True)
        return jsonify(result)
    
api.add_resource(simple_array, '/simple_array/<int:ID>')
api.add_resource(mult_array, '/mult_array/<int:ID>/<int:actives>')

if __name__ == '__main__':
    app.run(debug=True)
        
