# -*- coding: UTF-8 -*-
'''
Created on 22 de mai de 2018
Updated on 14 de ago de 2018

@author: braulio
'''

import json
import re
import redis
from pymongo import MongoClient
from cassandra.cluster import Cluster

def main():

    #Exemplo JSON temperatura
    json_temperatura_string_setData = '''{
                                                    "operation" : "setData", 
                                                    "db" : "temperature_db", 
                                                    "data" : "56", 
                                                    "storageOption" : "w_performance", 
                                                    "dataAnalysis" : "spark"
                                                }'''
    
    json_temperatura_cassandra_string_setData = '''{
                                                    "operation" : "setData", 
                                                    "db" : "temperaturas", 
                                                    "data" : "56", 
                                                    "storageOption" : "cassandra", 
                                                    "dataAnalysis" : "spark"
                                                }'''
    
    #Exemplo JSON latitude
    json_latitude_string_setData = '''{
                                            "operation" : "setData", 
                                            "db" : "latitude_db", 
                                            "data" : "90° 53´N", 
                                            "storageOption" : "w_performance", 
                                            "dataAnalysis" : "spark"
                                        }'''

    #Exemplo JSON multimedia
    json_multimedia_string_setData = '''{
                                                "operation" : "setData", 
                                                "db" : "multimedia_db", 
                                                "data" : "LaCasaDePapel01.mp4;BinData(0,'AAAAKGZ0eXBNNFYgAAAAAWlzb21hdmMxaXNvNk00QSBNNFYgbXA0MgAakaRtb292AAAAbG12aGQAAAAA1o8DytaPA8oAAAJYACXr5gABAAABAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADAAAAFWlvZHMAAAAAEAcAT///KH//ABQYg3RyYWsAAABcdGtoZAAAAAHWjwPK1o8DzQAAAAEAAAAAACXroAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAEAAAAADVQAAAeAAAAAAACRlZHRzAAAAHGVsc3QAAAAAAAAAAQAl66AAAAfQAAEAAAAUF/ttZGlhAAAAIG1kaGQAAAAA1o8DytaPA80AAGGoBiwEYFXEAAAAAABiaGRscgAAAAAAAAAAdmlkZQAAAAAAAAAAAAAAADI2NCN2aWRlbzpmcHM9MjU6cGFyPTE2MDoxNTlAR1BBQzAuNS4yLURFVi1yZXY5OTgtZ2JkZGEyZWUtbWFzdGVyAAAUF3FtaW5mAAAAFHZtaGQAAAABAAAAAAAAAAAAAAAkZGluZgAAABxkcmVmAAAAAAAAAAEAAAAMdXJsIAAAAAEAFBcxc3RibAAAANxzdHNkAAAAAAAAAAEAAADMYXZjMQAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAANQAeAASAAAAEgAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABj//wAAABBwYXNwAAAAoAAAAJ8AAAA6YXZjQwFkAB7/4QAdZ2QAHqzZQNQ9v/AKAAnxAAADAAEAAAMAMo8WLZYBAAZo6+LLIsD9+PgAAAAAGHV1aWQAAAAAAAAAAAAAAAAAAAAAAAAAFGJ0cnQAAXvWAB2NaAAHBEAAAAAYc3R0cwAAAAAAAAABAAGUfAAAA+gADE9AY3R0cwAAAAAAAYnmAAAAAQAAB9AAAAABAAATiAAAAAEAAAfQAAAAAQAAAAAAAAABAAAD6AAAAAEAABOIAAAAAQAAB9AAAAABAAAAAAAAAAEAAAPoAAAAAQAAE4gAAAABAAAH0AAAAAEAAAAAAAAAAQAAA+gAAAABAAATiAAAAAEAAAfQAAAAAQAAAAAAAAABAAAD6AAAAAIAAAfQAAAAAQAAD6AAAAACAAAD6AAAAAEAABOIAAAAAQAAB9AAAAABAAAAAAAAAAEAAAPoAAAAAQAAE4gAAAABAAAH0AAAAAEAAAAAAAAAAQAAA+gAAAABAAATiAAAAAEAAAfQAA==')", 
                                                "storageOption" : "r_performance", 
                                                "dataAnalysis" : "spark"
                                              }'''


    # Identifica o tipo de dado para escolher em qual banco gravar
    #tipo =  identificarTipoDeDadoIoT(json_temperatura_string_setData)
    #tipo =  identificarTipoDeDadoIoT(json_latitude_string_setData)
    #tipo =  identificarTipoDeDadoIoT(json_multimedia_string_setData)
      
    #Teste gravação Redis
    #setData(json_temperatura_string_setData)
    #Teste consulta Redis
    #getData(json_temperatura_string_setData)
    
    #Teste gravação MongoDB
    #setData(json_multimedia_string_setData)
    #Teste consulta MongoDB
    #getData(json_multimedia_string_setData)
    
    #Teste gravação Cassandra
    #setData(json_temperatura_cassandra_string_setData)
    #Teste consulta Cassandra
    #getData(json_temperatura_cassandra_string_setData)
    
#Operação para obter dados
def getData(requisicao_json):
    #get "temperature_db_escalar_56"
    tipo = identificarTipoDeDadoIoT(requisicao_json)
    
    json_decodificado = json.loads(requisicao_json)
    dado = u''.join(json_decodificado['data']).encode('utf-8').strip()
    banco = u''.join(json_decodificado['db']).encode('utf-8').strip()
    criterio = u''.join(json_decodificado['storageOption']).encode('utf-8').strip()
    nome = ''
    if ( tipo == 'multimedia' ):
        posicao = dado.find(';')
        nome = dado[0:posicao]
        
    if ( (criterio == 'w_performance') or (tipo != 'multimedia' and criterio == 'r_performance') ):
        print obterDadoRedis(dado, banco, tipo)
    # MongoDB obteve menor tempo de consulta multimídia
    elif ( tipo ==  'multimedia' and criterio == 'r_performance' ):
        obterDadoMongoDB(nome)
    else:
        obterDadoCassandra(dado, banco)

#Operação para gravar dados
def setData(requisicao_json):
    # obter o tipo de dado
    tipo = identificarTipoDeDadoIoT(requisicao_json)
    
    json_decodificado = json.loads(requisicao_json)
    dado = u''.join(json_decodificado['data']).encode('utf-8').strip()
    banco = u''.join(json_decodificado['db']).encode('utf-8').strip()
    criterio = u''.join(json_decodificado['storageOption']).encode('utf-8').strip()
    nome = ''
    if ( tipo == 'multimedia' ):
        posicao = dado.find(';')
        nome = dado[0:posicao]
    
    #Grava efetivamente a depender do criterio
    definirBDGravar(tipo, criterio, dado, banco, nome)

# Definir onde gravar a depender do tipo
def definirBDGravar(tipo, criterio, dado, banco, nome):
    # Redis obteve menores tempos de escrita e leitura escalar e posicional
    if ( (criterio == 'w_performance') or (tipo != 'multimedia' and criterio == 'r_performance') ):
        gravarRedis(dado, banco, tipo)
    # MongoDB obteve menor tempo de consulta multimídia
    elif ( tipo ==  'multimedia' and criterio == 'r_performance' ):
        gravarMongoDB(dado, tipo, nome)
    else:
        gravarCassandra(dado, banco)

# Gravar no Redis
def gravarRedis(dado, banco, tipo):
    r = redis.StrictRedis(host = '127.0.0.1', port = 6379, db = 0)
    r.set(banco + '_' + tipo + '_' + dado, dado)

# Obter dado do Redis
def obterDadoRedis(dado, banco, tipo):
    r = redis.StrictRedis(host = '127.0.0.1', port = 6379, db = 0)
    return r.get(banco + '_' + tipo + '_' + dado)

# Gravar no MongoDB
def gravarMongoDB(dado, tipo, nome):
    cliente = MongoClient('localhost', 27017)
    bd = cliente['test']
    colecao = bd['escalares_posicionais']
    colecao.insert({'_id' : nome, tipo : dado})

# Obter dado do MongoDB    
def obterDadoMongoDB(nome):
    cliente = MongoClient('localhost', 27017)
    bd = cliente['test']
    colecao = bd['escalares_posicionais']
    cursor = colecao.find({'_id' : nome})
    for document in cursor:
        print document

# Gravar no Cassandra
def gravarCassandra(dado, banco):
    # Conexao
    cluster = Cluster()
    # Conecta ao keyspace 'dadossensoriot'
    session = cluster.connect('dadossensoriot')
    session.execute("insert into " + banco + "(id, temperatura) values (%s, %s)", (int(dado), int(dado)))
    
# Obter dado do Cassandra
def obterDadoCassandra(dado, banco):
    # Conexao
    cluster = Cluster()
    # Conecta ao keyspace 'dadossensoriot'
    session = cluster.connect('dadossensoriot')
    consulta = "select temperatura from " + banco + " where id = " + dado
    linhas = session.execute(consulta)
    resultado = ""
    for l in linhas:
        resultado = str(l)
    resultado = resultado[ resultado.find("=")+1 : resultado.find(")") ]
    print resultado
    
# Identifica o tipo de dado
def identificarTipoDeDadoIoT(requisicao_json):
    #Decodifica o dado e obtem o mesmo
    json_decodificado = json.loads(requisicao_json)
    data = u''.join(json_decodificado['data']).encode('utf-8').strip()
    
    #Casa os padrões para identificar o tipo
    pattern_escalar = re.compile('\d+[^W]')
    pattern_posicional = re.compile("\d+°\s\d+´[W|N]")
    pattern_multimedia = re.compile(".*;BinData*")
    
    if (pattern_posicional.match(data) is not None):
        #return 'Posicional: %s' % data
        return 'posicional'
    elif (pattern_escalar.match(data) is not None):
        #return 'Escalar: %s' % data
        return 'escalar'
    elif (pattern_multimedia.match(data) is not None):
        #return 'Multimídia: %s' % data
        return 'multimedia'
    else:
        return 'tipo não identificado'

if __name__ == '__main__':
    main()