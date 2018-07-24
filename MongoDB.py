#-*- coding: UTF-8 -*-
'''
Created on 11 de ago de 2017

@author: braulio.junior
'''

import base64
import bson
from bson.binary import Binary
from bson.objectid import ObjectId
import gridfs
from gridfs.grid_file import (GridIn,
                              GridOut,
                              GridOutCursor,
                              DEFAULT_CHUNK_SIZE)
import io
import math
import os
from pprint import pprint
from pymongo import MongoClient
import timeit

from Sensor import SensorIoT


cliente = MongoClient('localhost', 27017)

banco = cliente['test']
colecao = banco['escalares_posicionais']

# V�rias temperaturas
def gravar_temperaturas_unico_mongodb(nome_arquivo):
    
    with io.open(nome_arquivo, 'rb', buffering=2<<16) as arquivo:
        indice = 0
        t = 0
        num_str = ""
        print '>>> Inicio de gravação de temperaturas no mongodb <<<'
        for linha in arquivo:
            linha_str = ""
            linha_str = str(linha)
            while (indice < len(linha_str)):
                if (linha[indice] != ','):
                    num_str = num_str + linha[indice]
                    indice += 1
                else:
                    num = int(num_str)
                    inicio = timeit.default_timer()
                    colecao.insert({'_id':colecao.count() + 1 ,'sensor':'DHT11', 'temperatura' : num})
                    fim = timeit.default_timer()
                    t = t + (inicio - fim)/1000000
                    print 'duracao parcial da gravação no mongodb: %f segundos' % t
                    indice += 1
                    num_str = ""   
        print '>>> Fim de gravação de temperaturas no mongodb <<<'
        t = t/60
        print 'duracao da gravação no mongodb: %f minutos' % t


def gravar_temperaturas_escalar_mongodb(tam_dataset):
        t = 0
        cont = 1
        #print '>>> Inicio de geração de temperaturas em memoria <<<'
        temperaturas = SensorIoT.gerar_dados_escalares(tam_dataset)
        #print '>>> Fim de geração de temperaturas em memoria <<<'
        for temperatura in temperaturas:
            inicio = timeit.default_timer()
            colecao.insert({'_id':cont, 'temperatura' : temperatura})
            fim = timeit.default_timer()
            t = t + (inicio - fim)/1000000
            tempo = abs(t) * 1000000
            cont += 1
            #print 'duracao parcial da gravação no mongodb: %f segundos' % t
        #print '>>> Fim de gravação de temperaturas no mongodb <<<'
        #return '%s temperaturas gravadas em %d segundos. ' % (len(temperaturas), tempo)
        return tempo

def gravar_multimidia_mongodb(local, arquivo):
    
    local_e_arquivo = local + "/" + arquivo
    
    #tamanho = os.path.getsize(local_e_arquivo)
    #chunk_size = tamanho
    chunk_size = 1024
    
    gridfs.grid_file.DEFAULT_CHUNK_SIZE = chunk_size
    
    #fsb = gridfs.GridFSBucket(banco)
    fs = gridfs.GridFS(banco)
            
    '''for i in fs.find(): # or fs.list()
        fs.delete(i._id)'''
    
    with open(local_e_arquivo, 'rb') as a:
        f = a.read()
        encoded = Binary(f, 0)
    
    '''inicio = timeit.default_timer()
    colecao.insert({'nome_arquivo' : arquivo, 'arquivo' : encoded})
    fim = timeit.default_timer()
    t = (inicio - fim)/1000000'''
    #tempo = abs(t) * 1000000   
    
    inicio = timeit.default_timer()
    id = fs.put(encoded, filename = arquivo)
    #a = fsb.upload_from_stream(arquivo, local_e_arquivo)
    #a = fsi.write(encoded)
    fim = timeit.default_timer()
    t = (inicio - fim)/1000000
    t = abs(t)
    tempo = abs(t)*1000000
    
    id_tempo = []
    id_tempo.append(id)
    id_tempo.append(tempo)
    
    #print 'Arquivo de multimidia gravado em %.9f segundos. ' % t
    return id_tempo

def deletar_multimidia_unico_mongodb(id):
    fs = gridfs.GridFS(banco)
    inicio = timeit.default_timer()
    if fs.exists({"_id": id}):
        fim = timeit.default_timer()
        t = (inicio - fim)/1000000
        tempo = abs(t) * 1000
        print 'arquivo localizado em %d milisegundos. ' % tempo
        fs.delete(id)
    else:
        print 'arquivo não existe!'

def inserir_pedacos_multimidia(caminho):
    diretorio = os.listdir(caminho)
    for arquivo in diretorio:
        local_e_arquivo = caminho + "/" + arquivo
        with open(local_e_arquivo, 'rb') as a:
            f = a.read()
            encoded = Binary(f, 0)
            colecao.insert({'nome_arquivo' : arquivo, 'arquivo': encoded})

#https://stackoverflow.com/questions/31155508/how-to-store-and-get-binary-files-from-gridfs
def consultar_chunk_multimidia_mongodb(IdArquivo):
    inicio = timeit.default_timer()
    banco.fs.chunks.find_one({"files_id" : ObjectId(IdArquivo), "n" : (banco.fs.chunks.count() - 1)})
    fim = timeit.default_timer()
    t = (inicio - fim)/1000000
    t = abs(t)
    tempo = abs(t)*1000000
    return tempo
    #print banco.fs.chunks.find_one({"files_id" : ObjectId(IdArquivo), "n" : (banco.fs.chunks.count() - 1)})
    #print (banco.fs.chunks.count() - 1)
    
def consultar_multimidia_mongodb(arquivo):
    if (colecao.find({'nome_arquivo' : arquivo}).count() > 0):     
        consulta = colecao.find({'nome_arquivo' : arquivo}).explain()['executionStats']
        for atrib, valor in consulta.items():
            if atrib == "executionTimeMillis":
                print 'Tempo de busca: %.20f' % (float(valor) / 1000) + ' segundos.'
    else:
        print "Item não encontrado"
    
def consultar_temperatura_mongodb(temperatura):
    if (colecao.find({'temperatura' : temperatura}).count() > 0):     
        consulta = colecao.find({'temperatura' : temperatura}).explain()['executionStats']
        for atrib, valor in consulta.items():
            if atrib == "executionTimeMillis":
                #return 'Tempo de busca: %.2f' % (float(valor) / 1000) + ' segundos'
                return float(valor) / 1000
    #else:
        #return "Item não encontrado"
            
def gravar_dados_posicionais_mongodb(tam_dataset):
        t = 0
        cont = 1
        #print '>>> Inicio de geração de dados posicionais em memoria <<<'
        latitudes = SensorIoT.gerar_dados_posicionais(tam_dataset)
        #print '>>> Fim de geração de dados posicionais em memoria <<<'
        for latitude in latitudes:
            inicio = timeit.default_timer()
            colecao.insert({'_id': cont , 'latitude' : latitude})
            fim = timeit.default_timer()
            t = t + (inicio - fim)/1000000
            tempo = abs(t) * 1000000
            cont += 1
            #print 'duracao parcial da gravação no mongodb: %f segundos' % t
        #print '>>> Fim de gravação de dados posicionais no mongodb <<<'
        #return '%s latitudes gravadas em %d segundos. ' % (len(latitudes), tempo)
        return tempo

def consultar_posicao_mongodb(latitude):
    if (colecao.find({'latitude' : latitude}).count() > 0):     
        consulta = colecao.find({'latitude' : latitude}).explain()['executionStats']
        for atrib, valor in consulta.items():
            if atrib == "executionTimeMillis":
                #return 'Tempo de busca: %.2f' % (float(valor) / 1000) + ' segundos.'
                return float(valor) / 1000
    #else:
        #return "Item não encontrado"
        
#Inserir documento
#colecao.insert({'latitude' : '0° 0´N'})
#colecao.insert({'temperatura' : 99})

# Atualizar documento
# colecao.update({'sensor':'DHT11'},{'$set': {'sensor' : 'Outro'}})
# colecao.update({'sensor':'Outro'},{'$set': {'temperatura' : '99', 'sensor' : 'DHT11'}})

# Deletar
#colecao.remove({'temperatura' : 99})
#colecao.remove({'latitude' : '0° 0´N'})
#colecao.remove({})

#gravar_temperaturas_unico_mongodb('temperaturas_bin')
#colecao.remove({})
#tam_dataset = 1048576
#gravar_temperaturas_escalar_mongodb(tam_dataset)
#gravar_dados_posicionais_mongodb(tam_dataset)
#print 'Total: %s dados sensoriais' % colecao.count()
#gravar_dados_posicionais_mongodb()
#print 'Total: %s dados posicionais' % colecao.count()

# Consultar
#consultar_temperatura_mongodb(99)

'''consulta = colecao.find({'temperatura' : 99})
for item in consulta:
    print item'''

# latitude: 63\xb0 57\xb4N
#consultar_posicao_mongodb('0° 0´N')

#print colecao.find({"latitude" : "82º 75N"}).explain()['executionStats']
'''consulta = colecao.find({'latitude' : '0° 0´N'})
for item in consulta:
    print item'''

'''consulta = colecao.find({'nome_arquivo' : "IMG_4561.jpg"})
for item in consulta:
    print item'''

#consulta = colecao.find({"sensor" : "posicional"})
'''for posicao in colecao.find():
    print posicao'''

# print 'Total: %s temperaturas' % colecao.count()

#id = gravar_multimidia_unico_mongodb("diretorio","1048576.mp4")
#print id
#deletar_multimidia_unico_mongodb(ObjectId(id))

#Testes de consulta de arquivo multimidia
#inserir_pedacos_multimidia("diretorio")
#consultar_multimidia_mongodb("teste")

# Datasets de 1048576, 2097152, 4194304, 8388608, 16777216, 33554432, 67108864 bytes
'''datasets = [1048576]
mensagem = ""
for dataset in datasets:
    colecao.remove({})
    mensagem = mensagem + 'Dataset de %dMB: ' % math.floor(dataset/1048576)
    mensagem = mensagem + gravar_temperaturas_escalar_mongodb(datasets) + '\n'
    colecao.insert({'temperatura' : 0})    
    mensagem = mensagem + consultar_temperatura_mongodb(0) + '\n'
print mensagem'''

# Datasets de 1048576, 2097152, 4194304, 8388608, 16777216, 33554432, 67108864 bytes
#colecao.remove({})
#tam_dataset = 1048576
#print gravar_temperaturas_escalar_mongodb(tam_dataset)
#colecao.insert({'temperatura' : 100})
#print consultar_temperatura_mongodb(100)

#colecao.remove({})
#tam_dataset = 1048576
#print gravar_dados_posicionais_mongodb(tam_dataset)
#print gravar_temperaturas_escalar_mongodb(tam_dataset)
#colecao.insert({'latitude' : '0° 0´N'})
#print consultar_posicao_mongodb('0° 0´N')
#colecao.insert({'temperatura' : 100})
#print consultar_temperatura_mongodb(100)

#colecao.remove({})
'''tam_dataset = 1048576
print gravar_temperaturas_escalar_mongodb(tam_dataset)'''

def gravar_dados_escalares_geral_mongodb(dataset, contadorgeral):
    limpar_dados_mongodb()
    texto = ""
    #contador do número de escritas
    contadorescritaescalar = 0
    #Inicializador dos tempos de escrita escalares/posicionais
    tempoescalar = 0
    #Insere os dados escalares no número de vezes do contadorgeral e pega o tempo médio
    print 'Gravação de dados escalares de %dMb no mongodb.' % (dataset/1048576)
    while contadorescritaescalar < contadorgeral:
        tempoescalar = tempoescalar + gravar_temperaturas_escalar_mongodb(dataset)
        contadorescritaescalar = contadorescritaescalar + 1
        if contadorescritaescalar < contadorgeral:
            colecao.remove({})
    texto = texto +  'Gravação de %dMb de dados escalares com tempo médio final de %d segundos.' % (dataset/1048576, tempoescalar/contadorescritaescalar) + '\n'
    return texto

def gravar_dados_posicionais_geral_mongodb(dataset, contadorgeral):
    limpar_dados_mongodb()
    texto = ""
    #Limpa os dados antes de continuar os testes
    colecao.remove({})
    #contador do número de escritas posicionais
    contadorescritaposicional = 0
    #Inicializador dos tempos de escritas posicionais
    tempoposicional = 0
    #Insere os dados posicionais no número de vezes do contadorgeral e pega o tempo médio
    print 'Escrita de dados posicionais de %dMb no mongodb.' % (dataset/1048576)
    while contadorescritaposicional < contadorgeral:
        tempoposicional = tempoposicional + gravar_dados_posicionais_mongodb(dataset)
        contadorescritaposicional = contadorescritaposicional + 1
        if contadorescritaposicional < contadorgeral:
            colecao.remove({})
    texto = texto + 'Gravação de %dMb de dados posicionais com tempo médio final de %d segundos.' % (dataset/1048576, tempoposicional/contadorescritaposicional) + '\n'
    return texto

def gravar_dados_multimidia_geral_mongodb(dataset, contadorgeral):
    limpar_dados_mongodb()
    texto = ""
    #Inicializador contador de escrita multimidia
    contadorescritamultimidia = 0
    #Inicializador dos tempos de escrita multimidia
    tempoescritamultimidia = 0
    print 'Escrita de dados multimídia de %dMb no mongodb.' % (dataset/1048576)
    while contadorescritamultimidia < contadorgeral:
        Id_tempo = gravar_multimidia_mongodb("diretorio", str(dataset) + ".mp4")
        tempoescritamultimidia = tempoescritamultimidia + Id_tempo[1]
        contadorescritamultimidia = contadorescritamultimidia  + 1
    texto = texto + 'Gravação de %dMb de dados multimidia com tempo médio final de %.6f segundos.' % (dataset/1048576, tempoescritamultimidia/contadorescritamultimidia) + '\n'
    return texto

def consultar_dados_escalares_geral_mongodb(dataset, contadorgeral):
    texto = ""
    #Consulta de dado escalar
    contadorconsultaescalar = 0
    #Inicializador dos tempo de consulta escalar
    tempoconsultaescalar = 0
    colecao.insert({'temperatura' : 100})
    print 'Consulta de dados escalares em %dMb no mongodb.' % (dataset/1048576)
    while contadorconsultaescalar < contadorgeral:
        tempoconsultaescalar = tempoconsultaescalar + consultar_temperatura_mongodb(100)
        contadorconsultaescalar = contadorconsultaescalar + 1
    texto = texto + 'Consulta de chave 100 em %dMb de dados escalares com tempo médio final de %.6f segundos.' % (dataset/1048576, tempoconsultaescalar/contadorconsultaescalar) + '\n'
    texto = texto +  '--------------------------------------------------------------------------------------------------------------------------------\n'
    return texto

def consultar_dados_posicionais_geral_mongodb(dataset, contadorgeral):
    texto = ""
    #Inicializador contador de consultaposicional
    contadorconsultaposicional = 0
    #Inicializador dos tempos de consulta posicional
    tempoconsultaposicional = 0
    colecao.insert({'latitude' : '0° 0´N'})
    print 'Consulta de dados posicionais em %dMb no mongodb.' % (dataset/1048576)
    while contadorconsultaposicional < contadorgeral:
        tempoconsultaposicional = tempoconsultaposicional + consultar_posicao_mongodb('0° 0´N')
        contadorconsultaposicional = contadorconsultaposicional + 1
    texto = texto +  'Consulta de chave 0° 0´N em %dMb de dados posicionais com tempo médio final de %.6f segundos.' % (dataset/1048576, tempoconsultaposicional/contadorconsultaposicional) + '\n'
    texto = texto +  '--------------------------------------------------------------------------------------------------------------------------------\n'
    return texto

def consultar_dados_multimidia_geral_mongodb(dataset, contadorgeral):
    texto = ""
    #Inicializador contador de consulta multimidia
    contadorconsultamultimidia = 0
    #Inicializador contador de consulta multimidia
    tempoconsultamultimidia = 0
    #Exclui arquivos e chunks antes de continuar os testes multimídia
    banco.fs.chunks.drop()
    banco.fs.files.drop()
    Id_tempo = gravar_multimidia_mongodb("diretorio", str(dataset) + ".mp4")
    IdArquivo = Id_tempo[0]
    print 'Consulta de dados multimídia em %dMb no mongodb.' % (dataset/1048576)
    while contadorconsultamultimidia < contadorgeral:
        tempoconsultamultimidia = tempoconsultamultimidia + consultar_chunk_multimidia_mongodb(IdArquivo)
        contadorconsultamultimidia = contadorconsultamultimidia + 1
    texto = texto + 'Consulta de dado multimidia em %dMb de dados com tempo médio final de %.6f segundos.' % (dataset/1048576, tempoconsultamultimidia/contadorconsultamultimidia) + '\n'
    texto = texto + '--------------------------------------------------------------------------------------------------------------------------------\n'
    return texto

#################################
###  SCRIPT DE EXECUÇÃO GERAL ###
#################################


def limpar_dados_mongodb():
    #Limpa todos os dados antes de iniciar os testes
    colecao.remove({})
    banco.fs.chunks.drop()
    banco.fs.files.drop()

def executar_escritas_e_leituras_mongodb():
    t_geral = 0 #inicializa o tempo geral
    mensagem = ""
    
    #Inicio do experimento
    inicio_geral = timeit.default_timer()
    
    #Limpa todos os dados antes de iniciar os testes
    limpar_dados_mongodb()
        
    #datasets = [1048576, 2097152, 4194304, 8388608, 16777216, 33554432, 67108864]
    datasets = [2097152]

    #Contador geral de execuções
    contadorgeral = 10

    for dataset in datasets:
        
        #Escrita geral de dados escalares de acordo com o contadorgeral
        #mensagem = mensagem +  gravar_dados_escalares_geral_mongodb(dataset, contadorgeral)
        #Consulta geral de dados escalares de acordo com o contadorgeral
        #mensagem = mensagem + consultar_dados_escalares_geral_mongodb(dataset, contadorgeral)
        #Escrita geral de dados posicionais de acordo com o contadorgeral
        #mensagem = mensagem + gravar_dados_posicionais_geral_mongodb(dataset, contadorgeral)
        #Consulta geral de dados posicionais de acordo com o contadorgeral
        #mensagem = mensagem + consultar_dados_posicionais_geral_mongodb(dataset, contadorgeral)
        #Escrita geral de dados multimidia de acordo com o contadorgeral
        mensagem = mensagem + gravar_dados_multimidia_geral_mongodb(dataset, contadorgeral)
        #Consulta geral de dados multimidia de acordo com o contadorgeral
        #mensagem = mensagem + consultar_dados_multimidia_geral_mongodb(dataset, contadorgeral)
                
    #Marca o tempo final do experimento
    fim_geral = timeit.default_timer()
    t_geral = t_geral + (inicio_geral - fim_geral)/1000000
    tempo_geral = abs(t_geral) * 1000000
    
    mensagem = mensagem + 'Tempo total do experimento: %d segundos.\n' % tempo_geral
        
    #Escreve dados no arquivo
    nome_arquivo = "execucao_geral_mongodb.txt"
    if (os.path.exists(nome_arquivo)):
        os.remove(nome_arquivo)
        
    arquivo = open(nome_arquivo,'wb+')
    arquivo.write(mensagem)
    arquivo.close()
        
#################################
###  SCRIPT DE EXECUÇÃO GERAL ###
#################################
executar_escritas_e_leituras_mongodb()

'''banco.fs.chunks.drop()
banco.fs.files.drop()
Id_tempo = gravar_multimidia_mongodb("diretorio", str(67108864) + ".mp4")
IdArquivo = Id_tempo[0]
print IdArquivo
consultar_chunk_multimidia_mongodb('5b523a505236ae28753d0c5b')'''