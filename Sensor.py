# -*- coding: UTF-8 -*-
'''
Created on 19 de ago de 2017

@author: braulio.junior
'''

from random import randint
import timeit
import os
import sys

class SensorIoT(object):
    def __init__(self, temperatura):
        self.temperatura = temperatura
        
    def obter_temperatura(self):
        return self.temperatura
    
    @staticmethod
    def gerar_temperaturas():
        i = 0
        numeros = []
        while (i < 100):
            numeros.append(str(randint(0, 100)).strip(" "))
            i+=1
        return numeros
    
    @staticmethod
    def gravar_temperaturas_arquivo(nome_arquivo):
        if (os.path.exists(nome_arquivo)):
            arquivo = open(nome_arquivo,'ab+')
        else:
            arquivo = open(nome_arquivo,'wb+')
        # Gera um Dataset de 50MB
        tamanho = 0
        temperaturas = []
        print '>>> Iniciando grava��o de Dataset <<<'
        while (tamanho <= 1048576):
        #while (tamanho <= 10000):
            tamanho = SensorIoT.obter_tamanho_arquivo_temperaturas(nome_arquivo)   
            print 'Tamanho parcial do Dataset: %s Bytes' % tamanho
            #temperaturas = SensorIoT.gerar_temperaturas()
            temperaturas.append(str(randint(0, 100)).strip(" ")+',')
            arquivo.write(''.join(temperaturas))
        print '>>> Grava��o de Dataset finalizada <<<'
        print 'Tamanho do Dataset: %s Bytes' % tamanho
        arquivo.close()
    
    @staticmethod
    def gravar_numeros_grafico_arquivo():
        cont = 0
        numeros = []
        for cont in range (0, 36000, 500):
            numeros.append(str(cont) + "\n")
            cont += 500
        
        numeros_str = ''.join(numeros)
        
        print numeros_str
    
    @staticmethod    
    def gerar_dados_escalares(tam_dataset):
        tamanho = 0
        temperaturas = []
        while (tamanho <= tam_dataset):
            tamanho = sys.getsizeof(temperaturas) 
            temperaturas.append(randint(0, 99))
        return temperaturas
    
    @staticmethod    
    def gerar_dados_posicionais(tam_dataset):
        tamanho = 0
        latitudes = []
        while (tamanho <= tam_dataset):
            tamanho = sys.getsizeof(latitudes) 
            latitudes.append( str(randint(1, 100)) + '° ' + str(randint(1, 100))+'´N')
        return latitudes
            
    @staticmethod
    def obter_tamanho_arquivo_temperaturas(nome_arquivo):
        tamanho = os.path.getsize(nome_arquivo)
        return tamanho
    
    @staticmethod
    def obter_temperaturas(nome_arquivo):
        arquivo = open(nome_arquivo,'rb')
        linha = arquivo.readline()
        t = 0
        print '>>> Lendo temperaturas e guardando na coleção <<<'
        while linha:
            inicio = timeit.default_timer()
            valores = linha.strip(" ").split(',')
            fim = timeit.default_timer()
            t = t + (inicio - fim)/1000000
            print ('duracao parcial da gravação na coleção: %f segundos' % t)
        arquivo.close()
        print '>>> Coleção de temperaturas finalizada <<<'
        print ('Duração da gravação na coleção: %f minutos' % t/60)
        return valores
    
# Trabalhando com arquivo para manipular dados escalares
#SensorIoT.gravar_temperaturas_arquivo('temperaturas_bin')
# print SensorIoT.obter_temperaturas('temperaturas.txt')
#print SensorIoT.obter_tamanho_arquivo_temperaturas('temperaturas.txt')+'KB'

#Temperaturas = SensorIoT.gerar_dados_escalares()
#print type(Temperaturas)

#SensorIoT.gravar_numeros_grafico_arquivo()