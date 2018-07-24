# -*- coding: UTF-8 -*-

'''
Created on 12 de set de 2017

@author: braulio.junior
'''

import redis
import timeit
import math
import json
import os
from bson.binary import Binary
from Sensor import SensorIoT

r = redis.StrictRedis(host = '127.0.0.1', port = 6379, db = 0)

# Consulta dado escalar ou posicional
def buscar_dado_redis(chave):
    keys = r.keys('*')
    achou = 0
    tempo = 0
    for key in keys:
        inicio = timeit.default_timer()
        if key == chave:
            achou = 1
        fim = timeit.default_timer()
        t = (inicio - fim)/1000000
        tempo = tempo + abs(t)*1000000
        if achou == 1:
            #return 'Chave %s achada em %.6f segundos.' % (chave, tempo)
            return tempo
    #if achou == 0:
        #return 'Registro %s não localizado.' % str(chave)
    
def gravar_dados_escalar_redis(tam_dataset):
        t = 0
        cont = 1
        #print '>>> Inicio de geração de temperaturas em memoria <<<'
        temperaturas = SensorIoT.gerar_dados_escalares(tam_dataset)
        #print '>>> Fim de geração de temperaturas em memoria <<<'
        #print '>>> Inicio da gravação de temperaturas no redis <<<'
        for temperatura in temperaturas:
                
                inicio = timeit.default_timer()
            
                # Insere no redis
                #r.set("temperatura"+str(temperatura), str(temperatura))
                #r.set(str(temperatura), str(temperatura))
                r.set(temperatura, temperatura)
            
                fim = timeit.default_timer()
                t = t + (inicio - fim)/1000000
                #t = t + (inicio/1000000 - fim/1000000)
                cont += 1
                tempo = abs(t) * 1000000
                #print 'duração parcial da gravação no redis: %d segundos' % tempo
                #print '>>> Fim da gravação de temperaturas no redis <<<'

        return int(tempo)                        
        #return '%s temperaturas gravadas em %d segundos. ' % (len(temperaturas), tempo)
        #return '%s temperaturas gravadas em %d segundos. ' % (len(temperaturas)*contador, totaltempo/5)
        #print 'total de temperaturas gravadas no redis: %s' % len(temperaturas)
  
  
def gravar_dados_posicionais_redis(tam_dataset):
        t = 0
        cont = 1
        #print '>>> Inicio de geração de dados posicionais em memoria <<<'
        posicionais = SensorIoT.gerar_dados_posicionais(tam_dataset)
        #print '>>> Fim de geração de dados posicionais em memoria <<<'
        for posicional in posicionais:
            inicio = timeit.default_timer()
            
            # Insere no redis
            #r.set(str(cont), str(posicional))
            r.set(str(posicional), str(posicional))
            
            fim = timeit.default_timer()
            t = t + (inicio - fim)/1000000
            cont += 1
            tempo = abs(t) * 1000000
            #print 'duração parcial da gravação no redis: %d segundos' % tempo
        #print '>>> Fim de gravação de dados posicionais no redis <<<'
        #return '%s dados posicionais gravados em %d segundos. ' % (len(posicionais), tempo)
        return int(tempo)
        #print 'total de dados posicionais gravados no redis: %s' % len(posicionais)
        
def gravar_multimedia_redis(local, arquivo):
    
    local_e_arquivo = local + "/" + arquivo
    #print os.path.getsize(local_e_arquivo)
    tamanho = os.path.getsize(local_e_arquivo)
    tempototal = 0
    i = 0
    #chunk_size = tamanho
    chunk_size = 1024 #1 Kb
    with open(local_e_arquivo, 'rb') as infile:
        data = infile.read()
    c = 0
    for i in range(0, len(data), chunk_size):
        chunk = data[i:i+chunk_size]
        encoded = str(json.dumps(chunk, ensure_ascii=False, encoding='utf-8'))
        
        inicio = timeit.default_timer()
        
        r.set(c, encoded)
        
        fim = timeit.default_timer()
        t = (inicio - fim)/1000000
        tempo = abs(t)
        tempototal += tempo
        inicio = 0
        fim = 0
        c += 1 #contador 
        
    #print 'Arquivo multimedia de %.fMb gravado em %.9f segundos. ' % (tamanho/1048576, abs(tempototal) * 1000000)
    #print 'Arquivo multimedia de %.fMb gravado em %d segundos. ' % (tamanho/1048576, abs(tempototal) * 1000000)
    return abs(tempototal) * 1000000

def inserir_pedacos_multimidia(caminho):
    diretorio = os.listdir(caminho)
    for arquivo in diretorio:
        local_e_arquivo = caminho + "/" + arquivo
        with open(local_e_arquivo, 'rb') as a:
            f = a.read()
            encoded = Binary(f, 0)
            r.set(arquivo, encoded)            

def contar_registros():
    cont = 0
    keys = r.keys('*')
    for key in keys:
        cont += 1
    print cont

#################################
###  SCRIPT DE EXECUÇÃO GERAL ###
#################################
def executar_escritas_e_leituras_redis():
    t_geral = 0 #inicializa o tempo geral
    
    #Inicio do experimento
    inicio_geral = timeit.default_timer()

    #Limpa todos os dados antes de iniciar os testes
    r.flushall()
    
    mensagem = ""
    
    datasets = [1048576, 2097152, 4194304, 8388608, 16777216, 33554432, 67108864]
    #datasets = [1048576]

    #Contador geral de execuções
    contadorgeral = 10

    for dataset in datasets:
        #contador do número de escritas
        contadorescrita = 0
        #Inicializador dos tempos de escrita escalares/posicionais
        tempoescalar = 0
        tempoposicional = 0
        #Insere 25x dados escalares e posicionais e pega o tempo médio
        print 'Gravação de dados escalares e posicionais de %dMb no redis.' % (dataset/1048576)
        while contadorescrita < contadorgeral:
            tempoescalar = tempoescalar + gravar_dados_escalar_redis(dataset)
            tempoposicional = tempoposicional + gravar_dados_posicionais_redis(dataset)
            #print  'tempo parcial de %d segundos. ' % (tempo)
            contadorescrita = contadorescrita + 1
        mensagem = mensagem +  'Gravação de %dMb de dados escalares com tempo médio final de %d segundos.' % (dataset/1048576, tempoescalar/contadorescrita) + '\n'
        mensagem = mensagem +  'Gravação de %dMb de dados posicionais com tempo médio final de %d segundos.' % (dataset/1048576, tempoposicional/contadorescrita) + '\n'
    
        #Consultas de dados escalares/posicionais
        tempoconsultaescalar = 0
        contadorconsultaescalar = 0
        tempoconsultaposicional = 0
        contadorconsultaposicional = 0
    
        r.set(100, 100)
        print 'Consulta de dados escalares de %dMb no redis.' % (dataset/1048576)
        while contadorconsultaescalar < contadorgeral:
            tempoconsultaescalar = tempoconsultaescalar + buscar_dado_redis("100")
            contadorconsultaescalar = contadorconsultaescalar + 1
        mensagem = mensagem +  'Consulta da chave 100 em %dMb de dados escalares com tempo médio final %.6f segundos.' % (dataset/1048576, tempoconsultaescalar/contadorconsultaescalar) + '\n'
    
        r.set("0° 0´N", "0° 0´N")
        print 'Consulta de dados posicionais de %dMb no redis.' % (dataset/1048576)
        while contadorconsultaposicional < contadorgeral:
            tempoconsultaposicional = tempoconsultaposicional  + buscar_dado_redis("0° 0´N")
            contadorconsultaposicional = contadorconsultaposicional + 1
        mensagem = mensagem +  'Consulta da chave 0° 0´N em %dMb de dados posicionais com tempo médio final %.6f segundos.' % (dataset/1048576, tempoconsultaposicional/contadorconsultaposicional) + '\n'
        mensagem = mensagem +  '-------------------------------------------------------------------------------------------------------------------------------------------------\n'
        
        #Escrita multimídia
        contadorescritamultimidia = 0
        tempoescritamultimidia = 0
        print 'Gravação de dados multimídia de %dMb no redis.' % (dataset/1048576)
        while contadorescritamultimidia < contadorgeral:
            tempoescritamultimidia = tempoescritamultimidia + gravar_multimedia_redis("diretorio", str(dataset) + ".mp4")
            contadorescritamultimidia = contadorescritamultimidia + 1
        mensagem = mensagem +  'Gravação de %dMb de dados multimidia com tempo médio final de %.6f segundos.' % (dataset/1048576, tempoescritamultimidia/contadorescritamultimidia) + '\n'    
        
        #Consulta multimídia
        contadorconsultamultimidia = 0
        tempoconsultamultimidia = 0
        r.rename("0", "teste")
        print 'Consulta de dados multimídia de %dMb no redis.' % (dataset/1048576)
        while contadorconsultamultimidia < contadorgeral:
            tempoconsultamultimidia = tempoconsultamultimidia + buscar_dado_redis("teste")
            contadorconsultamultimidia = contadorconsultamultimidia + 1
        mensagem = mensagem +  'Consulta em %dMb de dados multimidia com tempo médio final %.6f segundos.' % (dataset/1048576, tempoconsultamultimidia/contadorconsultamultimidia) + '\n'
        mensagem = mensagem +  '-------------------------------------------------------------------------------------------------------------------------------------------------\n'
        print '----------------------------------------------------------------------'
        
        #Limpa todos os dados antes de pular para os testes do próximo dataset
        r.flushall()
    
    #Marca o tempo final do experimento
    fim_geral = timeit.default_timer()
    t_geral = t_geral + (inicio_geral - fim_geral)/1000000
    tempo_geral = abs(t_geral) * 1000000
    
    mensagem = mensagem + 'Tempo total do experimento: %d segundos.\n' % tempo_geral
    
    #print mensagem
    
    #Escreve dados no arquivo
    nome_arquivo = "execucao_geral_redis.txt"
    if (os.path.exists(nome_arquivo)):
        os.remove(nome_arquivo)
    
    arquivo = open(nome_arquivo,'wb+')
    arquivo.write(mensagem)
    arquivo.close()
    
#################################
###  SCRIPT DE EXECUÇÃO GERAL ###
#################################
# 1 - Configurar e executar todos os datasets em memória (gerar os gráficos e variaveis estatisticas)
# 2 - Configurar e executar todos os datasets em disco (gerar os gráficos e variaveis estatisticas)
#########################
executar_escritas_e_leituras_redis()