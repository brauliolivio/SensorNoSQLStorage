# -*- coding: UTF-8 -*-
'''
Created on 13 de set de 2017

@author: braulio.junior
'''

from cassandra.cluster import Cluster
from Sensor import SensorIoT
import timeit
import json
import os
from bson.binary import Binary

# Conexao
cluster = Cluster()
# Conecta ao keyspace 'dadossensoriot'
session = cluster.connect('dadossensoriot')

# Ajusta o timeout de operacoes para 5h
session.default_timeout = 18000
session.default_fetch_size = 50

def gravar_dados_escalar_cassandra(tam_dataset):
        t = 0
        
        #print '>>> Inicio de geracao de temperaturas em memoria <<<'
        temperaturas = SensorIoT.gerar_dados_escalares(tam_dataset)
        #print '>>> Fim de geracao de temperaturas em memoria <<<'
        
        # Obtem total de linhas da tabela
        #resultado = session.execute('select count(id) from temperaturas', timeout = 10800)
        #resultado = session.execute('select count(*) from temperaturas')
        #cont = resultado[0].count
        cont = 0
        
        for temperatura in temperaturas:
            inicio = timeit.default_timer()
            
            # Insere no cassandra            
            #session.execute("insert into temperaturas (id, temperatura) values (%s, %s)", (cont, temperatura))
            session.execute("insert into temperaturasdisk (id, temperatura) values (%s, %s)", (cont, temperatura))
            
            fim = timeit.default_timer()
            t = t + (inicio - fim)/1000000
            tempo = abs(t) * 1000000
            cont += 1
            #print 'duracao parcial da gravacao no cassandra: %f segundos' % t
        #print '>>> Fim de gravacao de temperaturas no cassandra <<<'
        #return '%s temperaturas gravadas em %d segundos. ' % (len(temperaturas), tempo)
        return tempo
        #print 'total de temperaturas gravadas no cassandra: %s' % len(temperaturas)
        
def gravar_dados_posicionais_cassandra(tam_dataset):
        t = 0
        #print '>>> Inicio de geracao de latitudes em memoria <<<'
        latitudes = SensorIoT.gerar_dados_posicionais(tam_dataset)
        #print '>>> Fim de geracao de latitudes em memoria <<<'
        
        # Obtem total de linhas da tabela
        '''resultado = session.execute('select count(*) from latitudes')
        cont = resultado[0].count'''
        cont = 0
        
        for latitude in latitudes:
            inicio = timeit.default_timer()
            
            # Insere no cassandra            
            #session.execute("insert into latitudes (id, latitude) values (%s, %s)", (cont, latitude))
            session.execute("insert into latitudesdisk (id, latitude) values (%s, %s)", (cont, latitude))
            
            fim = timeit.default_timer()
            t = t + (inicio - fim)/1000000
            tempo = abs(t) * 1000000
            cont += 1
            #print 'duracao parcial da gravacao no cassandra: %f segundos' % t
        #print '>>> Fim de gravacao de latitudes no cassandra <<<'
        #return '%s dados posicionais gravados em %d segundos. ' % (len(latitudes), tempo)
        return tempo
        #print 'total de latitudes gravadas no cassandra: %s' % len(latitudes)

def gravar_multimedia_cassandra(local, arquivo):
    
    local_e_arquivo = local + "/" + arquivo
    #print os.path.getsize(local_e_arquivo)
    #tamanho = os.path.getsize(local_e_arquivo)
    tempototal = 0
    #chunk_size = tamanho
    chunk_size = 1024
    with open(local_e_arquivo, 'rb') as infile:
        data = infile.read()
    for i in range(0, len(data), chunk_size):
        chunk = data[i:i+chunk_size]
        encoded = str(json.dumps(chunk, ensure_ascii=False, encoding='utf-8'))
        
        params = [i, bytearray(encoded), arquivo, i]
        inicio = timeit.default_timer()
        session.execute("INSERT INTO multimediadisk (id, file, filename, splitno) VALUES (%s, %s, %s, %s)", params)
        fim = timeit.default_timer()
        t = (inicio - fim)/1000000
        tempo = abs(t) * 1000000
        tempototal += tempo
        inicio = 0
        fim = 0 
        
    #print 'Arquivo multimedia de %.fMb gravado em %.9f segundos. ' % (tamanho/1048576, tempototal)
    return tempototal
    
def inserir_pedacos_multimidia(diretorio):
    diretorio = os.listdir(diretorio)
    cont = 0
    for arquivo in diretorio:
        local_e_arquivo = diretorio + "/" + arquivo
        with open(local_e_arquivo, 'rb') as a:
            f = a.read()
            encoded = Binary(f, 0)
            params = [cont, bytearray(encoded), arquivo, 0]
            session.execute("INSERT INTO multimediadisk (id, file, filename, splitno) VALUES (%s, %s, %s, %s)", params)
            cont += 1      

def buscar_dado_multimedia_cassandra(arquivo, max_chunk):
    consulta = "select count(*) from multimediadisk where filename = "  + "'" + str(arquivo) + "'" + " and splitno = " + str(max_chunk) + " allow filtering;"
    inicio = timeit.default_timer()
    con = session.execute(consulta)
    fim = timeit.default_timer()
    t = (inicio - fim)/1000000
    tempo = abs(t) * 1000000
    if con[0].count > 0:
        #return 'Chave %s achada em %.6f segundos.' % (arquivo, tempo)
        return tempo
    #else:
        #return 'Registro %s não localizado.' % str(arquivo)
    
    #with open(local_e_arquivo, 'rb') as a:
        #f = a.read()
        #encoded = str(json.dumps(f, ensure_ascii=False, encoding='utf-8'))
        #encoded = f.encode('utf-8', 'ignore')
    
    # Insere no cassandra
    # params = [0, bytearray(encoded)]
    # session.execute("INSERT INTO multimedia (id, file) VALUES (%s, %s)", params)
        
        
# Consulta dado escalar ou posicional
def buscar_dado_cassandra(chave):
    banco = ""
    tipo = ""
    consulta = ""
    if '°' in chave:
        banco = "latitudesdisk" 
        tipo = "latitude"
        consulta = "select count(*) from " + banco + " where " + tipo + " = " + "'" + str(chave) + "'" + " ALLOW FILTERING"
    else:
        banco = "temperaturasdisk" 
        tipo = "temperatura"
        consulta = "select count(*) from " + banco + " where " + tipo + " = " + chave + " ALLOW FILTERING"
    
    inicio = timeit.default_timer()
    con = session.execute(consulta)
    fim = timeit.default_timer()
    t = (inicio - fim)/1000000
    tempo = abs(t) * 1000000
    
    if con[0].count > 0:
        #return 'Chave %s achada em %.9f segundos.' % (chave, tempo)
        return tempo
    #else:
        #return 'Registro %s não localizado.' % str(chave)

# Gravar dados escalares no Cassandra
#gravar_dados_escalar_cassandra()

# Gravar dados posicionais no Cassandra
#gravar_dados_posicionais_cassandra()

#############################
# Datasets de 1048576, 2097152, 4194304, 8388608, 16777216, 33554432, 67108864 bytes
#datasets = [1048576]
#mensagem = ""
'''for dataset in datasets:
    #session.execute('truncate table temperaturas')
    #mensagem = mensagem + 'Dataset de %dMB: ' % math.floor(dataset/1048576)
    #mensagem = mensagem + gravar_dados_escalar_cassandra(dataset)
    #session.execute('delete from temperaturasdisk where temperatura = 100')
    result = session.execute('select count(*) from temperaturas allow filtering')
    cont = result[0].count
    fim = cont + 1
    session.execute("insert into temperaturas (id, temperatura) values (%s, %s)", (fim, 100))
    #atualizacao = "update temperaturas set temperatura = 0 where id = " + str(metade)
    #session.execute(atualizacao)
    mensagem = mensagem + buscar_dado_cassandra("100", dataset) + '\n'
print mensagem'''

# Datasets de 1048576, 2097152, 4194304, 8388608, 16777216, 33554432, 67108864 bytes
'''datasets = [1048576]
mensagem = ""
for dataset in datasets:
    #session.execute('truncate table latitudes')
    #mensagem = mensagem + 'Dataset de %dMB: ' % math.floor(dataset/1048576)
    #mensagem = mensagem + gravar_dados_posicionais_cassandra(dataset)
    #session.execute('delete from temperaturas where temperatura = 0')
    result = session.execute('select count(*) from latitudes allow filtering')
    cont = result[0].count
    fim = cont + 1
    session.execute("insert into latitudes(id, latitude) values (%s, %s)", (fim, "0° 0´N"))
    #atualizacao = "update latitudes set latitude = '0° 0´N' where id = " + str(metade)
    #session.execute(atualizacao)
    mensagem = mensagem + buscar_dado_cassandra("0° 0´N", dataset) + '\n'
print mensagem'''
#############################


#inicio = timeit.default_timer()
#gravar_multimedia_cassandra("diretorio/nome_arquivo")
#gravar_multimedia_cassandra("diretorio/nome_arquivo")
#gravar_multimedia_cassandra("diretorio", "nome_arquivo")
#print gravar_dados_posicionais_cassandra(tam_dataset)
#print buscar_dado_cassandra("0", tam_dataset)
#print buscar_dado_cassandra("0° 0´N", tam_dataset)
#session.execute('truncate table latitudes')
#inserir_pedacos_multimidia("diretorio")
#print buscar_dado_multimedia_cassandra('teste')

def gravar_dados_escalares_geral_cassandra(dataset, contadorgeral):
    texto = ""
    #contador do número de escritas escalares
    contadorescritaescalar = 0
    #Inicializador dos tempos de escritas escalares
    tempoescalar = 0
    #Insere os dados escalares no número de vezes do contadorgeral e pega o tempo médio
    print 'Gravação de dados escalares de %dMb no Cassandra.' % (dataset/1048576)
    while contadorescritaescalar < contadorgeral:
        print '%dª execução de escrita escalar' % (contadorescritaescalar+1)
        tempoescalar = tempoescalar + gravar_dados_escalar_cassandra(dataset)
        contadorescritaescalar = contadorescritaescalar + 1
        #Não apaga a última execução para poder permitir as consultas
        if contadorescritaescalar < contadorgeral:
            session.execute('truncate table temperaturasdisk')
    texto = texto + 'Gravação de %dMb de dados escalares com tempo médio final de %d segundos.' % (dataset/1048576, tempoescalar/contadorescritaescalar) + '\n'
    texto = texto + '---------------------------------------------------------------------------------------------------------------\n'
    print '---------------------------------------------------------------------------------------------------------------'
    return texto

def gravar_dados_posicionais_geral_cassandra(dataset, contadorgeral):
    texto = ""
    #contador do número de escritas posicionais
    contadorescritaposicional = 0
    #Inicializador dos tempos de escritas posicionais
    tempoposicional = 0
    #Insere os dados posicionais no número de vezes do contadorgeral e pega o tempo médio
    print 'Gravação de dados posicionais de %dMb no Cassandra.' % (dataset/1048576)
    while contadorescritaposicional < contadorgeral:
        print '%dª execução de escrita posicional' % (contadorescritaposicional+1)
        tempoposicional = tempoposicional + gravar_dados_posicionais_cassandra(dataset)
        contadorescritaposicional = contadorescritaposicional + 1
        if contadorescritaposicional < contadorgeral:
            session.execute('truncate table latitudesdisk')
    texto = texto +  'Gravação de %dMb de dados posicionais com tempo médio final de %d segundos.' % (dataset/1048576, tempoposicional/contadorescritaposicional) + '\n'
    texto = texto + '---------------------------------------------------------------------------------------------------------------\n'
    print '---------------------------------------------------------------------------------------------------------------'
    return texto

def gravar_dados_multimidia_geral_cassandra(dataset, contadorgeral):
    texto = ""
    #contador do número de escritas multimídia
    contadorescritamultimidia = 0
    #Inicializador dos tempos de escritas multimídia
    tempoescritamultimidia = 0
    print 'Gravação de dados multimídia de %dMb no Cassandra.' % (dataset/1048576)
    while contadorescritamultimidia < contadorgeral:
        print '%dª execução de escrita multimídia' % (contadorescritamultimidia+1)
        tempoescritamultimidia = tempoescritamultimidia + gravar_multimedia_cassandra("diretorio", str(dataset) + ".mp4")
        contadorescritamultimidia = contadorescritamultimidia + 1
        if contadorescritamultimidia < contadorgeral:
            session.execute('truncate table multimediadisk')
    texto = texto + 'Gravação de %dMb de dados multimidia com tempo médio final de %d segundos.' % (dataset/1048576, tempoescritamultimidia/contadorescritamultimidia) + '\n'
    texto = texto + '----------------------------------------------------------------------------------------------------------------\n'
    return texto
      
def consultar_dados_escalares_geral_cassandra(dataset, contadorgeral):
    texto = ""
    #Inicializador dos tempos de consultas escalares
    tempoconsultaescalar = 0
    #contador do número de consultas escalares
    contadorconsultaescalar = 0
    #Consultas de dados escalares
    fim_escalar = dataset/1024
    session.execute("insert into temperaturasdisk (id, temperatura) values (%s, %s)", (fim_escalar, 100))
    print 'Consulta de dados escalares de %dMb no Cassandra.' % (dataset/1048576)
    while contadorconsultaescalar < contadorgeral:
        print '%dª execução de consulta escalar' % (contadorconsultaescalar+1)
        tempoconsultaescalar = tempoconsultaescalar + buscar_dado_cassandra("100")
        contadorconsultaescalar = contadorconsultaescalar + 1
    texto = texto + 'Consulta da chave 100 em %dMb de dados escalares com tempo médio final %.6f segundos.' % (dataset/1048576, tempoconsultaescalar/contadorconsultaescalar) + '\n'
    texto = texto + '-----------------------------------------------------------------------------------------------------------------------------\n'
    print '-----------------------------------------------------------------------------------------------------------------------------'
    return texto
    
def consultar_dados_posicionais_geral_cassandra(dataset, contadorgeral):
    texto = ""
    #Inicializador dos tempos de consultas posicionais
    tempoconsultaposicional = 0
    #contador do número de consultas posicionais
    contadorconsultaposicional = 0
    #Consultas de dados posicionais
    fim_posicional = dataset/1024
    session.execute("insert into latitudesdisk(id, latitude) values (%s, %s)", (fim_posicional, "0° 0´N"))
    print 'Consulta de dados posicionais de %dMb no Cassandra.' % (dataset/1048576)
    while contadorconsultaposicional < contadorgeral:
        print '%dª execução de consulta posicional' % (contadorconsultaposicional+1)
        tempoconsultaposicional = tempoconsultaposicional  + buscar_dado_cassandra("0° 0´N")
        contadorconsultaposicional = contadorconsultaposicional + 1
    texto = texto +  'Consulta da chave 0° 0´N em %dMb de dados posicionais com tempo médio final %.6f segundos.' % (dataset/1048576, tempoconsultaposicional/contadorconsultaposicional) + '\n'
    texto = texto + '-----------------------------------------------------------------------------------------------------------------------------------\n'
    print '-----------------------------------------------------------------------------------------------------------------------------------'
    return texto

def consultar_dados_multimidia_geral_cassandra(dataset, contadorgeral):
    texto = ""
    #Inicializador dos tempos de consultas multimídia
    contadorconsultamultimidia = 0
    #contador do número de consultas multimídia
    tempoconsultamultimidia = 0
    max_chunk = dataset/1024
    gravar_multimedia_cassandra("diretorio", str(dataset) + ".mp4")
    print 'Consulta de dados multimídia de %dMb no Cassandra.' % (dataset/1048576)
    while contadorconsultamultimidia < contadorgeral:
        print '%dª execução de consulta multimídia' % (contadorconsultamultimidia+1)
        tempoconsultamultimidia = tempoconsultamultimidia + buscar_dado_multimedia_cassandra(str(dataset)+".mp4", max_chunk)
        contadorconsultamultimidia = contadorconsultamultimidia + 1
    texto = texto + 'Consulta em %dMb de dados multimidia com tempo médio final %.6f segundos.' % (dataset/1048576, tempoconsultamultimidia/contadorconsultamultimidia) + '\n'
    texto = texto + '------------------------------------------------------------------------------------------------------------\n'
    print '------------------------------------------------------------------------------------------------------------'
    return texto

def executar_escritas_e_leituras_cassandra():
    #inicializa o tempo geral
    t_geral = 0 
    #Inicio do experimento
    inicio_geral = timeit.default_timer()
    #datasets = [1048576, 2097152, 4194304, 8388608, 16777216, 33554432, 67108864]
    #datasets = [67108864, 33554432, 16777216, 8388608, 4194304, 2097152, 1048576]
    datasets = [67108864]
    #Contador geral de execuções
    contadorgeral = 10
    #Texto para arquivo com os resultados
    mensagem = ""
    
    #Limpa dados antes dos testes
    #session.execute('truncate table temperaturasdisk')
    session.execute('truncate table latitudesdisk')
    session.execute('truncate table multimediadisk')

    for dataset in datasets:
        
        #Escrita geral de dados escalares de acordo com o contadorgeral
        #mensagem = mensagem + gravar_dados_escalares_geral_cassandra(dataset, contadorgeral)
        #Consulta geral de dados escalares de acordo com o contadorgeral    
        mensagem = mensagem + consultar_dados_escalares_geral_cassandra(dataset, contadorgeral)
        
        #Escrita geral de dados posicionais de acordo com o contadorgeral
        #mensagem = mensagem + gravar_dados_posicionais_geral_cassandra(dataset, contadorgeral)
        #Consulta geral de dados posicionais de acordo com o contadorgeral    
        #mensagem = mensagem + consultar_dados_posicionais_geral_cassandra(dataset, contadorgeral)
        
        #Escrita geral de dados multimídia de acordo com o contadorgeral
        #mensagem = mensagem + gravar_dados_multimidia_geral_cassandra(dataset, contadorgeral)
        #Consulta geral de dados multimídia de acordo com o contadorgeral    
        #mensagem = mensagem + consultar_dados_multimidia_geral_cassandra(dataset, contadorgeral)
        
        
    #Marca o tempo final do experimento
    fim_geral = timeit.default_timer()
    t_geral = t_geral + (inicio_geral - fim_geral)/1000000
    tempo_geral = abs(t_geral) * 1000000
    
    mensagem = mensagem + 'Tempo total do experimento: %d segundos.\n' % tempo_geral
    
    #Escreve dados no arquivo
    nome_arquivo = "execucao_geral_cassandra.txt"
    if (os.path.exists(nome_arquivo)):
        os.remove(nome_arquivo)
    
    arquivo = open(nome_arquivo,'wb+')
    arquivo.write(mensagem)
    arquivo.close()
    
#########################
###  SCRIPT DE EXECUÇÃO GERAL ###
#########################
# 1 - Configurar e executar todos os datasets em memória (gerar os gráficos e variaveis estatisticas)
# sudo dse-5.1.7/bin/dse cassandra -R
# 2 - Configurar e executar todos os datasets em disco (gerar os gráficos e variaveis estatisticas)
#########################
executar_escritas_e_leituras_cassandra()
