# -*- coding: UTF-8 -*-
'''
Created on 17 de set de 2017

@author: braulio.junior
'''

from neo4j.v1 import GraphDatabase, basic_auth
from Sensor import SensorIoT
import timeit
import math

# Abre a conexao
uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=basic_auth("neo4j", "1234"))
session = driver.session()

def gravar_dados_escalar_neo4j(tam_dataset):
        t = 0
        #cont = 0
        
        #print '>>> Inicio de geracao de temperaturas em memoria <<<'
        temperaturas = SensorIoT.gerar_dados_escalares(tam_dataset)
        #print '>>> Fim de geracao de temperaturas em memoria <<<'
        
        # Obtem total de nos
        resultado = session.run('match (sensor) where sensor:temperatura return count(*)')
        cont = resultado.single()[0]
        cont += 1
        
        for temperatura in temperaturas:
            inicio = timeit.default_timer()
            
            # insere um novo noh no neo4j
            session.run("CREATE (sensor:temperatura {id: {id}, temperatura: {temperatura}})",
                        {"id": cont, "temperatura": temperatura})
            
            fim = timeit.default_timer()
            t = t + (inicio - fim)/1000000
            tempo = abs(t) * 1000000
            cont += 1
            #print 'duracao parcial da gravacao no neo4j: %f segundos' % t
        #print '>>> Fim de gravacao de temperaturas no neo4j <<<'
        return '%s temperaturas gravadas em %d segundos. ' % (len(temperaturas), tempo)
        # Fecha conexao
        #session.close()
        
def gravar_dados_posicional_neo4j():
        t = 0
                
        print '>>> Inicio de geracao de latitudes em memoria <<<'
        latitudes = SensorIoT.gerar_dados_posicionais()
        print '>>> Fim de geracao de latitudes em memoria <<<'
        
        # Obtem total de nos
        resultado = session.run('match (sensor) where sensor:sp return count(*)')
        cont = resultado.single()[0]
        cont += 1
        
        for latitude in latitudes:
            inicio = timeit.default_timer()
            
            # insere um novo noh no neo4j
            session.run("CREATE (sensor:sp {id: {id}, latitude: {latitude}})",
                        {"id": cont, "latitude": latitude})
            
            fim = timeit.default_timer()
            t = t + (inicio - fim)/1000000
            cont += 1
            print 'duracao parcial da gravacao no neo4j: %f segundos' % t
        print '>>> Fim de gravacao de latitudes no neo4j <<<'
        t = t/60
        print 'duracao da gravacao no neo4j: %f minutos' % t
        print 'total de latitudes gravadas no neo4j: %s' % len(latitudes)
        # Fecha conexao
        session.close()

def buscar_dado_neo4j(chave):
    inicio = timeit.default_timer()
    res = session.run("match (sensor) where sensor:st and sensor.temperatura = 0 return count(*)")
    fim = timeit.default_timer()
    t = (inicio - fim)/1000000
    tempo = abs(t)
        
    if res.single()[0] > 0:
        return 'Chave %s achada em %f segundos.' % (chave, tempo)
    else:
        return 'Registro s% não localizado.' % str(chave)

# Gravar dados escalares neo4j
#gravar_dados_escalar_neo4j()

# Gravar dados posicionais neo4j
#gravar_dados_posicional_neo4j()


#datasets = [1048576, 2097152, 4194304, 8388608, 16777216, 33554432]
datasets = [1048576]
mensagem = ""
#################################
for dataset in datasets:
    '''r = session.run('match (sensor) where sensor:st return max(sensor.id)')
    max1 = r.single()[0]'''
    
    mensagem = mensagem + 'Dataset de %dMB: ' % math.floor(dataset/1048576)
    mensagem = mensagem + gravar_dados_escalar_neo4j(dataset)
    
    '''result = session.run('match (sensor) where sensor:st return count(*)')
    cont = result.single()[0]
    metade = int(cont/2)
    session.run("MATCH (n { id: {metade} }) SET n.temperatura = 0", {"metade": metade})
    
    mensagem = mensagem + buscar_dado_neo4j('0')'''
print mensagem
    
#################################