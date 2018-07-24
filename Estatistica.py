# -*- coding: UTF-8 -*-
'''
Created on 23 de jul de 2018

@author: braulio.junior
'''

def calcularDiferencaPercentual():
    
    grupo = [ [0.0015, 0.0083, 0.005],  
                    [0.0009, 0.0005, 0.006], 
                    [0.0013, 0.0006, 0.011], 
                    [0.0008, 0.0008, 0.017], 
                    [0.0011, 0.0008, 0.030], 
                    [0.0041, 0.0006, 0.105],
                    [0.0095, 0.0103, 0.429] ]
    
    #Formula: ((v2/v1) − 1) ∗ 100
    #Calculo da diferença percentual entre redis [0] e mongodb [1]
    x = 0
    for c in range(0, 7):
        x = x + ((grupo[c][1] / grupo[c][0]) - 1) * 100
    
    #Calculo da diferença percentual entre redis [0] e cassandra [2]
    y = 0
    for c in range(0, 7):
        y = y + ((grupo[c][2] / grupo[c][0]) - 1) * 100
    
    print "O Redis/MongoDB obteve em média %d%% de desempenho superior que o Redis/MongoDB e %d%% superior ao Cassandra." %(x/7, y/7)
    
calcularDiferencaPercentual()
    