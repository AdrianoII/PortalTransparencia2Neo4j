#!/usr/bin/env python
# coding: utf-8

# # Adriano Marcelo Vilarga Corbelino II 201711310023

import pandas as pd
from neo4j import GraphDatabase
from string import Template
import re
import time
import os
import argparse

comeco = time.time()

parser = argparse.ArgumentParser(description='Insere os dados do portal da transparência no Neo4j')
parser.add_argument('path_csv', type=str, help='Path absoluto para os csv\'s')
parser.add_argument('-u','-U','--user',dest='user',type=str,default='neo4j',help='Usuário utilizado para se conectar como neo4j(default: neo4j)')
parser.add_argument('-p','-P','--password',dest='pswd',type=str,default='neo4j',help='Senha utilzada para se conectar como neo4j(default: neo4j)')
parser.add_argument('-c','-C','--commit',dest='commit',type=str,default='',help='Número que define o a taxa em que os commits aconteceram, caso ocorra erros relacionados a memória tente um numero menor(default: 1000)')
args = parser.parse_args()

tempoCsv = time.time()

#Renomeia os arquivos com nome com caracteres estranhos
arqs = os.listdir(args.path_csv)

for arq in arqs:
  if(arq == '201908_ItemLicitaá∆o.csv'):
    os.rename(args.path_csv +'201908_ItemLicitaá∆o.csv',args.path_csv + '201908_ItemLicitacao.csv')
  elif(arq == '201908_Licitaá∆o.csv'):
    os.rename(args.path_csv +'201908_Licitaá∆o.csv',args.path_csv + '201908_Licitacao.csv')
  elif(arq == '201908_ParticipantesLicitaá∆o.csv'):
    os.rename(args.path_csv +'201908_ParticipantesLicitaá∆o.csv',args.path_csv + '201908_ParticipantesLicitacao.csv')


#Carrego todos os csv's que serão utilizados, somente com as informações necessárias
pessoa = pd.read_csv( args.path_csv + '201908_Cadastro.csv',';',encoding='latin1',dtype = str, usecols = ['NOME','CPF','MATRICULA','DESCRICAO_CARGO','SITUACAO_VINCULO','COD_ORG_EXERCICIO','ORG_EXERCICIO'])
pessoa.to_csv(args.path_csv + '/Pessoa.csv')

empresa = pd.read_csv( args.path_csv + '201908_CNPJ.csv',';',encoding='latin1',dtype = str, usecols = ['CNPJ','NOMEFANTASIA','RAZAOSOCIAL'])
#Preencho as células da coluna NOMEFANTASIA que não possuiam valor com " ",
#pois o merge não aceita um valor nulo
empresa['NOMEFANTASIA'] = empresa['NOMEFANTASIA'].fillna(" ")
empresa['RAZAOSOCIAL'] = empresa['RAZAOSOCIAL'].fillna(" ")
empresa.to_csv(args.path_csv + '/Empresa.csv')
del empresa



socio = pd.read_csv( args.path_csv + '201908_Socios.csv',';',encoding='latin1',dtype = str, usecols = ['Nome','CPF-CNPJ','CNPJ','Tipo'])
socio.rename(columns = {'CPF-CNPJ':'CPF_CNPJ'}, inplace=True)
#Realizo joins para diminuir o número de linhas das tabelas e conseguir outras informações casos precise
socioPessoa = pd.merge(left=pessoa, right=socio,left_on=['CPF'], right_on=['CPF_CNPJ'], how='inner')
#Retiro a mascara do CPNJ e deixo somente as linhas com CNPJ numéricos
socioEmpresa = socio.copy()
socioEmpresa['CPF_CNPJ'] = socioEmpresa['CPF_CNPJ'].apply( lambda x : re.sub(r"\D", "", str(x)))
socioEmpresa = socioEmpresa[~socioEmpresa['CPF_CNPJ'].astype(str).str.isalpha()]
socioPessoa.to_csv(args.path_csv + '/SocioPessoa.csv')
socioEmpresa.to_csv(args.path_csv + '/SocioEmpresa.csv')
del socio
del socioPessoa
del socioEmpresa
del pessoa

licitacao = pd.read_csv(args.path_csv + '201908_Licitacao.csv',';',encoding='latin1', dtype = str,usecols = ['Número Licitação','Objeto','Situação Licitação','Valor Licitação','Data Resultado Compra','Nome Órgão','Código Órgão'])
licitacao.rename(columns = {'Número Licitação':'Numero_Licitacao','Situação Licitação':'Situacao_Licitacao','Código Órgão':'Codigo_Orgao','Nome Órgão':'Nome_Orgao','Data Resultado Compra':"Data_Resultado_Compra","Valor Licitação" : "Valor_Licitacao"}, inplace=True)
licitacao['Valor_Licitacao'] = licitacao['Valor_Licitacao'].apply(lambda x: x.replace(',', '.'))
licitacao['Valor_Licitacao'] =  pd.to_numeric(licitacao['Valor_Licitacao'])
licitacao = licitacao.drop_duplicates(subset=['Numero_Licitacao'])
licitacao.to_csv(args.path_csv + '/Licitacao.csv')
del licitacao

contrato = pd.read_csv(args.path_csv + '201908_Compras.csv',';',encoding='latin1',dtype = str, usecols = ['Número do Contrato','Objeto','Código Órgão','Nome Órgão','Valor Final Compra','Data Assinatura Contrato','CNPJ Contratado'])
contrato.rename(columns = {'Número do Contrato':'Numero_Contrato','Código Órgão':'Codigo_Orgao','Nome Órgão':'Nome_Orgao','Data Assinatura Contrato':"Data_Contrato","CNPJ Contratado" : "CNPJ","Valor Final Compra" : "Valor_Compra"}, inplace=True)
contrato['Valor_Compra'] = contrato['Valor_Compra'].apply(lambda x: x.replace(',', '.'))
contrato['Valor_Compra'] =  pd.to_numeric(contrato['Valor_Compra'])
contrato.to_csv(args.path_csv + '/Contrato.csv')
del contrato

participante = pd.read_csv(args.path_csv + '201908_ParticipantesLicitacao.csv',';',encoding='latin1',dtype = str, usecols = ['Número Licitação','CNPJ Participante','Flag Vencedor'])
#Renomeio colunas que possuiam espaço e/ou acentos em seu nome
participante.rename(columns = {'Número Licitação' : 'Numero_Licitacao','CNPJ Participante' : 'CNPJ_Participante', 'Flag Vencedor' : 'Flag_Vencedor'}, inplace=True)
participante['Flag_Vencedor'] = participante['Flag_Vencedor']=='SIM'
participante.to_csv(args.path_csv + '/Participou.csv')
del participante

print("O tratamento dos csv's levou " + str(time.time() - tempoCsv) + " segundos")

driver = GraphDatabase.driver("bolt://localhost:7687", auth=(args.user, args.pswd))

nos = [['Pessoa.csv','MERGE (p:Pessoa { nome: linha.NOME, cpf: linha.CPF})']
      ,['Empresa.csv','MERGE (e:Empresa {nomeSocial: linha.RAZAOSOCIAL, nomeFantasia: linha.NOMEFANTASIA, cnpj: linha.CNPJ})']
      ,['Licitacao.csv','MERGE (o:Orgao {nome: linha.Nome_Orgao, codigoOrgao: linha.Codigo_Orgao})']
      ,['Pessoa.csv','MERGE (o:Orgao {nome: linha.ORG_EXERCICIO, codigoOrgao: linha.COD_ORG_EXERCICIO})']
      ,['Licitacao.csv','MERGE (o:Licitacao {numero: linha.Numero_Licitacao, objeto: linha.Objeto, situacao: linha.Situacao_Licitacao, valor: toFloat(linha.Valor_Licitacao), data: date(datetime({epochmillis:apoc.date.parse(linha.Data_Resultado_Compra, "ms", "dd/MM/yyyy") }))})']
      ,['Contrato.csv','MERGE (o:Contrato {numero: linha.Numero_Contrato, objeto: linha.Objeto, valor: toFloat(linha.Valor_Compra), data: date(datetime({epochmillis:apoc.date.parse(linha.Data_Contrato, "ms", "dd/MM/yyyy") }))})']
      ]
arestas = [['Pessoa.csv','MATCH (o:Orgao {codigoOrgao: linha.COD_ORG_EXERCICIO}),(p:Pessoa {cpf : linha.CPF, nome : linha.NOME}) CALL apoc.create.relationship(p,"Servidor",{matricula : linha.MATRICULA, cargo : linha.DESCRICAO_CARGO, situacao : linha.SITUACAO_VINCULO},o) YIELD rel RETURN rel']
          ,['SocioPessoa.csv','MATCH (e:Empresa {cnpj: linha.CNPJ}),(p:Pessoa {cpf : linha.CPF_CNPJ, nome : linha.NOME}) CALL apoc.merge.relationship(p,"Socio",{tipo : linha.Tipo},{tipo : linha.Tipo},e,{}) YIELD rel RETURN rel']
          ,['SocioEmpresa.csv','MATCH (e1:Empresa {cnpj: linha.CPF_CNPJ}),(e2:Empresa {cnpj: linha.CNPJ}) CALL apoc.create.relationship(e1,"Socio",{tipo : linha.Tipo},e2) YIELD rel RETURN rel']
          ,['Licitacao.csv','MATCH (o:Orgao {codigoOrgao: linha.Codigo_Orgao}),(l:Licitacao {numero : linha.Numero_Licitacao}) CALL apoc.create.relationship(o,"Licitou",{},l) YIELD rel RETURN rel']
          ,['Contrato.csv','MATCH (e:Empresa {cnpj: linha.CNPJ}),(c:Contrato {numero : linha.Numero_Contrato, objeto : linha.Objeto}) CALL apoc.create.relationship(e,"Atuou",{},c) YIELD rel RETURN rel']
          ,['Contrato.csv','MATCH (o:Orgao {codigoOrgao: linha.Codigo_Orgao}),(c:Contrato {numero : linha.Numero_Contrato, objeto : linha.Objeto}) CALL apoc.create.relationship(o,"Contratou",{},c) YIELD rel RETURN rel']
          ,['Participou.csv','MATCH (e:Empresa {cnpj: linha.CNPJ_Participante}),(l:Licitacao {numero : linha.Numero_Licitacao}) MERGE (e)-[p:Participou]->(l) ON CREATE SET p += {vencedor : toBoolean(linha.Flag_Vencedor), participacoes : 1} ON MATCH SET p.participacoes = p.participacoes + 1 return p']
          ]

indexes = ["CREATE INDEX ON :Pessoa(cpf,nome)"
           ,"CREATE INDEX ON :Empresa(cnpj)"
           ,"CREATE INDEX ON :Contrato(numero,objeto)"
           ,"CREATE INDEX ON :Orgao(codigoOrgao)"
           ,"CREATE INDEX ON :Licitacao(numero)"
          ]

template = Template("USING PERIODIC COMMIT "+ args.commit +" LOAD CSV WITH HEADERS FROM 'file:///"+ args.path_csv +"$path' AS linha $create ;")

tempoCriar = time.time()

with driver.session() as sessao:
    #Cria indexes que serão EXTREMAMENTE NECESSÁRIOS
    print("Criando indexes.")
    for index in indexes:
        sessao.run(index)
    #Insere os nós
    print("Inserindo nós.")
    for no in nos:
        sessao.run(template.substitute(path = no[0],create = no[1]))
    #Insere as relações
    print("Inserindo relações.")
    for aresta in arestas:
        sessao.run(template.substitute(path = aresta[0],create = aresta[1]))

print("O inserção no Neo4j levou " + str(time.time() - tempoCriar) + " segundos")

print("Removendo csv's")

os.remove(args.path_csv + 'Pessoa.csv')
os.remove(args.path_csv + 'Empresa.csv')
os.remove(args.path_csv + 'SocioPessoa.csv')
os.remove(args.path_csv + 'SocioEmpresa.csv')
os.remove(args.path_csv + 'Licitacao.csv')
os.remove(args.path_csv + 'Contrato.csv')
os.remove(args.path_csv + 'Participou.csv')

print("O processo inteiro levou " + str(time.time() - comeco) + " segundos")
