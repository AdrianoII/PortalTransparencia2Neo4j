import pandas as pd
from neo4j import GraphDatabase
from string import Template
import re
import time

comeco = time.time()

#Path para a pasta em que os .csv estão extraidos exemplo: '/home/adrianoii/TabelasBd/'
path_csv = 'insira o seu path aqui'




#Carrego todos os csv's que serão utilizados, somente com as informações necessárias
pessoa = pd.read_csv( path_csv + '201908_Cadastro.csv',';',encoding='latin1',dtype = str, usecols = ['NOME','CPF','MATRICULA','DESCRICAO_CARGO','SITUACAO_VINCULO','COD_ORG_EXERCICIO','ORG_EXERCICIO'])
empresa = pd.read_csv( path_csv + '201908_CNPJ.csv',';',encoding='latin1',dtype = str, usecols = ['CNPJ','NOMEFANTASIA','RAZAOSOCIAL'])
socio = pd.read_csv( path_csv + '201908_Socios.csv',';',encoding='latin1',dtype = str, usecols = ['Nome','CPF-CNPJ','CNPJ','Tipo'])
licitacao = pd.read_csv(path_csv + '201908_Licitacao.csv',';',encoding='latin1', usecols = ['Número Licitação','Objeto','Situação Licitação','Valor Licitação','Data Resultado Compra','Nome Órgão','Código Órgão'])
contrato = pd.read_csv(path_csv + '201908_Compras.csv',';',encoding='latin1',dtype = str, usecols = ['Número do Contrato','Objeto','Código Órgão','Nome Órgão','Valor Final Compra','Data Assinatura Contrato','CNPJ Contratado'])
participante = pd.read_csv(path_csv + '201908_ParticipantesLicitacao.csv',';',encoding='latin1',dtype = str, usecols = ['Número Licitação','CNPJ Participante','Flag Vencedor'])


#Renomeio colunas que possuiam espaço e/ou acentos em seu nome
socio.rename(columns = {'CPF-CNPJ':'CPF_CNPJ'}, inplace=True)
licitacao.rename(columns = {'Número Licitação':'Numero_Licitacao','Situação Licitação':'Situacao_Licitacao','Código Órgão':'Codigo_Orgao','Nome Órgão':'Nome_Orgao','Data Resultado Compra':"Data_Resultado_Compra","Valor Licitação" : "Valor_Licitacao"}, inplace=True)
contrato.rename(columns = {'Número do Contrato':'Numero_Contrato','Código Órgão':'Codigo_Orgao','Nome Órgão':'Nome_Orgao','Data Assinatura Contrato':"Data_Contrato","CNPJ Contratado" : "CNPJ","Valor Final Compra" : "Valor_Compra"}, inplace=True)
participante.rename(columns = {'Número Licitação' : 'Numero_Licitacao','CNPJ Participante' : 'CNPJ_Participante', 'Flag Vencedor' : 'Flag_Vencedor'}, inplace=True)


#Preencho as células da coluna NOMEFANTASIA que não possuiam valor com " ",
#pois o merge não aceita um valor nulo
empresa['NOMEFANTASIA'] = empresa['NOMEFANTASIA'].fillna(" ")
empresa['RAZAOSOCIAL'] = empresa['RAZAOSOCIAL'].fillna(" ")

#Realizo joins para diminuir o número de linhas das tabelas e conseguir outras informações casos precise
socioPessoa = pd.merge(left=pessoa, right=socio,left_on=['CPF'], right_on=['CPF_CNPJ'], how='inner')
participante = pd.merge(left=participante, right=empresa,left_on=['CNPJ_Participante'], right_on=['CNPJ'], how='inner')

#Retiro a mascara do CPNJ e deixo somente as linhas com CNPJ numéricos
socioEmpresa = socio.copy()
socioEmpresa['CPF_CNPJ'] = socioEmpresa['CPF_CNPJ'].apply( lambda x : re.sub(r"\D", "", str(x)))
socioEmpresa = socioEmpresa[~socioEmpresa['CPF_CNPJ'].astype(str).str.isalpha()]

#Salvo os novos cv's
pessoa.to_csv('/home/adrianoii/TabelasBd/Pessoa.csv')
empresa.to_csv('/home/adrianoii/TabelasBd/Empresa.csv')
socioPessoa.to_csv('/home/adrianoii/TabelasBd/SocioPessoa.csv')
socioEmpresa.to_csv('/home/adrianoii/TabelasBd/SocioEmpresa.csv')
licitacao.to_csv('/home/adrianoii/TabelasBd/Licitacao.csv')
contrato.to_csv('/home/adrianoii/TabelasBd/Contrato.csv')
participante.to_csv('/home/adrianoii/TabelasBd/Participou.csv')

del pessoa
del empresa
del socio
del socioPessoa
del socioEmpresa
del licitacao
del contrato
del participante

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "9090"))

nos = [['Pessoa.csv','MERGE (p:Pessoa { nome: linha.NOME, cpf: linha.CPF})']
      ,['Empresa.csv','MERGE (e:Empresa {nomeSocial: linha.RAZAOSOCIAL, nomeFantasia: linha.NOMEFANTASIA, cnpj: linha.CNPJ})']
      ,['Licitacao.csv','MERGE (o:Orgao {nome: linha.Nome_Orgao, codigoOrgao: linha.Codigo_Orgao})']
      ,['Pessoa.csv','MERGE (o:Orgao {nome: linha.ORG_EXERCICIO, codigoOrgao: linha.COD_ORG_EXERCICIO})']
      ,['Licitacao.csv','MERGE (o:Licitacao {numero: linha.Numero_Licitacao, objeto: linha.Objeto, situacao: linha.Situacao_Licitacao, valor: linha.Valor_Licitacao, data: linha.Data_Resultado_Compra})']
      ,['Contrato.csv','MERGE (o:Contrato {numero: linha.Numero_Contrato, objeto: linha.Objeto, valor: linha.Valor_Compra, data: linha.Data_Contrato})']
      ]
arestas = [['Pessoa.csv','MATCH (o:Orgao {codigoOrgao: linha.COD_ORG_EXERCICIO}),(p:Pessoa {cpf : linha.CPF, nome : linha.NOME}) CALL apoc.create.relationship(p,"Servidor",{matricula : linha.MATRICULA, cargo : linha.DESCRICAO_CARGO, situacao : linha.SITUACAO_VINCULO},o) YIELD rel RETURN rel']
          ,['SocioPessoa.csv','MATCH (e:Empresa {cnpj: linha.CNPJ}),(p:Pessoa {cpf : linha.CPF_CNPJ, nome : linha.NOME}) CALL apoc.merge.relationship(p,"Socio",{tipo : linha.Tipo},{tipo : linha.Tipo},e,{}) YIELD rel RETURN rel']
          ,['Licitacao.csv','MATCH (o:Orgao {codigoOrgao: linha.Codigo_Orgao}),(l:Licitacao {numero : linha.Numero_Licitacao, objeto : linha.Objeto}) CALL apoc.create.relationship(o,"Licitou",{},l) YIELD rel RETURN rel']
          ,['Contrato.csv','MATCH (e:Empresa {cnpj: linha.CNPJ}),(c:Contrato {numero : linha.Numero_Contrato, objeto : linha.Objeto}) CALL apoc.create.relationship(e,"Atuou",{},c) YIELD rel RETURN rel']
          ,['Contrato.csv','MATCH (o:Orgao {codigoOrgao: linha.Codigo_Orgao}),(c:Contrato {numero : linha.Numero_Contrato, objeto : linha.Objeto}) CALL apoc.create.relationship(o,"Contratou",{},c) YIELD rel RETURN rel']
          ,['Participou.csv','MATCH (e:Empresa {cnpj: linha.CNPJ_Participante}),(l:Licitacao {numero : linha.Numero_Licitacao}) CALL apoc.create.relationship(e,"Participou",{vencedor : linha.Flag_Vencedor},l) YIELD rel RETURN rel']
          ,['SocioEmpresa.csv','MATCH (e1:Empresa {cnpj: linha.CPF_CNPJ}),(e2:Empresa {cnpj: linha.CNPJ}) CALL apoc.create.relationship(e1,"Socio",{tipo : linha.Tipo},e2) YIELD rel RETURN rel']
          ]

template = Template("USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM 'file:///"+ path_csv +"$path' AS linha $create ;")

with driver.session() as sessao:
    #Cria indexes que serão EXTREMAMENTE NECESSÁRIOS
    sessao.run('CREATE INDEX ON :Pessoa(cpf,nome)')
    sessao.run('CREATE INDEX ON :Empresa(cnpj)')
    sessao.run('CREATE INDEX ON :Contrato(numero,objeto)')
    sessao.run('CREATE INDEX ON :Orgao(codigoOrgao)')
    sessao.run('CREATE INDEX ON :Licitacao(numero,objeto)')
    sessao.run('create index on :Licitacao(numero)')
    #Insere os nós
    for no in nos:
        sessao.run(template.substitute(path = no[0],create = no[1]))
    #Insere as relações
    for aresta in arestas:
        sessao.run(template.substitute(path = aresta[0],create = aresta[1]))

print(time.time() - comeco)
