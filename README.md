# Portal da Transparência => Neo4j
## Pré-requesitos para o script:
* Descomentar/Adicionar `dbms.security.allow_csv_import_from_file_urls=true`, nas configurações do grafo, para podermos carregar cvs's locais;
* Comentar e/ou remover `dbms.directories.import=import`, nas configurações do grafo para podermos abrir arquivos em qualquer path do sistema;
* Possuir o pandas instalado;
* Possuir o driver para o python do neo4j;
##### Para alterar as configurações do grafo basta ir pelo neo4j no grafo desejado, ir em settings e em modificar.

#### Para executar o script basta alterar o valor da variável path_csv para o path da onde está localizados os seus csv's extraidos.

## Para somente importar o banco, basta [baixar] (https://drive.google.com/file/d/1OXy-ZOSvUZcyUwR3ytfVmjH50TEoX5nZ/view?usp=sharing) e extrair o graph.db.zip no local aonde esteja localizado o seu graph.db e substituir os arquivos.
