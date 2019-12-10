# Portal da Transparência => Neo4j
## Pré-requesitos para o script:
* Descomentar/Adicionar `dbms.security.allow_csv_import_from_file_urls=true`, nas configurações do grafo, para podermos carregar cvs's locais;
* Comentar e/ou remover `dbms.directories.import=import`, nas configurações do grafo para podermos abrir arquivos em qualquer path do sistema;
* Possuir o pandas instalado;
* Possuir o driver para o python do neo4j;
##### Para alterar as configurações do grafo basta ir pelo neo4j no grafo desejado, ir em settings e em modificar.

#### Para executar o script basta alterar o valor da variável path_csv para o path da onde está localizados os seus csv's extraídos.

## Para somente importar o banco, basta [baixar](https://drive.google.com/file/d/1efTagHwfbrTflWlBur0WufFowpLceX2S/view?usp=sharing) e copiar o arquivo para a pasta raiz do seu banco no neo4j e executar   o comando `bin/neo4j-admin load --from=transparencia.dump` na pasta raiz do seu banco.
