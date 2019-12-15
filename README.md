# Portal da Transparência => Neo4j

* ## Script criado para importar os dados do portal da transparência para o Neo4j, para serem utilizados nos trabalhos da disciplina de Banco de dados não convencionais.
### Atualmente é possível ou rodar o script para gerar o banco, ou apenas importar o banco já gerado.

* ## Para gerar o banco:

### Pré-requesitos para o script:
* Descomentar/Adicionar `dbms.security.allow_csv_import_from_file_urls=true`, nas configurações do grafo, para podermos carregar cvs's locais;
* Comentar e/ou remover `dbms.directories.import=import`, nas configurações do grafo para podermos abrir arquivos em qualquer path do sistema;
* Possuir o pandas instalado;
* Possuir o driver para o python do neo4j;
##### Para alterar as configurações do grafo basta ir pelo neo4j no grafo desejado, ir em settings e em modificar.
##### Caso tenha alguma dúvida em relação aos parâmetros que você precisa passar para o script, tente o -h.
##### Caso tenha algum problema relacionado com memória tente diminuir o numero do parâmetro commit.

#### Para executar o script basta inicialo com o path absoluto da onde está localizados os seus csv's extraídos.

* ## Para importar o banco:

### Para somente importar o banco, basta [baixar](https://drive.google.com/file/d/1zCvcykya8mvsb61IX1eKWyE7AjyWu5Y_/view?usp=sharing) e copiar o arquivo para a pasta raiz do seu banco no neo4j(pelo Neo4j ir nas configurações do sue grafo e abrir terminal) e executar o comando `bin/neo4j-admin load --from=transparencia.dump --force`(creio que para executar no windows basta trocar a / por \\) na pasta raiz do seu banco.
