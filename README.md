# spark-tutorial-zeppelin

Tutorial de Apache Spark usando cadernos Zeppelin

Abra o terminal e entre na pasta do tutorial:

`$ cd /path/to/spark-tutorial-zeppelin`

Após, execute o comando:

`
$ docker run -p 8080:8080 --rm -v $PWD/data:/data -v $PWD/notebook:/notebook -e ZEPPELIN_NOTEBOOK_DIR='/notebook' --name zeppelin apache/zeppelin:0.7.3
`

Este comando irá baixar as dependências do Zeppelin (2.42GB) e executá-lo.

Para ver os cadernos do Zeppelin abra um navegador e acesse [localhost:8080/][1]

Teste se está funcionando abrindo o caderno [Básico de Scala][2]

Para executar os códigos nos cadernos, aperte o "play" ou selecione o parágrafo e use o atalho `shift + enter`

[1]: http://localhost:8080/
[2]: http://localhost:8080/#/notebook/2D18J63WY
