# Tutorial de Spark

## Resilient Distributed Dataset (RDD)

Principal componente de Spark, funciona como uma abstração do conjunto de dados, fazendo com que um conjunto particionado em várias partes seja tratado como se fosse um único elemento.

### SparkContext
* Criado pelo programa Driver
* Resposponsável por fazer os RDDs serem resilientes e distribuídos
* Cria RDDs
* É criado automaticamente pelo Zeppelin e o Spark Shell como o objeto `sc`

~~~scala
println(sc)
~~~

~~~python
print(sc)
~~~

### Criando RDDs
* `val nums = sc.parallelize(List(1, 2, 3, 4))`
* `sc.textFile("file:///c:/users/frank/gobs-o-text.txt")`
	* or `s3n://` , `hdfs://`
* `hiveCtx = HiveContext(sc)` 
	* `rows = hiveCtx.sql("SELECT name, age FROM users")`
* Can also create from:
	* JDBC
	* Cassandra
	* HBase
	* Elastisearch
	* JSON, CSV, sequence files, object files, various compressed formats


~~~scala
val nums = sc.parallelize(List(1, 2, 3, 4)) // RDD
~~~

~~~python
nums = sc.parallelize([1, 2, 3, 4]) # RDD
~~~

### Operações

Spark possui dois tipos de operações em RDDs, as transformações e as ações:

#### Transformações

Transformações são todas as operações que geram um novo RDD a partir de outro ou, em outras palavras, que transformam um RDD em outro.

Algumas transformações:

* map
* flatmap
* filter
* distinct
* sample
* union, intersection, subtract, cartesian

~~~scala
val squares = nums.map(x => x * x) // this yield 1, 4, 8, 16

def squareIt(x: Int): Int = x * x

val squares2 = nums.map(squareIt)
~~~

~~~python
squares = nums.map(lambda x: x * x) # this yield 1, 4, 8, 16

def squareIt(x):
    return x * x

squares2 = nums.map(squareIt)
~~~

#### Ações

Ações são todas as operações que geram um resultado final que não um RDD. Na realidade, uma ação é o que desencadeia de fato a execução de todas as transformações até a execução da ação final que gera algum resultado que não um novo RDD. Isso ocorre porque RDDs são Lazy Evaluated.

Algumas das principais ações:

* collect
* count
* countByValue
* take
* top
* reduce

~~~scala
val squaresSum = squares.reduce(_+_)

println(squaresSum)
~~~

~~~python
squaresSum = squares.reduce(lambda a, b: a + b)

print(squaresSum)
~~~

### Exemplos

#### MovieLens

Vamos trabalhar com uma base de dados do site MovieLens de classificação de filmes. Nosso conjunto contem 100,000 classificações de 1000 usuários em 1700 filmes. Os dados são de filmes produzidos até 1998. Os dados estão na pasta `/data/ml-100k`. Segue a descrição dos dois principais arquivos:

u.data     -- The full u data set, 100000 ratings by 943 users on 1682 items.
              Each user has rated at least 20 movies.  Users and items are
              numbered consecutively from 1.  The data is randomly
              ordered. This is a tab separated list of 
	         user id | item id | rating | timestamp. 
              The time stamps are unix seconds since 1/1/1970 UTC   

u.item      -- Information about the items (movies); this is a tab separated
              list of
              movie id | movie title | release date | video release date |
              IMDb URL | unknown | Action | Adventure | Animation |
              Children's | Comedy | Crime | Documentary | Drama | Fantasy |
              Film-Noir | Horror | Musical | Mystery | Romance | Sci-Fi |
              Thriller | War | Western |
              The last 19 fields are the genres, a 1 indicates the movie
              is of that genre, a 0 indicates it is not; movies can be in
              several genres at once.
              The movie ids are the ones used in the u.data data set.
              
### Exemplo 1
Vamos contar quantas vezes cada classificação (de 1 a 5 estrelas) foi dada: 

~~~scala
// Load up each line of the ratings data into an RDD
val lines = sc.textFile("/data/ml-100k/u.data")
    
// Convert each line to a string, split it out by tabs, and extract the third field.
// (The file format is userID, movieID, rating, timestamp)
val ratings = lines.map(x => x.toString().split("\t")(2))
    
// Count up how many times each value (rating) occurs
val results = ratings.countByValue()
    
// Sort the resulting map of (rating, count) tuples
val sortedResults = results.toSeq.sortBy(_._1)
    
// Print each result on its own line.
sortedResults.foreach(println)
~~~

~~~python
# Load up each line of the ratings data into an RDD
lines = sc.textFile("/data/ml-100k/u.data")
    
# Convert each line to a string, split it out by tabs, and extract the third field.
# (The file format is userID, movieID, rating, timestamp)
ratings = lines.map(lambda x: x.split("\t")[2])
    
# Count up how many times each value (rating) occurs
results = ratings.countByValue().items()
    
# Print each result on its own line.
for r in results:
    print r
~~~

### Exemplo 2
Vamos trabalhar com RDDs de tuplas de chave-valor. Esse tipo de RDD possui uma série de operações específicas que permitem agrupar e processar valores de acordo com suas chaves e permite operações de Join estilo SQL.

Nesse exemplos vamos analisar um conjunto de dados que contem informações sobre pessoas e seu número de amigos. Cada linha do arquivo está da seguinte forma:

ID, name, age,  number of friends

Nosso objetivo é encontrar a média do número de amigos por idade.

~~~scala
// Load each line of the source data into an RDD
val lines = sc.textFile("/data/fakefriends.csv")
    
/** A function that splits a line of input into (age, numFriends) tuples. */
def parseLine(line: String) = {
    // Split by commas
    val fields = line.split(",")
    // Extract the age and numFriends fields, and convert to integers
    val age = fields(2).toInt
    val numFriends = fields(3).toInt
    // Create a tuple that is our result.
    (age, numFriends)
}
    
// Use our parseLines function to convert to (age, numFriends) tuples
val rdd = lines.map(parseLine)
    
// Lots going on here...
// We are starting with an RDD of form (age, numFriends) where age is the KEY and numFriends is the VALUE
// We use mapValues to convert each numFriends value to a tuple of (numFriends, 1)
// Then we use reduceByKey to sum up the total numFriends and total instances for each age, by
// adding together all the numFriends values and 1's respectively.
val totalsByAge = rdd.mapValues(x => (x, 1)).reduceByKey( (x,y) => (x._1 + y._1, x._2 + y._2))
    
// So now we have tuples of (age, (totalFriends, totalInstances))
// To compute the average we divide totalFriends / totalInstances for each age.
val averagesByAge = totalsByAge.mapValues(x => x._1 / x._2)
    
// Collect the results from the RDD (This kicks off computing the DAG and actually executes the job)
val results = averagesByAge.collect()
    
// Sort and print the final results.
println("\nThe average of friends by age:")
results.sorted.foreach(println)
~~~

~~~python
# Load each line of the source data into an RDD
lines = sc.textFile("/data/fakefriends.csv")
    
# A function that splits a line of input into (age, numFriends) tuples.
def parseLine(line):
    # Split by commas
    fields = line.split(",")
    # Extract the age and numFriends fields, and convert to integers
    age = int(fields[2])
    numFriends = int(fields[3])
    # Create a tuple that is our result.
    return (age, numFriends)
    
# Use our parseLines function to convert to (age, numFriends) tuples
rdd = lines.map(parseLine)
    
# Lots going on here...
# We are starting with an RDD of form (age, numFriends) where age is the KEY and numFriends is the VALUE
# We use mapValues to convert each numFriends value to a tuple of (numFriends, 1)
# Then we use reduceByKey to sum up the total numFriends and total instances for each age, by
# adding together all the numFriends values and 1's respectively.
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
    
# So now we have tuples of (age, (totalFriends, totalInstances))
# To compute the average we divide totalFriends / totalInstances for each age.
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
    
# Collect the results from the RDD (This kicks off computing the DAG and actually executes the job)
results = averagesByAge.collect()
    
# Sort and print the final results.
print("\nThe average of friends by age:")
for r in results:
    print r
~~~

#### Exercício 1
Encontre a média do numero de amigos pelo primeiro nome de uma pessoa.

### Exemplo 3
Nesse exemplos vamos trabalhar com a transformação `filter`, que recebe uma função booleana que avalia se cada item do RDD deve continuar ou não no conjunto de dados. Nosso conjunto de dados contém informações do tempo de estações na Europa no ano de 1800. Cada linha do conjunto contém as seguintes informações:

ID da estação, data, tipo da informação do tempo (TMIN, TMAX, PRCP), valor da informação, e outros dados.

Nesse exemplo queremos encontrar a menor temperatura mínima por estação.


~~~scala
// Read each line of input data
val lines = sc.textFile("/data/1800.csv")

def parseLine(line:String)= {
    val fields = line.split(",")
    val stationID = fields(0)
    val entryType = fields(2)
    val temperature = fields(3).toFloat
    (stationID, entryType, temperature)
}
    
// Convert to (stationID, entryType, temperature) tuples
val parsedLines = lines.map(parseLine)
    
// Filter out all but TMIN entries
val minTemps = parsedLines.filter(x => x._2 == "TMIN")
    
// Convert to (stationID, temperature)
val stationTemps = minTemps.map(x => (x._1, x._3.toFloat))
    
// Reduce by stationID retaining the minimum temperature found
val minTempsByStation = stationTemps.reduceByKey( (x,y) => if(x < y) x else y)
    
// Collect, format, and print the results
val results = minTempsByStation.collect()
    
for (result <- results.sorted) {
    val station = result._1
    val temp = result._2
    val formattedTemp = f"$temp%.2f C"
    println(s"$station minimum temperature: $formattedTemp") 
}
~~~

~~~python
# Read each line of input data
lines = sc.textFile("/data/1800.csv")

def parseLine(line):
    fields = line.split(",")
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3])
    return (stationID, entryType, temperature)
    
# Convert to (stationID, entryType, temperature) tuples
parsedLines = lines.map(parseLine)
    
# Filter out all but TMIN entries
minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])
    
# Convert to (stationID, temperature)
stationTemps = minTemps.map(lambda x: (x[0], float(x[2])))
    
def min(x, y):
    if(x < y):
        return x
    else:
        return y

# Reduce by stationID retaining the minimum temperature found
minTempsByStation = stationTemps.reduceByKey(min)
    
# Collect, format, and print the results
results = minTempsByStation.collect()
    
for result in results:
    station = result[0]
    temp = result[1]
    print(station, temp) 
~~~

#### Exercício 2
Com o mesmo conjunto de dados do Exemplo 3:

1. Encontre a maior temperatura máxima por estação (TMAX)
2. Encontre o dia que teve a maior precipitação para cada estação que possui informação sobre precipitação (PRCP)

### Exemplo 4

Neste exemplo vamos contar a quantidade de vezes em que cada palava aparece em um texto e vamos exibir as 10 mais frequentes. Este exemplo é o mais classico no contexto de Big Data. Vamos ter a oportunidade de trabalhar com a transformação `flatMap`, que trabalha de forma parecida ao `map`, mas com a diferença de que a função passada como parâmetro deve retornar algum objeto iterável (listas, sequências, arrays...) e o resultado final da transformação é a concatenação de todos os objetos retornados.

~~~scala
// Read each line of my book into an RDD
val input = sc.textFile("/data/book.txt")
    
// Split into words separated by a space character
val words = input.flatMap(x => x.split(" ")).cache()
    
// Count up the occurrences of each word
val wordCounts = words.countByValue()

// Print the results.
wordCounts.toSeq.sortBy(_._2).reverse.take(10).foreach(println)
~~~

~~~python
# Read each line of my book into an RDD
input = sc.textFile("/data/book.txt")
    
# Split into words separated by a space character
words = input.flatMap(lambda x: x.split(" ")).cache()
    
# Count up the occurrences of each word
wordCounts = words.countByValue()

# Print the results.
sortedWordCounts = sorted(wordCounts.items(), key=lambda x: x[1], reverse=True)
for n in range(0, 9):
    print sortedWordCounts[n]
~~~

### Exemplo 5

Um problema da nossa solução para o contador de palavras é que estamos carregando para o Driver todas as palavras e suas frequências, mas, uma vez que só queremos exibir as 10 palavras mais frequentes, estamos carregando um número muito grande de dados desnecessários (6983 palavras e frequências que não estão sendo exibidas) para o Driver, o que em um exemplo maior pode ser um problema. Para melhorar isso, uma possível solução é fazer a ordenação no próprio RDD sem ter que coletar todos os seus dados antes e só depois pegar a quantidade necessária de dados. 

Se você reparou bem no exemplo anterior, no RDD armazenado na variável words, logo após a chamada do `flatMap`, nós chamamos a operação `cache`. Em Spark, todo o processamento feito em cima dos dados só é iniciado de fato após se chamar alguma ação, logo, transformações como `map` e `filter`, por exemplo, só são computadas quando alguma ação, como `reduce` ou `collect`, por exemplo, é chamada. Antes disso, as transformações realizadas formam apenas um plano de execução que será ativado quando alguma ação for chamada, sendo essa a razão por qual Spark é Lazy Evaluated. 

Esse plano de ação é armazenado por Spark como uma DAG (Direct Acyclic Graph) que forma uma linha do tempo de dependências entre RDDs. Essa DAG serve tanto para optimizar a execução da ação quanto para guardar o plano de execução para o caso em que ocorra alguma falha no processamento e alguma etapa precise ser computada novamente, dessa forma, garantindo a resiliência de Spark. Uma vez que uma transformação não retorna um conjunto de dados de fato (concreto), se nós chamarmos várias vezes uma ação no mesmo RDD, o que acontece é que todas as transformações vão ser computadas novamente para cada nova ação que chamarmos. 

Uma forma de evitar isso é colocar em memória os dados intermediários de um RDD. Fazendo isso, fazemos com que após o RDD ser computado pela primeira vez, todas as ações seguintes realizadas no mesmo RDD não precisem recomputar todas as trasnformações novamente, evitando, assim, novos processamentos desnecessários. Em Spark isso pode ser feito ao chamar a operação `cache` que coloca o resultado do RDD em memória principal. Uma outra alternativa é chamar a operação `persist` que por default tem o mesmo comportamento que `cache`, sendo que a diferença principal é que `persist` permite armazenar os dados em outros tipos de memória (disco) e utilizando tipos diferentes de compressão. 

No nosso exemplo abaixo, estamos reutilizando o RDD `words` do Exemplo 4, uma vez que nós fizemos um `cache` deste RDD, o processamento que foi feito antes em `words` irá ser feito novamente:

~~~scala
val wordCounts = words.map(x => (x, 1)).reduceByKey((x, y) => x + y)

// Flip (word, count) tuples to (count, word) and then sort by key (the counts)
val wordCountsSorted = wordCounts.map(x => (x._2, x._1)).sortByKey(false).take(10) // descending 

// Print the results, flipping the (count, word) results to word: count as we go.
for (result <- wordCountsSorted) {
    val count = result._1
    val word = result._2
    println(s"($word, $count)")
}
~~~

~~~python
wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)

# Flip (word, count) tuples to (count, word) and then sort by key (the counts)
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey(False).take(10) # descending 

# Print the results, flipping the (count, word) results to word: count as we go.
for result in wordCountsSorted:
    print(result)
~~~

#### Exercício 3

Nosso exemplo do contador de palavras ainda tem uma série de problemas:

1. Palavras terminadas com algum tipo de pontuação, como "way?", "of." e "touch,", por exemplo, estão sendo computadas como uma palavra normal, sendo que a pontuação não deveria fazer parte da nossa análise;
2. Palavras com letras em caixa alta ou caixa baixa estão estão sendo processadas de forma diferente, dessa forma, palavras como "The" e "the" são consideradas palavras diferentes, sendo, na verdade, a mesma palavra;
3. Palavras comuns como "to", "you", "the", "a" e etc são as palavras, de fato, que mais aparecem em um texto em inglês, entretanto, essas palavras não nos ajudam a fazer uma boa análise do texto uma vez que não são relacionadas ao assunto tratado, por isso deveriam ser excluídas durante a análise.

Melhore o exemplo do contador de palavras solucionando os três problemas citados acima.

Dicas:

1. Use expressões regulares (`"\\W+"`);
2. Deixe tudo em caixa alta ou caixa baixa;
3. Crie um dicionário de termos comuns e filtre-os.

#### Exercício 4

Com o mesmo conjunto de dados do site MovieLens analisado no Exemplo 1:

1. Encontre os 10 filmes com maior numero de classificações (ratings)
2. Encontre o filme com a média de classificação (rating) mais alta

Em ambos os casos, deve ser exibido o título do filme ao invés do seu id.

Dica:

* Obtenha os títulos dos filmes no arquivo `/data/u.item`
    * Uma alternativa é criar um RDD de (id, title) e fazer um join com o RDD principal
    * Uma outra alternativa, mais eficiente, é usar uma Broadcast Variable para guardar um dicionário de (id, title) (Pesquise essa alternativa)