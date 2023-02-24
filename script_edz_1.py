# Databricks notebook source
# MAGIC %md
# MAGIC #Stack Academy - Processando dados

# COMMAND ----------

# MAGIC %md
# MAGIC ###Task
# MAGIC 
# MAGIC Converter arquivos csv em parquet e enviar para processing zone.
# MAGIC 
# MAGIC ###Dataset
# MAGIC 
# MAGIC Usaremos esse dataset https://www.kaggle.com/nhs/general-practice-prescribing-data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Modos de leitura
# MAGIC - **permissive**: *Define todos os campos para NULL quando encontra registros corrompidos e coloca todos registros corrompidos em uma coluna chamada _corrupt_record.* (default)
# MAGIC 
# MAGIC - **dropMalformed**: *Apaga uma linha corrompida ou que este não consiga ler.*
# MAGIC 
# MAGIC - **failFast**: *Falha imediatamente quando encontra uma linha que não consiga ler.*

# COMMAND ----------

# ler arquivos vários arquivos csv do dbfs com spark
# Lendo todos os arquivos .csv do diretório bigdata (>4GB)

df = spark.read.format("csv")\
.option("header", "True")\
.option("inferSchema","True")\
.load("/FileStore/tables/bigdata/*.csv")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# imprime as 10 primeiras linhas do dataframe
display(df.head(10))

# COMMAND ----------

# conta a quantidade de linhas
df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Leva os dados convertidos para a Processing Zone
# MAGIC 
# MAGIC - *Atente para NÃO escrever e ler arquivos parquet em versoes diferentes*

# COMMAND ----------

# Converte para formato parquet
df.write.format("parquet")\
.mode("overwrite")\
.save("/FileStore/tables/processing/df-parquet-file.parquet")

# COMMAND ----------

# lendo arquivos parquet
# atente para a velocidade de leitura

df_parquet = spark.read.format("parquet")\
.load("/FileStore/tables/processing/df-parquet-file.parquet")

# COMMAND ----------

# conta a quantidade de linhas do dataframe
df_parquet.count()

# COMMAND ----------

# DBTITLE 1,Script para obter o tamanho do dataset
# MAGIC %scala
# MAGIC // script para pegar tamanho em Gigabytes
# MAGIC val path="/FileStore/tables/processing/df-parquet-file.parquet"
# MAGIC val filelist=dbutils.fs.ls(path)
# MAGIC val df_temp = filelist.toDF()
# MAGIC df_temp.createOrReplaceTempView("adlsSize")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- consulta a view criada.
# MAGIC select round(sum(size)/(1024*1024*1024),3) as sizeInGB from adlsSize

# COMMAND ----------

display(df_parquet.head(10))

# COMMAND ----------

#Add columns to DataFrame using SQL
df_parquet.createOrReplaceTempView("view_df_parquet")

spark.sql("SELECT BNF_CODE as Bnf_code \
                  ,SUM(ACT_COST) as Soma_Act_cost \
                  ,SUM(QUANTITY) as Soma_Quantity \
                  ,SUM(ITEMS) as Soma_items \
                  ,SUM(ACT_COST) as Media_Act_cost \
           FROM view_df_parquet \
           GROUP BY bnf_code").show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Avançando com Pyspark
# MAGIC - Linguagem em plena ascensão.
# MAGIC - Linguagem simples e com uma comunidade crescente.
# MAGIC - Velocidade idêntica para as Apis SQL, Scala.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Criando um schema
# MAGIC - A opção **infer_schema** nem sempre vai definir o melhor datatype.
# MAGIC - Melhora a performance na leitura de grandes bases.
# MAGIC - Permite uma customização dos tipos das colunas.
# MAGIC - É importante saber para reescrita de aplicações. (Códigos pandas)

# COMMAND ----------

# visualizando datasets de exemplos da databricks
display(dbutils.fs.ls("/databricks-datasets"))

# COMMAND ----------

# Lendo o arquivo de dados
arquivo = "dbfs:/databricks-datasets/flights/"

# COMMAND ----------

# lendo o arquivo previamente com a opção inferSchema ligada
df = spark \
.read \
.option("inferSchema", "True")\
.option("header", "True")\
.csv(arquivo)

# COMMAND ----------

# imprime o schema do dataframe (infer_schema=True)
df.printSchema()

# COMMAND ----------

display(df)

# COMMAND ----------

# usa o objeto StructType
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType, TimestampType

schema_df = StructType([
    StructField("date", StringType()),
    StructField("delay", IntegerType()),
    StructField("distance", IntegerType()),
    StructField("origin", StringType()),
    StructField("destination", StringType())
])

# COMMAND ----------

# verificando o tipo da variável schema_df
type(schema_df)

# COMMAND ----------

# usando o parâmetro schema()
df = spark.read.format("csv")\
.option("header", "True")\
.schema(schema_df)\
.load(arquivo)

# COMMAND ----------

# imprime o schema do dataframe.
df.printSchema()

# COMMAND ----------

# imprime 10 primeiras linhas do dataframe.
df.show(10)

# COMMAND ----------

# imprime o tipo da varia'vel df 
type(df)

# COMMAND ----------

# retorna as primeiras 10 linhas do dataframe em formato de array.
df.take(10)

# COMMAND ----------

# imprime a quantidade de linhas no dataframe.
df.count()

# COMMAND ----------

from pyspark.sql.functions import max
df.select(max("delay")).take(1)

# COMMAND ----------

# Filtrando linhas de um dataframe usando filter
df.filter("delay < 2").show(2)

# COMMAND ----------

# Usando where (um alias para o metodo filter)
df.where("delay < 2").show(2)

# COMMAND ----------

# ordena o dataframe pela coluna delay
df.sort("delay").show(5)

# COMMAND ----------

from pyspark.sql.functions import desc, asc, expr
# ordenando por ordem crescente
df.orderBy(expr("delay desc")).show(10)

# COMMAND ----------

# visualizando estatísticas descritivas
df.describe().show()

# COMMAND ----------

# iterando sobre todas as linhas do dataframe
for i in df.collect():
  #print (i)
  print(i[0], i[1], i[2] * 2)

# COMMAND ----------

# Adicionando uma coluna ao dataframe
df = df.withColumn('Nova Coluna',df['delay']+2)
df.show(10)

# COMMAND ----------

# Reovendo coluna
df = df.drop('Nova Coluna')
df.show(10)

# COMMAND ----------

# Renomenando uma coluna no dataframe
df.withColumnRenamed('Nova Coluna','New Column').show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Trabalhando com missing values
# MAGIC - Tratamento de dados e limpeza de dados

# COMMAND ----------

# checa valoes null na coluna delay
df.filter("delay is NULL").show()

# COMMAND ----------

# conta a quantidade de linhas nulas
print ("Valores nulos coluna Delay: {0}".format(df.filter("delay is NULL").count()))
print ("Valores nulos coluna Date: {0}".format(df.filter("date is NULL").count()))
print ("Valores nulos coluna Distance: {0}".format(df.filter("distance is NULL").count()))
print ("Valores nulos coluna Origin: {0}".format(df.filter("origin is NULL").count()))
print ("Valores nulos coluna Destination: {0}".format(df.filter("destination is NULL").count()))

# COMMAND ----------

# preenche os dados missing com o valor 0
# para fazer o preenchimento sobrescreva a variável df e retire o método show()
df = df.na.fill(value=0)

# COMMAND ----------

# checa valoes null na coluna delay
df.filter("delay is NULL").show()

# COMMAND ----------

# preenche valores missing com valor 0 apenas da coluna delay
df.na.fill(value=0, subset=['delay']).show()

# COMMAND ----------

# imprime o dataframe
df.show()

# COMMAND ----------

# preenche os dados com valores de string vazia
df.na.fill("").show()

# COMMAND ----------

# remove qualquer linha nula de qualquer coluna
df = df.na.drop()

# COMMAND ----------

# obtem o valor máximo da coluna delay
from pyspark.sql.functions import max
df.select(max("delay")).take(1)

# COMMAND ----------

# Filtrando linhas de um dataframe usando filter
df.filter("delay < 2").show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Manipulando Strings

# COMMAND ----------

# lendo os arquivos de dados de voos (2010_summary.csv...2015_summary.csv)
df = spark\
.read\
.option("inferSchema", "True")\
.option("header", "True")\
.csv("/FileStore/tables/bronze2/*.csv")

# COMMAND ----------

# imprime 10 linhas do dataframe
df.show(10)

# COMMAND ----------

# imprime a quantidade de registros do dataframe
df.count()

# COMMAND ----------

from pyspark.sql.functions import lower, upper, col
df.select(col("DEST_COUNTRY_NAME"),lower(col("DEST_COUNTRY_NAME")),upper(lower(col("DEST_COUNTRY_NAME")))).show(10)

# COMMAND ----------

# remove espaços em branco a esquerda
from pyspark.sql.functions import ltrim
df.select(ltrim(col("DEST_COUNTRY_NAME"))).show(2)

# COMMAND ----------

# remove espaços a direita
from pyspark.sql.functions import rtrim
df.select(rtrim(col("DEST_COUNTRY_NAME"))).show(2)

# COMMAND ----------

# todas as operações juntas..
# a função lit cria uma coluna na cópia do dataframe
from pyspark.sql.functions import lit, ltrim, rtrim, rpad, lpad, trim
df.select(
ltrim(lit(" HELLO ")).alias("ltrim"),
rtrim(lit(" HELLO ")).alias("rtrim"),
trim(lit(" HELLO ")).alias("trim"),
lpad(lit("HELLO"), 3, " ").alias("lp"),
rpad(lit("HELLO"), 10, " ").alias("rp")).show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC Estatística descritiva básica:
# MAGIC - **mean()** - Retorna o valor médio de cada grupo.
# MAGIC - **max()** - Retorna o valor máximo de cada grupo.
# MAGIC - **min()** - Retorna o valor mínimo de cada grupo.
# MAGIC - **sum()** - Retorna a soma de todos os valores do grupo.
# MAGIC - **avg()** - Retorna o valor médio de cada grupo.

# COMMAND ----------

# ler o dataset retail-data
df = spark.read.format("csv")\
.option("header", "true")\
.option("inferSchema", "true")\
.load("/FileStore/tables/retail/retail_2010_12_01.csv")

# COMMAND ----------

# imprime as 10 primeiras linhas do dataframe
df.show(10)

# COMMAND ----------

# Soma preços unitários por país
df.groupBy("Country").sum("UnitPrice").show()

# COMMAND ----------

# Conta a quantidade de países distintos.
df.groupBy("Country").count().show()

# COMMAND ----------

# retorna o valor mínimo por grupo
df.groupBy("Country").min("UnitPrice").show()

# COMMAND ----------

# retorna o valor máximo por grupo
df.groupBy("Country").max("UnitPrice").show()

# COMMAND ----------

# retorna o valor médio por grupo
df.groupBy("Country").avg("UnitPrice").show()

# COMMAND ----------

# retorna o valor médio por grupo
df.groupBy("Country").mean("UnitPrice").show()

# COMMAND ----------

# GroupBy várias colunas
df.groupBy("Country","CustomerID") \
    .sum("UnitPrice") \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Trabalhando com datas
# MAGIC - Existem diversas funçoes em Pyspark para manipular datas e timestamp.
# MAGIC - Evite escrever suas próprias funçoes para isso.
# MAGIC - Algumas funcoes mais usadas:
# MAGIC     - current_day():
# MAGIC     - date_format(dateExpr,format):
# MAGIC     - to_date():
# MAGIC     - to_date(column, fmt):
# MAGIC     - add_months(Column, numMonths):
# MAGIC     - date_add(column, days):
# MAGIC     - date_sub(column, days):
# MAGIC     - datediff(end, start)
# MAGIC     - current_timestamp():
# MAGIC     - hour(column):

# COMMAND ----------

# imprime o dataframe
df.show()

# COMMAND ----------

# imprime o schema
df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import *
#current_date() = imprime
df.select(current_date().alias("current_date")).show(1)

# COMMAND ----------

# formata valores de data
df.select(col("InvoiceDate"), \
          date_format(col("InvoiceDate"), "dd/MM/yyyy hh:mm:ss")\
          .alias("Formato Brasil")).show()

# COMMAND ----------

# imprime a diferença entre duas datas
df.select(col("InvoiceDate"),
    datediff(current_date(),col("InvoiceDate")).alias("datediff")  
  ).show()

# COMMAND ----------

# meses entre datas
df.select(col("InvoiceDate"), 
    months_between(current_date(),col("InvoiceDate")).alias("months_between")  
  ).show()

# COMMAND ----------

# Extrai ano, mës, próximo dia, dia da semana.
df.select(col("InvoiceDate"), 
     year(col("InvoiceDate")).alias("year"), 
     month(col("InvoiceDate")).alias("month"), 
     next_day(col("InvoiceDate"),"Sunday").alias("next_day"), 
     weekofyear(col("InvoiceDate")).alias("weekofyear") 
  ).show()

# COMMAND ----------

# Dia da semana, dia do mës, dias do ano
df.select(col("InvoiceDate"),  
     dayofweek(col("InvoiceDate")).alias("dayofweek"), 
     dayofmonth(col("InvoiceDate")).alias("dayofmonth"), 
     dayofyear(col("InvoiceDate")).alias("dayofyear"), 
  ).show()

# COMMAND ----------

# imprime o timestamp atual
df.select(current_timestamp().alias("current_timestamp")
  ).show(1,truncate=False)

# COMMAND ----------

# retorna hora, minuto e segundo
df.select(col("InvoiceDate"), 
    hour(col("InvoiceDate")).alias("hour"), 
    minute(col("InvoiceDate")).alias("minute"),
    second(col("InvoiceDate")).alias("second") 
  ).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Condições com operadores boleanos

# COMMAND ----------

# Retorna linhas das colunas 'InvoiceNo' e 'Description' onde 'InvoiceNo' é diferente de 536365
from pyspark.sql.functions import col
df.where(col("InvoiceNo") != 536365)\
.select("InvoiceNo", "Description")\
.show(10)

# COMMAND ----------

# usando o operador boleando com um predicado em uma expressão.
df.where("InvoiceNo <> 536365").show(5)

# COMMAND ----------

# usando o operador boleando com um predicado em uma expressão.
df.where("InvoiceNo == 536365").show(5)

# COMMAND ----------

# Entendendo a ordem dos operadores boleanos
from pyspark.sql.functions import instr
priceFilter = col("UnitPrice") > 600
descripFilter = instr(df.Description, "POSTAGE") >= 1

# COMMAND ----------

# aplicando os operadores como filtros
df.where(df.StockCode.isin("DOT")).where(priceFilter | descripFilter).show()

# COMMAND ----------

# Create a view ou tabela temporária.
df.createOrReplaceTempView("dfTable")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Aplicando a mesmo código em SQL
# MAGIC SELECT * 
# MAGIC FROM dfTable 
# MAGIC WHERE StockCode in ("DOT")
# MAGIC AND(UnitPrice > 600 OR instr(Description, "POSTAGE") >= 1)

# COMMAND ----------

# Combinando filtros e operadores boleanos
from pyspark.sql.functions import instr
DOTCodeFilter = col("StockCode") == "DOT"
priceFilter = col("UnitPrice") > 600
descripFilter = instr(col("Description"), "POSTAGE") >= 1

# COMMAND ----------

# Combinando filtros e operadores boleanos
df.withColumn("isExpensive", DOTCodeFilter & (priceFilter | descripFilter))\
.where("isExpensive")\
.select("unitPrice", "isExpensive").show(5)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Aplicando as mesmas ideias usando SQL
# MAGIC SELECT UnitPrice, (StockCode = 'DOT' AND
# MAGIC (UnitPrice > 600 OR instr(Description, "POSTAGE") >= 1)) as isExpensive
# MAGIC FROM dfTable
# MAGIC WHERE (StockCode = 'DOT' AND
# MAGIC (UnitPrice > 600 OR instr(Description, "POSTAGE") >= 1))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Comparando a performance de SQL vs Python Apis

# COMMAND ----------

## utilizando SQL
sqlWay = spark.sql("""
SELECT StockCode, count(*)
FROM dfTable
GROUP BY StockCode
""")

# COMMAND ----------

# Utilizando Python
dataFrameWay = df.groupBy("StockCode").count()

# COMMAND ----------

# imprime o plano de execução do código
sqlWay.explain()

# COMMAND ----------

# imprime o plano de execução do código
dataFrameWay.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trabalhando com Joins

# COMMAND ----------

# Cria dataframes
pessoa = spark.createDataFrame([
(0, "João de Maria", 0, [100]),
(1, "Norma Maria", 1, [500, 250, 100]),
(2, "João de Deus", 1, [250, 100]),
(3, "Ana Maria Silva", 4, [250, 100])])\
.toDF("id", "name", "graduate_program", "spark_status")

programa_graduacao = spark.createDataFrame([
(0, "Masters", "School of Information", "UC Berkeley"),
(2, "Masters", "EECS", "UC Berkeley"),
(1, "Ph.D.", "EECS", "UC Berkeley")])\
.toDF("id", "degree", "department", "school")

status = spark.createDataFrame([
(500, "Vice President"),
(250, "PMC Member"),
(100, "Contributor")])\
.toDF("id", "status")

# COMMAND ----------

# cria tabelas para os dataframes criados acima
pessoa.createOrReplaceTempView("pessoa")
programa_graduacao.createOrReplaceTempView("programa_graduacao")
status.createOrReplaceTempView("status")

# COMMAND ----------

# imprime os dataframes criados
pessoa.show()
programa_graduacao.show()
status.show()

# COMMAND ----------

# cria um objeto com as chaves para fazer join
keys_join = pessoa["graduate_program"] == programa_graduacao['id']

# COMMAND ----------

# imprime objeto
type(keys_join)

# COMMAND ----------

# dataframe com inner join entre pessoa e programa de graduação
pessoa.join(programa_graduacao, keys_join).show()

# COMMAND ----------

# dataframe com inner join entre pessoa e programa de graduação
# sintaxe join(dataframealvo, condição-de-join, tipo-de-join)

pessoa.join(programa_graduacao, pessoa["graduate_program"] == programa_graduacao['id'], 'inner').show()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Inner join em SQL
# MAGIC SELECT * 
# MAGIC FROM pessoa INNER JOIN programa_graduacao
# MAGIC ON pessoa.graduate_program = programa_graduacao.id

# COMMAND ----------

# Outer joins: retorna null para linhas que não existam em um dos dataframes e retorna qualquer dado em qualquer dataframe caso exista a chave
join_type = "outer"
pessoa.join(programa_graduacao, keys_join, join_type).show()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Outer join em SQL
# MAGIC SELECT * 
# MAGIC FROM pessoa FULL OUTER JOIN programa_graduacao
# MAGIC ON pessoa.graduate_program = programa_graduacao.id

# COMMAND ----------

# Left joins: retorna null para linhas que não existam no dataframe da direita
join_type = "left_outer"
pessoa.join(programa_graduacao, keys_join, join_type).show()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Left outer join em SQL
# MAGIC SELECT * 
# MAGIC FROM pessoa LEFT OUTER JOIN programa_graduacao
# MAGIC ON pessoa.graduate_program = programa_graduacao.id

# COMMAND ----------

# Right joins: retorna null para linhas que não existam no dataframe a esquerda
join_type = "right_outer"
pessoa.join(programa_graduacao, keys_join, join_type).show()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Right join em SQL
# MAGIC SELECT * 
# MAGIC FROM pessoa RIGHT OUTER JOIN programa_graduacao
# MAGIC ON pessoa.graduate_program = programa_graduacao.id

# COMMAND ----------

# MAGIC %md
# MAGIC #### Condições

# COMMAND ----------

# altera a condição de join
keys_join = ((pessoa["graduate_program"] == programa_graduacao["id"]) & (pessoa["graduate_program"] > 0))
join_type = "inner"
pessoa.join(programa_graduacao, keys_join, join_type).show()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Inner join em SQL
# MAGIC -- adicionando uma codição where
# MAGIC SELECT * 
# MAGIC FROM pessoa INNER JOIN programa_graduacao
# MAGIC ON pessoa.graduate_program = programa_graduacao.id
# MAGIC WHERE pessoa.graduate_program > 0

# COMMAND ----------

# Condições mais complexas usando expressão

from pyspark.sql.functions import expr

pessoa.withColumnRenamed("id", "personId")\
.join(status, expr("array_contains(spark_status, id)")).show()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Condições mais complexas usando expressão feitas em SQL
# MAGIC SELECT * 
# MAGIC FROM
# MAGIC   (select id as personId
# MAGIC          ,name
# MAGIC          ,graduate_program
# MAGIC          ,spark_status
# MAGIC    FROM pessoa)
# MAGIC   INNER JOIN status ON array_contains(spark_status, id)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Trabalhando com UDFs
# MAGIC - Integração de código entre as APIs
# MAGIC - É preciso cuidado com performance dos códigos usando UDFs

# COMMAND ----------

from pyspark.sql.types import LongType
# define a função
def quadrado(s):
  return s * s

# COMMAND ----------

# registra no banco de dados do spark e define o tipo de retorno por padrão é stringtype
from pyspark.sql.types import LongType
spark.udf.register("Func_Py_Quadrado", quadrado, LongType())

# COMMAND ----------

# gera valores aleatórios
spark.range(1, 20).show()

# COMMAND ----------

# cria a visão View_temp
spark.range(1, 20).createOrReplaceTempView("View_temp")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Usando a função criada em python juntamente com código SQL
# MAGIC select id, 
# MAGIC        Func_Py_Quadrado(id) as id_ao_quadrado
# MAGIC from View_temp

# COMMAND ----------

# MAGIC %md
# MAGIC #### UDFs com Dataframes

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import LongType
# registra a Udf
Func_Py_Quadrado = udf(quadrado, LongType())

# COMMAND ----------

# cria um dataframe apartir da tabela temporária
df = spark.table("View_temp")

# COMMAND ----------

# imprime o dataframe
df.show(10)

# COMMAND ----------

# usando o dataframe juntamente com a Udf registrada
df.select("id", Func_Py_Quadrado("id").alias("id_quadrado")).show(20)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Koalas
# MAGIC - Koalas é um projeto de código aberto que fornece um substituto imediato para os pandas. 
# MAGIC - O pandas é comumente usado por ser um pacote que fornece estruturas de dados e ferramentas de análise de dados fáceis de usar para a linguagem de programação Python.
# MAGIC - O Koalas preenche essa lacuna fornecendo APIs equivalentes ao pandas que funcionam no Apache Spark. 
# MAGIC - Koalas é útil não apenas para usuários de pandas, mas também para usuários de PySpark.
# MAGIC   - Koalas suporta muitas tarefas que são difíceis de fazer com PySpark, por exemplo, plotar dados diretamente de um PySpark DataFrame.
# MAGIC - Koalas suporta SQL diretamente em seus dataframes.

# COMMAND ----------

import numpy as np
import pandas as pd
import databricks.koalas as ks

# COMMAND ----------

# cria um pandas DataFrame
pdf = pd.DataFrame({'A': np.random.rand(5),
                    'B': np.random.rand(5)})

# COMMAND ----------

# imprime um pandas dataframe
type(pdf)

# COMMAND ----------

# Cria um Koalas DataFrame
kdf = ks.DataFrame({'A': np.random.rand(5),
                    'B': np.random.rand(5)})

# COMMAND ----------

# imprime o tipo de dados
type(kdf)

# COMMAND ----------

# Cria um Koalas dataframe a partir de um pandas dataframe
kdf = ks.DataFrame(pdf)
type(kdf)

# COMMAND ----------

# métodos já conhecidos
pdf.head()

# COMMAND ----------

# métodos já conhecidos
kdf.head()

# COMMAND ----------

# método describe()
kdf.describe()

# COMMAND ----------

# ordenando um dataframe
kdf.sort_values(by='B')

# COMMAND ----------

# define configurações de layout de células
from databricks.koalas.config import set_option, get_option
ks.get_option('compute.max_rows')
ks.set_option('compute.max_rows', 2000)

# COMMAND ----------

# slice
kdf[['A', 'B']]

# COMMAND ----------

# slice
kdf[['A', 'B']]

# COMMAND ----------

# iloc
kdf.iloc[:3, 1:2]

# COMMAND ----------

# MAGIC %md
# MAGIC #### Usando funções python com dataframe koalas

# COMMAND ----------

# cria função python
def quadrado(x):
    return x ** 2

# COMMAND ----------

# habilita computação de dataframes e séries.
from databricks.koalas.config import set_option, reset_option
set_option("compute.ops_on_diff_frames", True)

# COMMAND ----------

# cria uma nova coluna a partir da função quadrado
kdf['C'] = kdf.A.apply(quadrado)

# COMMAND ----------

# visualizando o dataframe
kdf.head()

# COMMAND ----------

# agrupando dados
kdf.groupby('A').sum()

# COMMAND ----------

# agrupando mais de uma coluna
kdf.groupby(['A', 'B']).sum()

# COMMAND ----------

# para plotar gráfico diretamente na célula use o inline
%matplotlib inline

speed = [0.1, 17.5, 40, 48, 52, 69, 88]
lifespan = [2, 8, 70, 1.5, 25, 12, 28]

index = ['snail', 'pig', 'elephant',
         'rabbit', 'giraffe', 'coyote', 'horse']

kdf = ks.DataFrame({'speed': speed,
                   'lifespan': lifespan}, index=index)
kdf.plot.bar()

# COMMAND ----------

# MAGIC %md
# MAGIC **Usando SQL no Koalas**

# COMMAND ----------

# cria um dataframe Koalas
kdf = ks.DataFrame({'year': [1990, 1997, 2003, 2009, 2014],
                    'pig': [20, 18, 489, 675, 1776],
                    'horse': [4, 25, 281, 600, 1900]})

# COMMAND ----------

# Faz query no dataframe koalas
ks.sql("SELECT * FROM {kdf} WHERE pig > 100")

# COMMAND ----------

# cria um dataframe pandas
pdf = pd.DataFrame({'year': [1990, 1997, 2003, 2009, 2014],
                    'sheep': [22, 50, 121, 445, 791],
                    'chicken': [250, 326, 589, 1241, 2118]})

# COMMAND ----------

# Query com inner join entre dataframe pandas e koalas
ks.sql('''
    SELECT ks.pig, pd.chicken
    FROM {kdf} ks INNER JOIN {pdf} pd
    ON ks.year = pd.year
    ORDER BY ks.pig, pd.chicken''')

# COMMAND ----------

# converte koalas dataframe para Pyspark
kdf = ks.DataFrame({'A': [1, 2, 3, 4, 5], 'B': [10, 20, 30, 40, 50]})

# COMMAND ----------

pydf = kdf.to_spark()

# COMMAND ----------

type(pydf)

# COMMAND ----------


