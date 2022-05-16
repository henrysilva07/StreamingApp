import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import * 
#from pyspark.sql.types import StructType , StructField , StringType , IntegerType

# Definindo a nossa aplicação Spark
spark = SparkSession \
        .builder \
        .appName("StructuredStreamingHenryApp")\
        .getOrCreate()
    

# diretório com os arquivos csv 
diretorio = "/home/henry/landing"

# determinando o schema 


schema_df = "DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, COUNT INT"



df_streaming = spark.readStream \
            .schema(schema_df)\
            .option("maxFilesPerTrigger", "1")\
            .option("header", "True")\
            .option("delimiter", ",")\
            .csv(diretorio)    

# Processa os dados em streaming
resultado = df_streaming.groupBy("ORIGIN_COUNTRY_NAME").sum("COUNT")

# Escreve a saída do processamento para o console
saida = resultado.writeStream\
        .outputMode("complete")\
        .format("console")\
        .start()

# imprime o resultado até que a aplicação sejaencerrada
saida.awaitTermination()
