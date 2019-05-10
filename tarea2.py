import pyspark

sparkContext = pyspark.SparkContext("local", "Tarea2")
rdd = sparkContext.textFile("data-tarea2.csv") \
    .map(lambda linea: linea.split(","))

def removerCabecera(idx, itr):
    return iter(list(itr)[1:]) if idx == 0 else itr

rddSinCabecera = rdd.mapPartitionsWithIndex(removerCabecera)

total = rddSinCabecera.count()
print (total)
