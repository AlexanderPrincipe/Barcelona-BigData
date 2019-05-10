import pyspark

sparkContext = pyspark.SparkContext("local", "Tarea2")
rdd = sparkContext.textFile("data-tarea2.csv") \
    .map(lambda linea: linea.split(","))

def removerCabecera(idx, itr):
    return iter(list(itr)[1:]) if idx == 0 else itr

rddSinCabecera = rdd.mapPartitionsWithIndex(removerCabecera)

total = rddSinCabecera.count()
print (total)

rdd23 = rddSinCabecera.filter(lambda fila : int(fila[3]) < 24)
total23 = rdd23.count() # Action
print("Total Jugadores 23:{}".format(total23))
