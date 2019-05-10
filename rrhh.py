import pyspark

sparkContext = pyspark.SparkContext("local", "RRHHEjercicio")
rdd = sparkContext.textFile("core_dataset.csv") \
	.map(lambda linea: linea.split(","))

def removerCabecera(idx, itr):
	return iter(list(itr)[1:]) if idx == 0 else itr

rddSinCabecera = rdd.mapPartitionsWithIndex(removerCabecera)

total = rddSinCabecera.count() # Action
print("Total:{}".format(total))

rddMujeres = rddSinCabecera.filter(lambda fila : fila[7] == "Female")
totalMujeres = rddMujeres.count() # Action
print("Total Mujeres:{}".format(totalMujeres))

print("Promedio Historico Mujeres: {}".format((float(totalMujeres)/total)*100))

# Analisis de diversidad por raza

rddEmpleadosActuales = rddSinCabecera.filter(
	lambda fila : fila[15] == "Active") # Empleados que estan trabajando
rddEmpleadosKV = rddEmpleadosActuales.map(lambda fila : (fila[11], 1))

def contador(value1, value2):
	return value1 + value2

rddCantidadPorRaza = rddEmpleadosKV.reduceByKey(contador)

rddCantidadPorRaza.foreach(lambda fila : print(fila)) #action

# Analisis por edad

def categorizarEdad(fila):
	if  fila[6] == "Male" or fila[6] == "Female" or fila[6] == '':
		return fila[5]
	else:
		if int(fila[6]) < 18:
			return "A"
		elif int(fila[6]) >= 18 and int(fila[6]) < 25:
			return "B"
		elif int(fila[6]) >= 25 and int(fila[6]) <= 40:
			return "C"
		else:
			return "D"

rddCategoriasEdad = rddSinCabecera.map(categorizarEdad)
rddCatFinal=rddCategoriasEdad.map(lambda fila : (fila, 1)).reduceByKey(
	lambda v1, v2 : v1 + v2)

rddCatFinal.foreach(lambda fila : print(fila)) #action
   


 








