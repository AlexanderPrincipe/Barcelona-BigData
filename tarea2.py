import pyspark

# Datos
context = pyspark.SparkContext("local", "Tarea2")
rdd = context.textFile("data_tarea2.csv") \
    .map(lambda linea: linea.split(","))

# Remover Cabecera
def removerCabecera(idx, itr):
    return iter(list(itr)[1:]) if idx == 0 else itr

rdd = rdd.mapPartitionsWithIndex(removerCabecera)

# Solo jugadores menores a 24 años
rdd = rdd.filter(lambda fila: int(fila[3]) < 24)

# Creacion de rdd por posicion
rddArquero = rdd.filter(lambda fila : fila[21] == "GK")
rddDefensa = rdd.filter(lambda fila : fila[21] == "LB" or fila[21] == "CB" or fila[21] == "RB" or fila[21] == "LCB" or fila[21] == "RCB" or fila[21] == "LWB" or fila[21] == "RWB")
rddMedioCampista = rdd.filter(lambda fila : fila[21] == "CDM" or fila[21] == "CM" or fila[21] == "CAM" or fila[21] == "LAM" or fila[21] == "RAM" or fila[21] == "LCM" or fila[21] == "RCM" or fila[21] == "LDM" or fila[21] == "RDM" or fila[21] == "LM" or fila[21] == "RM")
rddDelantero = rdd.filter(lambda fila : fila[21] == "CF" or fila[21] == "LF" or fila[21] == "RF" or fila[21] == "LS" or fila[21] == "RS")


# Evaluacion Arquero
def EvaluacionArquero(fila):
	return (fila[21], fila[2:4] + [fila[11]] + [sum(map(int, fila[87:88]))])

posicion_rddArquero = rddArquero.map(lambda fila: EvaluacionArquero(fila))

# Evaluacion Defensa
def EvaluacionDefensa(fila):
	return (fila[21], fila[2:4] + [fila[11]] + [sum(map(int, fila[70:72] + fila[74:76] + fila[81:82]))])

posicion_rddDefensa = rddDefensa.map(lambda fila: EvaluacionDefensa(fila))

# Evaluacion MedioCampista
def EvaluacionMedioCampista(fila):
	return (fila[21], fila[2:4] + [fila[11]] + [sum(map(int, fila[56:57] + fila[60:63] + fila[67:68] + fila[76:77]))])

posicion_rddCC = rddMedioCampista.map(lambda fila: EvaluacionMedioCampista(fila))

# Evaluacion delantero
def EvaluacionDelantero(fila):
	return (fila[21], fila[2:4] + [fila[11]] + [sum(map(int, fila[55:69] + fila[76:77]))])

posicion_rddDelantero = rddDelantero.map(lambda fila: EvaluacionDelantero(fila))

# Agrupando por posicion
grupo_rddArquero = posicion_rddArquero.groupByKey().mapValues(list)
grupo_rddDefensa = posicion_rddDefensa.groupByKey().mapValues(list)
grupo_rddDelantero = posicion_rddDelantero.groupByKey().mapValues(list)
grupo_rddCC = posicion_rddCC.groupByKey().mapValues(list)

# Ordenando por puntaje
def Ordenamiento_Puntaje(fila):
    # Retornar maximo 2 filas
	lista_ordenada = sorted(fila, key = lambda x: x[3], reverse = 1) 
	return lista_ordenada[0:2]

orden_rddArquero = grupo_rddArquero.map(lambda x: (x[0], Ordenamiento_Puntaje(x[1])))
orden_rddDefensa = grupo_rddDefensa.map(lambda x: (x[0], Ordenamiento_Puntaje(x[1])))
orden_rddCC = grupo_rddCC.map(lambda x: (x[0], Ordenamiento_Puntaje(x[1])))
orden_rddDelantero = grupo_rddDelantero.map(lambda x: (x[0], Ordenamiento_Puntaje(x[1])))

# Mostrar datos del jugador
def mostrar_fila(fila):
	print("Nombre:   {}".format(fila[0]))
	print("Edad:     {}".format(fila[1]))
	print("Valor:    {}".format(fila[2]))
	print("Puntaje:  {}".format(fila[3]))

# Mostrar los 2 mejores por posicion
def mostrar_grupo(grupo_jugadores):
	print("\n POSICION : {}".format(grupo_jugadores[0]))
	lista = grupo_jugadores[1]
	print("\n 1. Titular")
	mostrar_fila(lista[0])
	if len(lista) == 2:
		print("\n 2. Suplente")
		mostrar_fila(lista[1])
	print("")

orden_rddArquero.foreach(lambda grupo: mostrar_grupo(grupo))
orden_rddDefensa.foreach(lambda grupo: mostrar_grupo(grupo))
orden_rddCC.foreach(lambda grupo: mostrar_grupo(grupo))
orden_rddDelantero.foreach(lambda grupo: mostrar_grupo(grupo))



