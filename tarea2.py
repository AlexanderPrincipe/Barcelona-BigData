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


#*********************EVALUACION POR POSICION************************#
def EvaluacionArquero(fila):
	# Posicion 
	return (fila[21], fila[2:4] + [fila[11]] + [sum(map(int, fila[87:88]))])

posicion_rddArquero = rddArquero.map(lambda fila: EvaluacionArquero(fila))

def EvaluacionDefensa(fila):
	return (fila[21], fila[2:4] + [fila[11]] + [sum(map(int, fila[70:72] + fila[74:76] + fila[81:82]))])

posicion_rddDefensa = rddDefensa.map(lambda fila: EvaluacionDefensa(fila))

def EvaluacionMedioCampista(fila):
	return (fila[21], fila[2:4] + [fila[11]] + [sum(map(int, fila[56:57] + fila[60:63] + fila[67:68] + fila[76:77]))])

posicion_rddCC = rddMedioCampista.map(lambda fila: EvaluacionMedioCampista(fila))

def EvaluacionDelantero(fila):
	return (fila[21], fila[2:4] + [fila[11]] + [sum(map(int, fila[55:69] + fila[76:77]))])

posicion_rddDelantero = rddDelantero.map(lambda fila: EvaluacionDelantero(fila))

# Agrupando por posicion
grupo_rddArquero = posicion_rddArquero.groupByKey()
grupo_rddArquero = grupo_rddArquero.mapValues(list)

grupo_rddDefensa = posicion_rddDefensa.groupByKey()
grupo_rddDefensa = grupo_rddDefensa.mapValues(list)

grupo_rddDelantero = posicion_rddDelantero.groupByKey()
grupo_rddDelantero = grupo_rddDelantero.mapValues(list)

grupo_rddCC = posicion_rddCC.groupByKey()
grupo_rddCC = grupo_rddCC.mapValues(list)

# Ordenando por puntaje
def Ordenamiento_Puntaje(fila):
    # Retornar maximo 2 filas
	lista_ordenada = sorted(fila, key = lambda x: x[3], reverse = True) 
	return lista_ordenada[0:2]


# Ordenar por puntaje
def Ordenamiento(grupo_posicion):
	return (grupo_posicion[0], Ordenamiento_Puntaje(grupo_posicion[1]))

orden_rddArquero = grupo_rddArquero.map(lambda grupo: Ordenamiento(grupo))
orden_rddDefensa = grupo_rddDefensa.map(lambda grupo: Ordenamiento(grupo))
orden_rddCC = grupo_rddCC.map(lambda grupo: Ordenamiento(grupo))
orden_rddDelantero = grupo_rddDelantero.map(lambda grupo: Ordenamiento(grupo))

# Mostrar datos del jugador
def mostrar_fila(fila):
	print("Nombre:     %s" % fila[0])
	print("Edad:   %s" % fila[1])
	print("Valor:   %s" % fila[2])
	print("Puntaje:  %i" % fila[3])

# Mostrar los 2 mejores por posicion
def mostrar_grupo(grupo_jugadores):
	print("\n POSICION : %s" % grupo_jugadores[0])
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



