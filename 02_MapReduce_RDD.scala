import org.apache.spark.sql.SparkSession

object MapReduce_TrataPersonas {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Punto3_MapReduce_RDD_TrataPersonas")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    // Lectura del CSV respetando codificacion ISO-8859-1 y convirtiendo a RDD puro
    val rutaCsv = "C:/proyecto_bigdata/trata_personas.csv"
    val lineasRaw = spark.read
      .option("encoding", "ISO-8859-1")
      .textFile(rutaCsv)
      .rdd

    val cabecera = lineasRaw.first()

    // Parseo seguro respetando comillas y filtrando filas resumen (_TOTAL / _SUB TOTAL)
    // Indices: 0:AÑO, 1:MES, 2:CCDD, 3:lugar, 4:PROVINCIA, 5:DISTRITO, 6:COMISARIA, 7:categoria, 8:comision, 9:Valor
    val datosRDD = lineasRaw
      .filter(_ != cabecera)
      .map(linea => linea.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1).map(_.replace("\"", "").trim))
      .filter(cols => cols.length >= 10 && !cols(8).toUpperCase.contains("TOTAL"))
      .map { cols =>
        val anio = cols(0).toInt
        val mes = cols(1).toInt
        val depto = cols(3).toUpperCase
        val comisaria = if (cols(6).isEmpty) "NO ESPECIFICADO" else cols(6).toUpperCase
        val categoria = cols(7).toUpperCase
        val comision = cols(8).toUpperCase
        val valor = try { cols(9).toInt } catch { case _: Exception => 0 }
        (anio, mes, depto, comisaria, categoria, comision, valor)
      }
      .cache()

    println("=========================================================================")
    println("PUNTO 3a: AGRUPAR POR DEPARTAMENTO Y HALLAR MAYOR Y MENOR VALOR (MAPREDUCE)")
    println("=========================================================================")
    // Pregunta: ¿Cual es el registro individual con mayor y menor cantidad de denuncias por departamento?
    // Etapa 1 (Map): Emitir (departamento -> (valor, valor))
    // Etapa 2 (ReduceByKey): Reducir conservando el maximo y el minimo por departamento
    val maxMinDeptoRDD = datosRDD
      .map { case (_, _, depto, _, _, _, valor) => (depto, (valor, valor)) }
      .reduceByKey { case ((max1, min1), (max2, min2)) =>
        (math.max(max1, max2), math.min(min1, min2))
      }
      .sortBy(_._2._1, ascending = false)

    maxMinDeptoRDD.collect().foreach { case (depto, (maxVal, minVal)) =>
      println(f"Departamento: $depto%-25s | Mayor registro: $maxVal%4d | Menor registro: $minVal%4d")
    }

    println("\n=========================================================================")
    println("PUNTO 3b: PROMEDIO, MEDIANA, DESVIACION ESTANDAR, MAYOR Y MENOR (MAPREDUCE)")
    println("=========================================================================")
    // Pregunta: ¿Cuales son los estadisticos descriptivos de las denuncias positivas (>0) en el periodo 2017-2025?
    val valoresPositivosRDD = datosRDD
      .map(_._7.toDouble)
      .filter(_ > 0.0)
      .cache()

    // MapReduce 1: Conteo, Suma, Suma de cuadrados, Maximo y Minimo en una sola pasada
    val (n, suma, sumaCuad, mayor, menor) = valoresPositivosRDD
      .map(v => (1L, v, v * v, v, v))
      .reduce { case ((c1, s1, sq1, max1, min1), (c2, s2, sq2, max2, min2)) =>
        (c1 + c2, s1 + s2, sq1 + sq2, math.max(max1, max2), math.min(min1, min2))
      }

    val promedio = suma / n
    val varianza = (sumaCuad / n) - (promedio * promedio)
    val desviacion = math.sqrt(varianza)

    // MapReduce 2: Ordenamiento distribuido con indice para calcular la mediana exacta
    val ordenadosConIndice = valoresPositivosRDD
      .sortBy(v => v)
      .zipWithIndex()
      .map { case (v, idx) => (idx, v) }

    val mediana = if (n % 2 == 1) {
      ordenadosConIndice.lookup(n / 2).head
    } else {
      val m1 = ordenadosConIndice.lookup((n / 2) - 1).head
      val m2 = ordenadosConIndice.lookup(n / 2).head
      (m1 + m2) / 2.0
    }

    println(f"Total registros positivos (N) : $n%d")
    println(f"Promedio (Media aritmetica)   : $promedio%.4f")
    println(f"Mediana (Valor central)       : $mediana%.4f")
    println(f"Desviacion Estandar           : $desviacion%.4f")
    println(f"Valor Mayor                   : $mayor%.2f")
    println(f"Valor Menor                   : $menor%.2f")

    println("\n=========================================================================")
    println("PUNTO 3c - CONSULTA 1 (TRIPLE MAPREDUCE CON DECIMALES)")
    println("Objetivo: Porcentaje (%) de participacion nacional de denuncias por departamento")
    println("=========================================================================")
    // MapReduce 1: Total de denuncias por departamento
    val totalPorDepto = datosRDD
      .map { case (_, _, depto, _, _, _, valor) => (depto, valor.toDouble) }
      .reduceByKey(_ + _)

    // MapReduce 2: Gran total nacional usando Map + Reduce
    val granTotalNacional = totalPorDepto
      .map(_._2)
      .reduce(_ + _)

    // MapReduce 3: Calculo de porcentaje decimal sobre el total y consolidacion
    val participacionDepto = totalPorDepto
      .map { case (depto, sumaDepto) =>
        val porcentaje = (sumaDepto / granTotalNacional) * 100.0
        (depto, (sumaDepto, porcentaje))
      }
      .reduceByKey { case ((s1, p1), (s2, p2)) => (s1 + s2, p1 + p2) }
      .sortBy(_._2._2, ascending = false)

    participacionDepto.collect().foreach { case (depto, (total, pct)) =>
      println(f"Depto: $depto%-25s | Total: ${total.toInt}%5d | Participacion: $pct%6.2f%%")
    }

    println("\n=========================================================================")
    println("PUNTO 3c - CONSULTA 2 (TRIPLE MAPREDUCE CON DECIMALES)")
    println("Objetivo: Promedio de denuncias por comisaria activa en cada anio (2017-2025)")
    println("=========================================================================")
    // MapReduce 1: Agrupar por (anio, comisaria) y sumar sus denuncias
    val denunciasPorAnioComisaria = datosRDD
      .filter(_._7 > 0)
      .map { case (anio, _, _, comisaria, _, _, valor) => ((anio, comisaria), valor.toDouble) }
      .reduceByKey(_ + _)

    // MapReduce 2: Mapear a (anio -> (sumaDenuncias, 1 comisaria)) y reducir por anio
    val resumenAnualComisarias = denunciasPorAnioComisaria
      .map { case ((anio, _), sumaCom) => (anio, (sumaCom, 1)) }
      .reduceByKey { case ((s1, c1), (s2, c2)) => (s1 + s2, c1 + c2) }

    // MapReduce 3: Calcular promedio decimal de denuncias/comisaria y consolidar por anio
    val promedioAnualComisaria = resumenAnualComisarias
      .map { case (anio, (totalDen, numCom)) => (anio, totalDen / numCom.toDouble) }
      .reduceByKey((a, _) => a)
      .sortByKey(ascending = true)

    promedioAnualComisaria.collect().foreach { case (anio, prom) =>
      println(f"Anio: $anio%d | Promedio denuncias por comisaria/dependencia: $prom%.3f")
    }

    println("\n=========================================================================")
    println("PUNTO 3c - CONSULTA 3 (TRIPLE MAPREDUCE CON DECIMALES)")
    println("Objetivo: Peso porcentual (%) de cada finalidad de explotacion (F1_)")
    println("=========================================================================")
    val finesRDD = datosRDD
      .filter { case (_, _, _, _, _, comision, valor) => comision.startsWith("F1_") && valor > 0 }

    // MapReduce 1: Total de denuncias por finalidad (codigo_comision)
    val totalPorFin = finesRDD
      .map { case (_, _, _, _, _, comision, valor) => (("FINALIDAD", comision), valor.toDouble) }
      .reduceByKey(_ + _)

    // MapReduce 2: Total general de denuncias en todas las finalidades F1_
    val totalCategoriaFines = totalPorFin
      .map { case ((grupo, _), suma) => (grupo, suma) }
      .reduceByKey(_ + _)

    // MapReduce 3: Mapear por grupo, hacer Join distribuido y reducir para obtener % decimal
    val porcentajePorFin = totalPorFin
      .map { case ((grupo, comision), sumaFin) => (grupo, (comision, sumaFin)) }
      .join(totalCategoriaFines)
      .map { case (_, ((comision, sumaFin), totalCat)) =>
        (comision, (sumaFin, (sumaFin / totalCat) * 100.0))
      }
      .reduceByKey { case ((s1, p1), (s2, p2)) => (s1 + s2, p1 + p2) }
      .sortBy(_._2._2, ascending = false)

    porcentajePorFin.collect().foreach { case (fin, (total, pct)) =>
      println(f"Finalidad: $fin%-38s | Casos: ${total.toInt}%5d | Peso: $pct%6.2f%%")
    }
  }
}