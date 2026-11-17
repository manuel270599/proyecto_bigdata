import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import java.util.Properties

object SparkSQL_TrataPersonas {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Punto5_SparkSQL_TrataPersonas")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    // 1. CONEXION Y LECTURA DE TABLAS DESDE POSTGRESQL
    val jdbcUrl = "jdbc:postgresql://localhost:5432/bd_trata_personas"
    val dbProps = new Properties()
    dbProps.setProperty("user", "postgres")
    dbProps.setProperty("password", "josemanuel270599") // 
    dbProps.setProperty("driver", "org.postgresql.Driver")

    val dfTiempo = spark.read.jdbc(jdbcUrl, "dim_tiempo", dbProps).cache()
    val dfUbicacion = spark.read.jdbc(jdbcUrl, "dim_ubicacion", dbProps).cache()
    val dfComisaria = spark.read.jdbc(jdbcUrl, "dim_comisaria", dbProps).cache()
    val dfTipificacion = spark.read.jdbc(jdbcUrl, "dim_tipificacion", dbProps).cache()
    val dfHechos = spark.read.jdbc(jdbcUrl, "hechos_denuncias", dbProps).cache()

    // DataFrame consolidado para consultas de API DataFrame (5a, 5b, 5d)
    val baseDF = dfHechos
      .join(dfTiempo, "id_tiempo")
      .join(dfUbicacion, "id_ubicacion")
      .join(dfComisaria, "id_comisaria")
      .join(dfTipificacion, "id_tipificacion")
      .cache()

    // =========================================================================
    // PARTE I: API DE SCALA - SPARK SQL (DATAFRAMES)
    // =========================================================================

    println("=========================================================================")
    println("5a - CONSULTA 1: GroupBy y Filter (2+ columnas)")
    println("Pregunta: ¿Cuantas denuncias por modalidad de captacion (C_) hubo en Lima y Cusco desde 2021?")
    println("=========================================================================")
    val q5a_1 = baseDF
      .filter(
        col("codigo_comision").startsWith("C_") &&
        col("departamento").isin("LIMA", "CUSCO") &&
        col("anio") >= 2021 &&
        col("valor") > 0
      )
      .groupBy("departamento", "anio", "descripcion_detalle")
      .agg(sum("valor").as("total_denuncias"))
      .orderBy(col("departamento"), col("anio"), desc("total_denuncias"))
    q5a_1.show(20, truncate = false)

    println("=========================================================================")
    println("5a - CONSULTA 2: GroupBy y Filter (2+ columnas)")
    println("Pregunta: ¿Cual es la evolucion trimestral de denuncias por perfil de edad y sexo (P3_) entre 2020 y 2024?")
    println("=========================================================================")
    val q5a_2 = baseDF
      .filter(
        col("codigo_comision").startsWith("P3_") &&
        col("anio").between(2020, 2024) &&
        col("valor") > 0
      )
      .groupBy("anio", "trimestre", "descripcion_detalle")
      .agg(sum("valor").as("cantidad_victimas"))
      .orderBy(col("anio"), col("trimestre"), desc("cantidad_victimas"))
    q5a_2.show(20, truncate = false)

    println("=========================================================================")
    println("5a - CONSULTA 3: GroupBy y Filter (2+ columnas)")
    println("Pregunta: ¿Cuales son los medios empleados (M_) mas frecuentes en departamentos de frontera (Tumbes, Piura, Tacna, Puno, Madre de Dios, Loreto)?")
    println("=========================================================================")
    val q5a_3 = baseDF
      .filter(
        col("codigo_comision").startsWith("M_") &&
        col("departamento").isin("TUMBES", "PIURA", "TACNA", "PUNO", "MADRE DE DIOS", "LORETO") &&
        col("valor") > 0
      )
      .groupBy("departamento", "descripcion_detalle")
      .agg(sum("valor").as("total_casos"))
      .orderBy(col("departamento"), desc("total_casos"))
    q5a_3.show(20, truncate = false)

    println("=========================================================================")
    println("5a - CONSULTA 4: GroupBy y Filter (2+ columnas)")
    println("Pregunta: ¿Cual es el destino de las denuncias (D_) por provincia dentro de la region Lima?")
    println("=========================================================================")
    val q5a_4 = baseDF
      .filter(
        col("codigo_comision").startsWith("D_") &&
        col("departamento") === "LIMA" &&
        col("valor") > 0
      )
      .groupBy("provincia", "descripcion_detalle")
      .agg(sum("valor").as("total_derivaciones"))
      .orderBy(desc("total_derivaciones"))
    q5a_4.show(20, truncate = false)

    println("=========================================================================")
    println("5b - CONSULTA 1: Promedio de una columna (avg)")
    println("Pregunta: ¿Cual es el promedio mensual de denuncias registradas por tipo de explotacion sexual (ES_ / F1_EXPLOTACION SEXUAL)?")
    println("=========================================================================")
    val q5b_1 = baseDF
      .filter(col("valor") > 0 && col("codigo_comision").contains("SEXUAL"))
      .groupBy("codigo_comision", "descripcion_detalle")
      .agg(
        count("id_hecho").as("meses_con_registro"),
        round(avg("valor"), 2).as("promedio_denuncias_por_registro")
      )
      .orderBy(desc("promedio_denuncias_por_registro"))
    q5b_1.show(15, truncate = false)

    println("=========================================================================")
    println("5b - CONSULTA 2: Ordenamiento decreciente (orderBy desc)")
    println("Pregunta: ¿Cuales son los 15 distritos con mayor acumulado historico de denuncias en todo el pais?")
    println("=========================================================================")
    val q5b_2 = baseDF
      .filter(col("distrito") =!= "NO ESPECIFICADO")
      .groupBy("departamento", "provincia", "distrito")
      .agg(sum("valor").as("acumulado_historico"))
      .orderBy(desc("acumulado_historico"))
    q5b_2.show(15, truncate = false)

    println("=========================================================================")
    println("5c - CONSULTA 1: INNER JOIN")
    println("Pregunta: Total de denuncias por trimestre cruzando hechos_denuncias y dim_tiempo (INNER JOIN)")
    println("=========================================================================")
    val q5c_inner = dfHechos
      .join(dfTiempo, Seq("id_tiempo"), "inner")
      .groupBy("anio", "trimestre")
      .agg(sum("valor").as("denuncias_trimestrales"))
      .orderBy("anio", "trimestre")
    q5c_inner.show(15, truncate = false)

    println("=========================================================================")
    println("5c - CONSULTA 2: LEFT OUTER JOIN")
    println("Pregunta: Verificar cobertura de todas las tipificaciones del catalogo frente a denuncias ocurridas en 2025 (LEFT JOIN)")
    println("=========================================================================")
    val hechos2025 = dfHechos.join(dfTiempo.filter(col("anio") === 2025), "id_tiempo")
    val q5c_left = dfTipificacion
      .join(hechos2025, Seq("id_tipificacion"), "left_outer")
      .groupBy("categoria", "descripcion_detalle")
      .agg(coalesce(sum("valor"), lit(0)).as("denuncias_2025"))
      .orderBy(desc("denuncias_2025"))
    q5c_left.show(15, truncate = false)

    println("=========================================================================")
    println("5c - CONSULTA 3: RIGHT OUTER JOIN")
    println("Pregunta: Auditar todas las ubicaciones geograficas registradas frente a hechos con valor > 10 (RIGHT JOIN)")
    println("=========================================================================")
    val hechosAltos = dfHechos.filter(col("valor") > 10)
    val q5c_right = hechosAltos
      .join(dfUbicacion, Seq("id_ubicacion"), "right_outer")
      .groupBy("departamento", "provincia")
      .agg(
        count("id_hecho").as("eventos_criticos"),
        coalesce(sum("valor"), lit(0)).as("suma_eventos_criticos")
      )
      .orderBy(desc("suma_eventos_criticos"))
    q5c_right.show(15, truncate = false)

    println("=========================================================================")
    println("5c - CONSULTA 4: FULL OUTER JOIN")
    println("Pregunta: Comparar dependencias policiales activas en 2018 vs dependencias activas en 2022 (FULL OUTER JOIN)")
    println("=========================================================================")
    val com2018 = baseDF.filter(col("anio") === 2018 && col("valor") > 0)
      .groupBy("nombre_comisaria").agg(sum("valor").as("casos_2018"))
    val com2022 = baseDF.filter(col("anio") === 2022 && col("valor") > 0)
      .groupBy("nombre_comisaria").agg(sum("valor").as("casos_2022"))
    val q5c_full = com2018
      .join(com2022, Seq("nombre_comisaria"), "full_outer")
      .na.fill(0, Seq("casos_2018", "casos_2022"))
      .orderBy(desc("casos_2022"))
    q5c_full.show(15, truncate = false)

    println("=========================================================================")
    println("5d - CONSULTA 1: Funciones Spark SQL (concat_ws, upper)")
    println("Pregunta: Generar etiqueta geografica unificada (Depto - Prov - Dist) y calcular total de denuncias")
    println("=========================================================================")
    val q5d_1 = baseDF
      .filter(col("valor") > 0)
      .withColumn("ruta_geografica", concat_ws(" > ", col("departamento"), col("provincia"), col("distrito")))
      .groupBy("ruta_geografica")
      .agg(sum("valor").as("total_casos"))
      .orderBy(desc("total_casos"))
    q5d_1.show(15, truncate = false)

    println("=========================================================================")
    println("5d - CONSULTA 2: Funciones Spark SQL (when, otherwise)")
    println("Pregunta: Clasificar los registros segun nivel de severidad (Bajo: 1-2, Medio: 3-10, Critico: >10)")
    println("=========================================================================")
    val q5d_2 = baseDF
      .filter(col("valor") > 0)
      .withColumn("nivel_severidad",
        when(col("valor") <= 2, "1. BAJO (1-2 casos)")
          .when(col("valor").between(3, 10), "2. MEDIO (3-10 casos)")
          .otherwise("3. CRITICO (>10 casos)")
      )
      .groupBy("nivel_severidad")
      .agg(
        count("id_hecho").as("frecuencia_registros"),
        sum("valor").as("total_denuncias")
      )
      .orderBy("nivel_severidad")
    q5d_2.show(10, truncate = false)

    println("=========================================================================")
    println("5d - CONSULTA 3: Funciones Spark SQL (round, stddev, max, min)")
    println("Pregunta: Dispersion estadistica (promedio, desviacion estandar, maximo y minimo) por anio")
    println("=========================================================================")
    val q5d_3 = baseDF
      .filter(col("valor") > 0)
      .groupBy("anio")
      .agg(
        round(avg("valor"), 2).as("promedio"),
        round(stddev("valor"), 2).as("desv_estandar"),
        max("valor").as("maximo"),
        min("valor").as("minimo")
      )
      .orderBy("anio")
    q5d_3.show(15, truncate = false)

    println("=========================================================================")
    println("5d - CONSULTA 4: Funciones Spark SQL (substring, length)")
    println("Pregunta: Extraer el prefijo tecnico del codigo de comision y contar volumen por grupo de variable")
    println("=========================================================================")
    val q5d_4 = baseDF
      .filter(col("valor") > 0)
      .withColumn("prefijo_variable", substring_index(col("codigo_comision"), "_", 1))
      .groupBy("prefijo_variable", "categoria")
      .agg(
        countDistinct("codigo_comision").as("subcategorias"),
        sum("valor").as("total_denuncias")
      )
      .orderBy(desc("total_denuncias"))
    q5d_4.show(15, truncate = false)

    // =========================================================================
    // PARTE II: VISTAS TEMPORALES Y SPARK SQL PURO (5e, 5f y 5g)
    // =========================================================================
    dfTiempo.createOrReplaceTempView("v_tiempo")
    dfUbicacion.createOrReplaceTempView("v_ubicacion")
    dfComisaria.createOrReplaceTempView("v_comisaria")
    dfTipificacion.createOrReplaceTempView("v_tipificacion")
    dfHechos.createOrReplaceTempView("v_hechos")

    println("=========================================================================")
    println("5e - CONSULTA 1 (Vista Temporal - INNER JOIN)")
    println("Pregunta: Top 15 comisarias/dependencias con mayor numero de denuncias de explotacion laboral")
    println("=========================================================================")
    val q5e_1 = spark.sql("""
      SELECT c.nombre_comisaria,
             t.descripcion_detalle AS tipo_explotacion,
             SUM(h.valor) AS total_denuncias
      FROM v_hechos h
      INNER JOIN v_comisaria c ON h.id_comisaria = c.id_comisaria
      INNER JOIN v_tipificacion t ON h.id_tipificacion = t.id_tipificacion
      WHERE t.codigo_comision LIKE '%LABORAL%'
        AND c.nombre_comisaria <> 'NO ESPECIFICADO'
      GROUP BY c.nombre_comisaria, t.descripcion_detalle
      ORDER BY total_denuncias DESC
      LIMIT 15
    """)
    q5e_1.show(15, truncate = false)

    println("=========================================================================")
    println("5e - CONSULTA 2 (Vista Temporal - LEFT JOIN)")
    println("Pregunta: Todos los meses del calendario (v_tiempo) y su volumen de denuncias en el departamento de Madre de Dios")
    println("=========================================================================")
    val q5e_2 = spark.sql("""
      SELECT ti.anio,
             ti.nombre_mes,
             COALESCE(SUM(h.valor), 0) AS denuncias_madre_de_dios
      FROM v_tiempo ti
      LEFT JOIN (
          SELECT h1.id_tiempo, h1.valor
          FROM v_hechos h1
          INNER JOIN v_ubicacion u1 ON h1.id_ubicacion = u1.id_ubicacion
          WHERE u1.departamento = 'MADRE DE DIOS'
      ) h ON ti.id_tiempo = h.id_tiempo
      GROUP BY ti.anio, ti.mes, ti.nombre_mes
      ORDER BY ti.anio ASC, ti.mes ASC
    """)
    q5e_2.show(20, truncate = false)

    println("=========================================================================")
    println("5e - CONSULTA 3 (Vista Temporal - RIGHT JOIN)")
    println("Pregunta: Listado completo de departamentos (v_ubicacion) cruzado con denuncias de mendicidad o venta de ninos (RIGHT JOIN)")
    println("=========================================================================")
    val q5e_3 = spark.sql("""
      SELECT u.departamento,
             COALESCE(SUM(h.valor), 0) AS casos_mendicidad_venta
      FROM (
          SELECT h1.id_ubicacion, h1.valor
          FROM v_hechos h1
          INNER JOIN v_tipificacion t1 ON h1.id_tipificacion = t1.id_tipificacion
          WHERE t1.codigo_comision LIKE '%MENDICIDAD%' OR t1.codigo_comision LIKE '%VENTA%'
      ) h
      RIGHT JOIN v_ubicacion u ON h.id_ubicacion = u.id_ubicacion
      GROUP BY u.departamento
      ORDER BY casos_mendicidad_venta DESC
    """)
    q5e_3.show(25, truncate = false)

    println("=========================================================================")
    println("5f - CONSULTA 1 (Vista Temporal - ORDER BY + BETWEEN + IN)")
    println("Pregunta: Evolucion anual en la macroregion sur (Arequipa, Cusco, Puno, Tacna, Moquegua) entre 2019 y 2025")
    println("=========================================================================")
    val q5f_1 = spark.sql("""
      SELECT u.departamento,
             ti.anio,
             SUM(h.valor) AS total_denuncias
      FROM v_hechos h
      INNER JOIN v_ubicacion u ON h.id_ubicacion = u.id_ubicacion
      INNER JOIN v_tiempo ti ON h.id_tiempo = ti.id_tiempo
      WHERE u.departamento IN ('AREQUIPA', 'CUSCO', 'PUNO', 'TACNA', 'MOQUEGUA')
        AND ti.anio BETWEEN 2019 AND 2025
      GROUP BY u.departamento, ti.anio
      ORDER BY u.departamento ASC, ti.anio DESC
    """)
    q5f_1.show(35, truncate = false)

    println("=========================================================================")
    println("5f - CONSULTA 2 (Vista Temporal - ORDER BY + LIKE + HAVING)")
    println("Pregunta: Modalidades de captacion por redes sociales o internet (LIKE) con mas de 50 denuncias acumuladas")
    println("=========================================================================")
    val q5f_2 = spark.sql("""
      SELECT t.categoria,
             t.codigo_comision,
             t.descripcion_detalle,
             SUM(h.valor) AS acumulado_casos
      FROM v_hechos h
      INNER JOIN v_tipificacion t ON h.id_tipificacion = t.id_tipificacion
      WHERE t.codigo_comision LIKE 'C_%'
        AND (t.descripcion_detalle LIKE '%REDES%' OR t.descripcion_detalle LIKE '%INTERNET%' OR t.descripcion_detalle LIKE '%OFERTA%')
      GROUP BY t.categoria, t.codigo_comision, t.descripcion_detalle
      HAVING SUM(h.valor) > 50
      ORDER BY acumulado_casos DESC
    """)
    q5f_2.show(20, truncate = false)

    println("=========================================================================")
    println("5f - CONSULTA 3 (Vista Temporal - ORDER BY + Operadores relacionales)")
    println("Pregunta: Meses pico (denuncias > 100) en Lima Metropolitana ordenados de mayor a menor impacto")
    println("=========================================================================")
    val q5f_3 = spark.sql("""
      SELECT ti.anio,
             ti.nombre_mes,
             u.provincia,
             SUM(h.valor) AS denuncias_mes
      FROM v_hechos h
      INNER JOIN v_tiempo ti ON h.id_tiempo = ti.id_tiempo
      INNER JOIN v_ubicacion u ON h.id_ubicacion = u.id_ubicacion
      WHERE u.departamento = 'LIMA'
        AND u.provincia = 'LIMA'
      GROUP BY ti.anio, ti.nombre_mes, u.provincia
      HAVING SUM(h.valor) > 100
      ORDER BY denuncias_mes DESC, ti.anio DESC
    """)
    q5f_3.show(25, truncate = false)

    // =========================================================================
    // 5g: EXPORTAR RESULTADOS DE CONSULTAS 5e Y 5f A EXCEL USANDO SPARK
    // =========================================================================
    val rutaExcel = "C:/proyecto_bigdata/Resultados_Consultas_5e_5f.xlsx"

    def guardarHojaExcel(df: DataFrame, nombreHoja: String): Unit = {
      df.write
        .format("com.crealytics.spark.excel")
        .option("header", "true")
        .option("dataAddress", s"'$nombreHoja'!A1")
        .mode("append")
        .save(rutaExcel)
    }

    guardarHojaExcel(q5e_1, "5e_1_InnerJoin_Laboral")
    guardarHojaExcel(q5e_2, "5e_2_LeftJoin_MadreDios")
    guardarHojaExcel(q5e_3, "5e_3_RightJoin_Mendicidad")
    guardarHojaExcel(q5f_1, "5f_1_OrderBy_MacroSur")
    guardarHojaExcel(q5f_2, "5f_2_OrderBy_Captacion")
    guardarHojaExcel(q5f_3, "5f_3_OrderBy_PicosLima")

    println(s"\n>>> EXITO! Archivo Excel generado correctamente en: $rutaExcel")
  }
}