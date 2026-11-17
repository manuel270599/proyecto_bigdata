import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.sql.functions._
import java.util.Properties

object ETL_TrataPersonas {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("ETL_Trata_Personas_PostgreSQL")
      .master("local[*]")
      .getOrCreate()

    val jdbcUrl = "jdbc:postgresql://localhost:5432/bd_trata_personas"
    val dbProps = new Properties()
    dbProps.setProperty("user", "postgres")
    dbProps.setProperty("password", "josemanuel270599") // 
    dbProps.setProperty("driver", "org.postgresql.Driver")

    // 1. EXTRACCION (E)
    val rutaCsv = "C:/proyecto_bigdata/trata_personas.csv"
    val rawDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("encoding", "ISO-8859-1")
      .csv(rutaCsv)

    println(s">>> Registros crudos leidos: ${rawDF.count()}")

    // 2. TRANSFORMACION Y LIMPIEZA (T)
    // Renombramos por posicion con .toDF() para evitar problemas con tildes/enies en Windows
    val renamedDF = rawDF.toDF(
      "anio",
      "mes",
      "ccdd",
      "departamento",
      "provincia",
      "distrito",
      "nombre_comisaria",
      "categoria",
      "codigo_comision",
      "valor_raw"
    )

    // Eliminar filas resumen (_TOTAL y _SUB TOTAL) para no duplicar metricas
    val cleanDF = renamedDF
      .filter(!upper(col("codigo_comision")).contains("TOTAL"))
      .withColumn("departamento", trim(upper(col("departamento"))))
      .withColumn("provincia", coalesce(trim(upper(col("provincia"))), lit("NO ESPECIFICADO")))
      .withColumn("distrito",
        when(trim(upper(col("distrito"))) === "CUZCO", lit("CUSCO"))
          .otherwise(coalesce(trim(upper(col("distrito"))), lit("NO ESPECIFICADO")))
      )
      .withColumn("nombre_comisaria",
        regexp_replace(coalesce(trim(upper(col("nombre_comisaria"))), lit("NO ESPECIFICADO")), "\\s+", " ")
      )
      .withColumn("categoria", trim(upper(col("categoria"))))
      .withColumn("codigo_comision", trim(upper(col("codigo_comision"))))
      .withColumn("valor", coalesce(col("valor_raw").cast("int"), lit(0)))

    println(s">>> Registros limpios (sin TOTALES): ${cleanDF.count()}")

    // 3. CARGA DE DIMENSIONES (L)
    val dimTiempoDF = cleanDF.select("anio", "mes").distinct()
      .withColumn("nombre_mes",
        when(col("mes") === 1, "Enero").when(col("mes") === 2, "Febrero")
        .when(col("mes") === 3, "Marzo").when(col("mes") === 4, "Abril")
        .when(col("mes") === 5, "Mayo").when(col("mes") === 6, "Junio")
        .when(col("mes") === 7, "Julio").when(col("mes") === 8, "Agosto")
        .when(col("mes") === 9, "Setiembre").when(col("mes") === 10, "Octubre")
        .when(col("mes") === 11, "Noviembre").otherwise("Diciembre")
      )
      .withColumn("trimestre",
        when(col("mes").between(1, 3), "T1")
        .when(col("mes").between(4, 6), "T2")
        .when(col("mes").between(7, 9), "T3")
        .otherwise("T4")
      )
      .orderBy("anio", "mes")
    dimTiempoDF.write.mode(SaveMode.Append).jdbc(jdbcUrl, "dim_tiempo", dbProps)

    val dimUbicacionDF = cleanDF.select("ccdd", "departamento", "provincia", "distrito").distinct()
      .orderBy("ccdd", "provincia", "distrito")
    dimUbicacionDF.write.mode(SaveMode.Append).jdbc(jdbcUrl, "dim_ubicacion", dbProps)

    val dimComisariaDF = cleanDF.select("nombre_comisaria").distinct()
      .orderBy("nombre_comisaria")
    dimComisariaDF.write.mode(SaveMode.Append).jdbc(jdbcUrl, "dim_comisaria", dbProps)

    val dimTipificacionDF = cleanDF.select("categoria", "codigo_comision").distinct()
      .withColumn("descripcion_detalle",
        regexp_replace(col("codigo_comision"), "^(F1_|F2_|EM_|EF_|T_|P3_|C_|M_|P4_|P5_|D_)", "")
      )
      .orderBy("categoria", "codigo_comision")
    dimTipificacionDF.write.mode(SaveMode.Append).jdbc(jdbcUrl, "dim_tipificacion", dbProps)

    // 4. CARGA DE TABLA DE HECHOS
    val pgTiempo = spark.read.jdbc(jdbcUrl, "dim_tiempo", dbProps)
    val pgUbicacion = spark.read.jdbc(jdbcUrl, "dim_ubicacion", dbProps)
    val pgComisaria = spark.read.jdbc(jdbcUrl, "dim_comisaria", dbProps)
    val pgTipificacion = spark.read.jdbc(jdbcUrl, "dim_tipificacion", dbProps)

    val hechosDF = cleanDF
      .join(pgTiempo, Seq("anio", "mes"), "inner")
      .join(pgUbicacion, Seq("ccdd", "departamento", "provincia", "distrito"), "inner")
      .join(pgComisaria, Seq("nombre_comisaria"), "inner")
      .join(pgTipificacion, Seq("categoria", "codigo_comision"), "inner")
      .select("id_tiempo", "id_ubicacion", "id_comisaria", "id_tipificacion", "valor")

    hechosDF.write.mode(SaveMode.Append).jdbc(jdbcUrl, "hechos_denuncias", dbProps)
    println(s">>> EXITO! Registros insertados en hechos_denuncias: ${hechosDF.count()}")
  }
}