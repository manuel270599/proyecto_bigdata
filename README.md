# Análisis Distribuido de Denuncias por Trata de Personas en el Perú (2017–2025)

![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.3.2-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Scala](https://img.shields.io/badge/Scala-2.12.15-DC322F?style=for-the-badge&logo=scala&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)
![Hadoop](https://img.shields.io/badge/Hadoop-3.3.0-66CCFF?style=for-the-badge&logo=apachehadoop&logoColor=black)
![Java](https://img.shields.io/badge/Java-8-007396?style=for-the-badge&logo=openjdk&logoColor=white)

<img width="1200" height="1509" alt="UNI-logo" src="https://github.com/user-attachments/assets/cdceea86-1136-401a-a0a2-12f4d248b5f6" />


> **Proyecto – Laboratorio 03 (CC531: Procesamiento Big Data)**  
> Pipeline integral de ingeniería de datos, procesamiento distribuido con **MapReduce (RDDs)**, modelado dimensional en **PostgreSQL** y analítica multidimensional con **Spark SQL** sobre los registros oficiales de la Policía Nacional del Perú (PNP).

---

## Tabla de Contenidos

- [1. Descripción General](#-1-descripción-general)
- [2. Arquitectura del Pipeline](#-2-arquitectura-del-pipeline)
- [3. Estructura del Proyecto](#-3-estructura-del-proyecto)
- [4. Modelo de Datos (Esquema Estrella)](#-4-modelo-de-datos-esquema-estrella)
- [5. Requisitos e Instalación](#-5-requisitos-e-instalación)
- [6. Guía de Ejecución Paso a Paso](#-6-guía-de-ejecución-paso-a-paso)
- [7. Resultados y Hallazgos Clave](#-7-resultados-y-hallazgos-clave)

---

## 1. Descripción General

Este proyecto procesa y analiza los registros históricos de denuncias por el delito de **Trata de Personas en el Perú (2017–2025)** publicados por la Policía Nacional del Perú (PNP) en el Portal Nacional de Datos Abiertos.

El flujo implementa cuatro pilares fundamentales:
1. **Extracción, Transformación y Carga (ETL):** Lectura del dataset crudo (`77,486` registros en codificación `ISO-8859-1`), depuración de `10,812` filas resumen (`_TOTAL` y `_SUB TOTAL`) para evitar duplicación de métricas, homologación geográfica y carga relacional de **`66,674` registros atómicos** en PostgreSQL.
2. **Procesamiento Distribuido con MapReduce (RDDs):** Implementación de transformaciones y acciones puras (`map`, `reduceByKey`, `reduce`, `zipWithIndex`, `join`) sin DataFrames, incluyendo tres pipelines encadenados de **triple MapReduce**.
3. **Analítica con Spark SQL y Vistas Temporales:** Ejecución de **20 consultas analíticas** conectadas vía JDBC a PostgreSQL mediante la API de DataFrames (`groupBy`, `filter`, `avg`, `orderBy`, `inner`/`left`/`right`/`full_outer` joins y funciones SQL) y consultas SQL puras sobre vistas temporales.
4. **Reportabilidad Automatizada:** Exportación directa de resultados multidimensionales a un libro de Microsoft Excel de 6 pestañas (`Resultados_Consultas_5e_5f.xlsx`) utilizando `spark-excel`.

---

## 2. Arquitectura del Pipeline

```text
+---------------------------+
|   trata_personas.csv      |  (77,486 registros crudos | ISO-8859-1)
+-------------+-------------+
              |
              v  [01_ETL_Postgres.scala] -> Filtrado de subtotales y normalización
+---------------------------+
|   PostgreSQL 15           |  (bd_trata_personas | Modelo Estrella: 5 tablas)
|   hechos_denuncias        |  -> 66,674 registros atómicos validados
+-------------+-------------+
              |
      +-------+-------+
      |               |
      v               v
[02_MapReduce_RDD]  [03_SparkSQL_Consultas]
  RDDs Puros          DataFrames + Vistas Temporales
  (Puntos 3a-3c)      (Puntos 5a-5f)
                            |
                            v  [spark-excel]
                Resultados_Consultas_5e_5f.xlsx
```
---

## 3. Estructura del Proyecto

```text
proyecto_bigdata/
├── trata_personas.csv                    # Dataset fuente limpio (codificación ISO-8859-1)
├── 01_crear_tablas.sql                   # Script DDL del Modelo Estrella e índices en PostgreSQL
├── 01_ETL_Postgres.scala                 # Pipeline ETL en Scala/Spark hacia PostgreSQL (Punto 4)
├── 02_MapReduce_RDD.scala                # Procesamiento MapReduce puro con RDDs (Punto 3)
├── 03_SparkSQL_Consultas.scala           # 20 consultas Spark SQL + Exportación Excel (Punto 5)
├── backup_trata_personas.sql             # Respaldo completo de la BD generado con pg_dump (Punto 8)
├── Resultados_Consultas_5e_5f.xlsx       # Libro Excel con las 6 hojas de vistas temporales (Punto 5g)
└── README.md                             # Documentación técnica del repositorio
```

---

## 4. Modelo de Datos (Esquema Estrella)

La base de datos **`bd_trata_personas`** está diseñada bajo un esquema estrella compuesto por **1 tabla de hechos** y **4 tablas de dimensiones**, optimizado con tipos `TEXT` para soportar listas extensas de distritos y dependencias policiales:

| Tabla | Rol | Descripción y Atributos Principales |
| :--- | :--- | :--- |
| **`hechos_denuncias`** | Tabla de Hechos | Granularidad mensual por dependencia y tipificación (`id_hecho`, `id_tiempo`, `id_ubicacion`, `id_comisaria`, `id_tipificacion`, `valor`). |
| **`dim_tiempo`** | Dimensión | Jerarquía temporal: `anio` (2017–2025), `mes` (1–12), `nombre_mes`, `trimestre` (`T1`–`T4`). |
| **`dim_ubicacion`** | Dimensión | Jerarquía espacial: `ccdd`, `departamento`, `provincia`, `distrito` (con normalización `CUZCO` → `CUSCO`). |
| **`dim_comisaria`** | Dimensión | Catálogo normalizado de dependencias y unidades policiales (`nombre_comisaria`). |
| **`dim_tipificacion`** | Dimensión | Clasificación criminológica (`categoria`, `codigo_comision`, `descripcion_detalle` con prefijos `F1_`, `C_`, `M_`, `P3_`, `D_`). |

---

## 5. Requisitos e Instalación

### Software Base
* **Java JDK:** `1.8` (`1.8.0_202`)
* **Scala:** `2.12.15`
* **Apache Spark:** `3.3.2` (con binarios `winutils.exe` de Hadoop `3.3.0` configurado en `HADOOP_HOME`)
* **PostgreSQL:** `15.20`

### Librerías y Conectores
* **Driver JDBC PostgreSQL:** Copiar `postgresql-42.7.6.jar` dentro de `C:\spark\jars\`.
* **Conector Spark-Excel:** `com.crealytics:spark-excel_2.12:3.3.1_0.18.7` (descargado automáticamente vía `--packages`).

---

## 6. Guía de Ejecución Paso a Paso

### 1️ Crear la base de datos y tablas en PostgreSQL
En **pgAdmin 4** o desde terminal con `psql`, crear la base de datos `bd_trata_personas` y ejecutar el DDL:
```bash
psql -U postgres -d bd_trata_personas -f 01_crear_tablas.sql
```

### 2️ Iniciar la consola interactiva `spark-shell`
Abrir PowerShell en `C:\proyecto_bigdata` y ejecutar:
```powershell
spark-shell --packages com.crealytics:spark-excel_2.12:3.3.1_0.18.7
```

### 3️ Ejecutar el Proceso ETL hacia PostgreSQL (Punto 4)
> **Nota:** Antes de ejecutar, configura tu contraseña local de PostgreSQL en la propiedad `dbProps.setProperty("password", "TU_CLAVE_POSTGRES")` dentro de `01_ETL_Postgres.scala` y `03_SparkSQL_Consultas.scala`.

```scala
:load 01_ETL_Postgres.scala
ETL_TrataPersonas.main(Array())

```

### 4️ Ejecutar el Procesamiento MapReduce con RDDs (Punto 3)
```scala
:load 02_MapReduce_RDD.scala
MapReduce_TrataPersonas.main(Array())
```

### 5️ Ejecutar las 20 Consultas Spark SQL y Exportar a Excel (Punto 5)
```scala
:load 03_SparkSQL_Consultas.scala
SparkSQL_TrataPersonas.main(Array())

```

### 6️ Generar el respaldo `.sql` de la base de datos (Punto 8)
```powershell
pg_dump -U postgres -d bd_trata_personas -f C:\proyecto_bigdata\backup_trata_personas.sql
```

---

##  7. Resultados y Hallazgos Clave

### 🔹 Estadísticos Descriptivos (MapReduce RDD – Punto 3b)
Calculados de forma distribuida sobre los **N = 11,865** registros con denuncias positivas (`valor > 0`):

| Métrica Estadística | Valor Obtenido | Interpretación Analítica |
| :--- | :---: | :--- |
| **Promedio (Media aritmética)** | `4.1624` | Denuncias promedio por registro activo |
| **Mediana (Valor central)** | `2.0000` | El 50% de los registros reportan 1 o 2 casos |
| **Desviación Estándar** | `9.4671` | Alta dispersión por concentración en eventos masivos |
| **Valor Máximo** | `249.00` | Pico máximo registrado en Lima |
| **Valor Mínimo** | `1.00` | Umbral mínimo de registro positivo |

### 🔹 Resultados del Triple MapReduce Encadenado (Punto 3c)
* **Distribución Territorial:** **Lima** concentra el **`52.07%`** (`25,717` casos) del total nacional, seguida de **Lambayeque** (`4.96%`), **Cusco** (`4.15%`), **Arequipa** (`4.02%`), **Huánuco** (`3.40%`) y **Madre de Dios** (`3.28%`).
* **Finalidades de Explotación (`F1_`):** La **Explotación Sexual** representa el **`54.81%`** (`2,859` casos), seguida por la **Explotación Laboral** con el **`21.07%`** (`1,099` casos) y casos en investigación/no identificados (`19.27%`).
* **Cambio de Granularidad Institucional:** Entre 2017 y 2022 el promedio anual por dependencia osciló entre `74.35` y `107.00` denuncias; entre 2023 y 2025 subió a `4,580.00 – 6,690.00` debido a la consolidación estadística de la PNP a nivel departamental.

### 🔹 Reporte Exportado a Excel (`Resultados_Consultas_5e_5f.xlsx`)
El libro generado automáticamente por Spark contiene **6 hojas** correspondientes a las consultas sobre vistas temporales (`v_hechos`, `v_tiempo`, `v_ubicacion`, `v_comisaria`, `v_tipificacion`):
* **`5e_1_InnerJoin_Laboral`:** Top 15 dependencias policiales en explotación laboral (`INNER JOIN`).
* **`5e_2_LeftJoin_MadreDios`:** Cobertura mensual completa (2017–2025) en Madre de Dios (`LEFT JOIN`).
* **`5e_3_RightJoin_Mendicidad`:** Auditoría nacional por departamento en mendicidad y venta de niños (`RIGHT JOIN`).
* **`5f_1_OrderBy_MacroSur`:** Evolución 2019–2025 en la Macrorregión Sur (`BETWEEN` + `IN`).
* **`5f_2_OrderBy_Captacion`:** Modalidades de captación digital mediante redes sociales, internet y ofertas (`LIKE` + `HAVING`).
* **`5f_3_OrderBy_PicosLima`:** Meses pico (`> 100` denuncias) en Lima Metropolitana, destacando **agosto de 2022 (`981` denuncias)**, **abril de 2017 (`754`)** y **diciembre de 2021 (`650`)**.
