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

## 7. Consultas:
## 1. Procesamiento Distribuido MapReduce con RDDs (`02_MapReduce_RDD.scala`)

Este módulo opera directamente sobre **RDDs puros** (`org.apache.spark.rdd.RDD`) sin utilizar DataFrames ni Spark SQL, demostrando el paradigma clásico **Map $\rightarrow$ Shuffle & Sort $\rightarrow$ Reduce** sobre los **66,674 registros atómicos** (`ISO-8859-1`).

### 1.1. Punto 3a – Valores Extremos por Departamento (`map` + `reduceByKey`)
* **Pregunta analítica:** ¿Cuál es el registro individual mensual con mayor y menor cantidad de denuncias en cada departamento del Perú (2017–2025)?
* **Flujo MapReduce:**
  1. **Map:** Transforma cada tupla en `(departamento, (valor, valor))`.
  2. **ReduceByKey:** Consolida simultáneamente `math.max` y `math.min` por clave departamental.
* **Hallazgos principales:** **Lima** registra el pico máximo individual (`249` denuncias en un mes), seguida de **Callao (`55`)**, **Puno (`36`)** y **Huánuco (`31`)**; todos los departamentos presentan un mínimo de `0` en meses/categorías sin incidencia.

<img width="784" height="504" alt="image" src="https://github.com/user-attachments/assets/cd8cfdc2-ad0f-4a00-8517-867280902274" />

---

### 1.2. Punto 3b – Estadísticos Descriptivos Globales (`filter` + `map` + `reduce` + `zipWithIndex`)
* **Pregunta analítica:** ¿Cuáles son los estadísticos descriptivos de los registros activos (`valor > 0`) a nivel nacional?
* **Flujo MapReduce:**
  1. **Etapa 1 (Pasada única):** Emite `(1L, v, v*v, v, v)` y reduce mediante `reduce` para obtener conteo ($N$), suma, suma de cuadrados, máximo y mínimo.
  2. **Etapa 2 (Mediana distribuida):** Ordena los valores con `sortBy`, asigna posición global con `zipWithIndex` y extrae el elemento central mediante `lookup`.
* **Resultados exactos ($N = 11{,}865$ registros positivos):**

<img width="682" height="147" alt="image" src="https://github.com/user-attachments/assets/92785b32-e314-44e9-965c-7485d3ee6837" />

---

### 1.3. Punto 3c – Tres Pipelines de Triple MapReduce con Decimales

#### 🔹 Consulta 3c-1: Participación Nacional (%) por Departamento
* **Etapas MapReduce:**
  1. `MR1`: Suma total de denuncias por departamento (`reduceByKey(_ + _)`).
  2. `MR2`: Cálculo del gran total nacional (`map(_._2).reduce(_ + _)`).
  3. `MR3`: Cálculo del porcentaje decimal `(sumaDepto / granTotalNacional) * 100.0` y consolidación ordenada.
* **Resultados destacados:** **Lima** concentra el **`52.07%`** (`25,717` casos), seguida de **Lambayeque (`4.96%` | `2,451`)**, **Cusco (`4.15%` | `2,050`)**, **Arequipa (`4.02%` | `1,986`)**, **Huánuco (`3.40%` | `1,680`)** y **Madre de Dios (`3.28%` | `1,618`)**.

<img width="682" height="497" alt="image" src="https://github.com/user-attachments/assets/cf6ad12b-6d82-4894-910b-a3aaeb9f586c" />


#### 🔹 Consulta 3c-2: Promedio Anual de Denuncias por Dependencia Policial Activa
* **Etapas MapReduce:**
  1. `MR1`: Agrupa por `(anio, comisaria)` sumando denuncias activas (`reduceByKey`).
  2. `MR2`: Mapea a `(anio, (sumaCom, 1))` y reduce para obtener total anual y número de dependencias activas.
  3. `MR3`: Divide `totalDen / numCom.toDouble` para obtener el promedio decimal por dependencia.
* **Resultados destacados:** Entre **2017 y 2022** el promedio osciló entre **`74.347`** (2020) y **`107.000`** (2021); entre **2023 y 2025** ascendió a **`5,247.000`** (2023), **`6,690.000`** (2024) y **`4,580.000`** (2025) debido a la consolidación estadística de la PNP a nivel departamental.

<img width="681" height="199" alt="image" src="https://github.com/user-attachments/assets/65fe4251-1f0a-4f96-a3b7-3b7e09349984" />


#### 🔹 Consulta 3c-3: Peso Porcentual (%) por Finalidad de Explotación (`F1_`)
* **Etapas MapReduce:**
  1. `MR1`: Filtra `comision.startsWith("F1_")` y suma denuncias por finalidad (`reduceByKey`).
  2. `MR2`: Calcula el total acumulado de todas las finalidades `F1_` (`reduceByKey`).
  3. `MR3`: Ejecuta un `join` distribuido entre parciales y total general para calcular el peso porcentual exacto:

<img width="736" height="162" alt="image" src="https://github.com/user-attachments/assets/16572e9d-586f-4e03-b87e-4b0035376043" />


##  Inventario Completo de las 20 Consultas en Spark SQL con Resultados (`03_SparkSQL_Consultas.scala`)

### Bloque 1: Punto 5a – Agregaciones y Filtros Multicolumna (`groupBy` + `filter`)
1. **Consulta 1 (`q5a_1`):** Denuncias por modalidad de captación (`C_`) en Lima y Cusco desde 2021.
   * **Resultado:** Predomina **Oferta de Trabajo** en ambos departamentos (`245` casos en Lima en 2021; `32` en Cusco en 2022, `25` en 2021 y `20` en 2023), seguido de `OTRO (ESPECIFICAR)` e `INTERNET`.

<img width="514" height="454" alt="image" src="https://github.com/user-attachments/assets/5ed73825-b944-4ee9-acfa-a2e0fa3b3575" />


2. **Consulta 2 (`q5a_2`):** Evolución trimestral de víctimas según nivel educativo/instrucción (`P3_`) entre 2020 y 2024.
   * **Resultado:** El nivel **Secundaria** lidera todos los trimestres (picos de `99` víctimas en `2021-T1` y `2021-T2`, y `91` en `2020-T1` y `2020-T4`), seguido de **Primaria** (`47` en `2020-T1` y `46` en `2021-T2`).
  
<img width="502" height="457" alt="image" src="https://github.com/user-attachments/assets/120d78e2-7184-4969-b9ed-452d2dc4f67a" />


3. **Consulta 3 (`q5a_3`):** Medios empleados por el tratante (`M_`) más frecuentes en los 6 departamentos fronterizos (Tumbes, Piura, Tacna, Puno, Madre de Dios y Loreto).
   * **Resultado:** El **Engaño** es el mecanismo principal en **Madre de Dios (`100` casos)** y **Loreto (`99` casos)**, mientras que en **Piura** destacan `OTRO (ESPECIFICAR)` (`28`) y `ENGAÑO` (`24`).

<img width="540" height="463" alt="image" src="https://github.com/user-attachments/assets/1fb03870-5a98-4301-85b0-e193b8696e6c" />


4. **Consulta 4 (`q5a_4`):** Destino institucional de las denuncias (`D_`) por provincia en la región Lima.
   * **Resultado:** En la provincia de **Lima** se derivaron **`998` denuncias a Fiscalía**, `240` quedaron pendientes de investigación, `108` en Comisaría, `83` en otras autoridades y `33` ante Juez.

<img width="582" height="363" alt="image" src="https://github.com/user-attachments/assets/6e40972f-4824-4dea-9ec0-8a5b958a43b4" />


---

### Bloque 2: Punto 5b – Promedio de columna (`avg`) y Ordenamiento Decreciente (`orderBy desc`)
5. **Consulta 5 (`q5b_1` – `avg`):** Promedio mensual de denuncias por registro en delitos vinculados a explotación sexual (`codigo_comision` contiene `SEXUAL`).
   * **Resultado:** **`5.39` denuncias por registro activo** (`F1_EXPLOTACION SEXUAL`) a lo largo de `530` registros mensuales con incidencia positiva.

<img width="863" height="100" alt="image" src="https://github.com/user-attachments/assets/073011dc-ad04-4ee8-ad04-3d860adb8c52" />


6. **Consulta 6 (`q5b_2` – `orderBy desc`):** Top 15 distritos con mayor acumulado histórico de denuncias a nivel nacional.
   * **Resultado:** Lidera **Lima Cercado (`4,129` casos)**, seguido de **Lima (`1,913`)**, **Chiclayo (`1,473`)**, **Iquitos (`1,058`)**, **Lima-Comas (`1,053`)** y **Arequipa (`1,032`)**.

<img width="1891" height="612" alt="image" src="https://github.com/user-attachments/assets/b650688f-8e18-403f-9035-6bf2de78f47b" />


---

### Bloque 3: Punto 5c – Cruces Relacionales con 4 Tipos de JOIN
7. **Consulta 7 (`q5c_inner` – `INNER JOIN`):** Consolidado trimestral de denuncias cruzando `hechos_denuncias` y `dim_tiempo`.
   * **Resultado:** El segundo trimestre de 2018 (**`2018-T2`**) registró el mayor pico trimestral con **`2,254` denuncias** (seguido de `2017-T2` con `2,036`), cayendo a `414` en `2020-T2` por el confinamiento.

<img width="374" height="361" alt="image" src="https://github.com/user-attachments/assets/7f952319-ff64-4f0f-a88e-60ecb3f584fa" />


8. **Consulta 8 (`q5c_left` – `LEFT OUTER JOIN`):** Cobertura de todas las tipificaciones (`dim_tipificacion`) frente a los hechos ocurridos en 2025.
   * **Resultado:** En 2025 lideran reclutamiento **Nacional (`412` casos)**, denuncia por **Otro (`342`)**, derivación a **Fiscalía (`321`)**, víctimas de **Secundaria (`313`)**, **Explotación Sexual (`151`)** y mujeres de **12–17 y 18–29 años (`121` c/u)**.

<img width="616" height="364" alt="image" src="https://github.com/user-attachments/assets/a3a27017-e700-4375-aaa3-7d51c9a976f6" />


9. **Consulta 9 (`q5c_right` – `RIGHT OUTER JOIN`):** Auditoría de todas las ubicaciones (`dim_ubicacion`) frente a eventos críticos (`valor > 10`).
   * **Resultado:** La provincia de **Lima** concentra **`563` eventos críticos (`15,397` denuncias)**, seguida de Lima sin provincia especificada (`166` eventos | `5,811` denuncias), **Chiclayo (`60` | `943`)**, **Callao (`32` | `785`)** y **San Román – Puno (`18` | `648`)**.

<img width="642" height="357" alt="image" src="https://github.com/user-attachments/assets/3ebd46e6-9aab-4532-88f3-56047ee02693" />


10. **Consulta 10 (`q5c_full` – `FULL OUTER JOIN`):** Comparativa operativa entre dependencias policiales activas en 2018 vs. 2022.
    * **Resultado:** Evidencia el relevo institucional desde unidades `DEPINTRAP` (`2,342` casos en 2018 vs. `261` en 2022) y `DEPINCRI` (`812` vs. `576`) hacia unidades especializadas `DIRCTPTIM` en 2022 (`LIMA-COMAS-DIRCTPTIM` con `981`, `LIMA-DIRCTPTIM` con `738`).

<img width="581" height="362" alt="image" src="https://github.com/user-attachments/assets/d4822be1-dd25-492a-9562-0e777d536559" />

---

### Bloque 4: Punto 5d – Funciones Integradas (`org.apache.spark.sql.functions._`)
11. **Consulta 11 (`q5d_1` – `concat_ws`):** Ruta geográfica jerárquica (`DEPARTAMENTO > PROVINCIA > DISTRITO`) con mayor acumulado.
    * **Resultado:** `LIMA > NO ESPECIFICADO > NO ESPECIFICADO` suma `7,777` casos (consolidados departamentales 2023–2025), seguido de `LIMA > LIMA > LIMA CERCADO` (`4,129`) y `LAMBAYEQUE > CHICLAYO > CHICLAYO` (`1,473`).

<img width="1825" height="359" alt="image" src="https://github.com/user-attachments/assets/9146246d-351b-48be-b04e-25351de1276e" />


12. **Consulta 12 (`q5d_2` – `when`, `otherwise`, `between`):** Segmentación de registros activos por nivel de severidad.
    * **Resultado:** **Bajo (`1–2` casos):** `8,049` registros (`10,289` denuncias); **Medio (`3–10` casos):** `2,846` registros (`13,333` denuncias); **Crítico (`>10` casos):** solo **`970` registros** concentran **`25,765` denuncias**.

<img width="566" height="132" alt="image" src="https://github.com/user-attachments/assets/ff5771f9-a3e9-41c5-9e72-c365aaa251ba" />


13. **Consulta 13 (`q5d_3` – `round`, `stddev`, `max`, `min`):** Dispersión estadística anual (2017–2025).
    * **Resultado:** **2023** presenta la mayor dispersión (`promedio: 11.14`, `desv_estandar: 30.44`, `maximo: 249`), mientras que **2025** muestra la menor media (`2.92`, `desv_estandar: 4.74`).

<img width="402" height="246" alt="image" src="https://github.com/user-attachments/assets/0f25199e-5f43-4f0b-a7eb-b302a113b356" />


14. **Consulta 14 (`q5d_4` – `substring_index`, `countDistinct`):** Agrupación por prefijo técnico de variable (`P4`, `P5`, `F1`, `C`, `M`, `P3`, `T`, `F2`, `D`, `EF`, `EM`).
    * **Resultado:** Las 6 variables estructurales (`F1`, `C`, `M`, `P3`, `P4`, `P5`) cuadran exactamente en **`5,216` denuncias** cada una; por sexo de la víctima, mujeres (`EF`: `3,904` casos) superan ampliamente a hombres (`EM`: `587` casos).

<img width="727" height="287" alt="image" src="https://github.com/user-attachments/assets/41c9d03d-797e-44c6-a91c-fbe1ca87eb34" />


---

### Bloque 5: Punto 5e – Vistas Temporales SQL con `JOIN` (Exportadas a Excel)
15. **Consulta 15 (`q5e_1` – `INNER JOIN` | Hoja `5e_1_InnerJoin_Laboral`):** Top 15 dependencias policiales en denuncias por explotación laboral.
    * **Resultado:** Lidera **`DEPINTRAP` (`133` casos)**, seguida de **`LIMA-COMAS-DIRCTPTIM` (`61`)**, **`DEPINCRI` (`58`)**, **`HUAROCHIRI-DIRCTPTIM` (`44`)** y **`DPTO TRATA` (`25`)**.

<img width="1874" height="363" alt="image" src="https://github.com/user-attachments/assets/952894a5-7a45-4612-83c4-e941779ae19f" />


16. **Consulta 16 (`q5e_2` – `LEFT JOIN` | Hoja `5e_2_LeftJoin_MadreDios`):** Serie mensual completa del calendario (`v_tiempo`) para Madre de Dios.
    * **Resultado:** Conserva todos los meses (incluidos meses con `0` denuncias como junio y julio de 2017/2018), destacando picos en **setiembre 2017 (`40` casos)**, **enero 2017 (`34`)** y **mayo 2017 (`33`)**.

<img width="390" height="458" alt="image" src="https://github.com/user-attachments/assets/1b01451a-d687-4978-a478-aa6033fc35f9" />


17. **Consulta 17 (`q5e_3` – `RIGHT JOIN` | Hoja `5e_3_RightJoin_Mendicidad`):** Auditoría de los 25 departamentos frente a denuncias de mendicidad o venta de niños.
    * **Resultado:** **Lima (`137` casos)** y **Cajamarca (`40`)** encabezan la incidencia, seguidos de **Apurímac (`9`)**, **Tumbes (`6`)** y **Piura (`6`)**, mientras 9 departamentos registran `0` casos.

<img width="358" height="555" alt="image" src="https://github.com/user-attachments/assets/8c6f96c3-e94b-4aa6-afa6-3a0cdfb432bc" />


---

### Bloque 6: Punto 5f – Vistas Temporales SQL con `ORDER BY` y Filtros Avanzados (Exportadas a Excel)

18. **Consulta 18 (`q5f_1` – `IN` + `BETWEEN` + `ORDER BY` | Hoja `5f_1_OrderBy_MacroSur`):** Evolución anual en la Macrorregión Sur (Arequipa, Cusco, Puno, Tacna y Moquegua) entre 2019 y 2025.
    * **Resultado:** Tabla comparativa ordenada por departamento y año descendente exportada íntegramente a Excel.

<img width="349" height="632" alt="image" src="https://github.com/user-attachments/assets/d12c65bd-dd22-48c6-954e-84d628cefe9a" />


19. **Consulta 19 (`q5f_2` – `LIKE` + `HAVING > 50` + `ORDER BY DESC` | Hoja `5f_2_OrderBy_Captacion`):** Modalidades de captación (`C_%`) vinculadas a redes sociales, internet u ofertas que superan las 50 denuncias acumuladas.
    * **Resultado:** Exporta las modalidades de captación digital y laboral de alto impacto ordenadas de mayor a menor volumen acumulado.

<img width="625" height="137" alt="image" src="https://github.com/user-attachments/assets/00faf8c9-9021-4efc-b728-816f10792835" />


20. **Consulta 20 (`q5f_3` – `HAVING > 100` + `ORDER BY DESC` | Hoja `5f_3_OrderBy_PicosLima`):** Meses pico con más de 100 denuncias en Lima Metropolitana.
    * **Resultado:** Encabeza **agosto de 2022 (`981` denuncias)**, seguido de **abril de 2017 (`754`)**, **diciembre de 2021 (`650`)**, **junio de 2022 (`504`)**, **marzo de 2017 (`475`)** y **mayo de 2022 (`459`)**.

<img width="389" height="553" alt="image" src="https://github.com/user-attachments/assets/437becf8-025e-4810-b44a-27e07989330a" />


##  8. Resultados y Hallazgos Clave

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
