# Procesamiento de Datos en Batch con Apache Spark y Hadoop HDFS

Este repositorio contiene la implementación práctica del **Procesamiento de Datos en Batch** correspondiente a la **Tarea 3: Procesamiento de Datos con Apache Spark** del curso **Big Data (Código: 202016911)** de la **Universidad Nacional Abierta y a Distancia (UNAD)** - Escuela de Ciencias Básicas, Tecnología e Ingeniería (ECBTI).

---

## 📋 Descripción del Problema y Conjunto de Datos

### 1. El Problema Financiero
En el ámbito financiero y macroeconómico, la **Tasa Representativa del Mercado (TRM)** del dólar en Colombia es una de las variables críticas para el análisis del comercio exterior, la inflación y la fijación de políticas monetarias. El procesamiento analítico tradicional sobre series de tiempo financieras extensas suele presentar lentitud en la carga y cálculo de métricas de dispersión temporal. 

El objetivo de esta solución es implementar un pipeline distribuido de procesamiento en lote que permita:
* Auditar la calidad y completitud de los datos históricos (Data Profiling).
* Transformar y limpiar el conjunto de datos aplicando conversión tipada.
* Derivar métricas financieras avanzadas: variación interdiaria en pesos, retornos porcentuales diarios y medias móviles simples (SMA de 7 días y 30 días) mediante funciones de ventana.
* Demostrar la manipulación distribuida a bajo nivel utilizando **Resilient Distributed Datasets (RDDs)** con operaciones de transformación y acción.
* Ejecutar un **Análisis Exploratorio de Datos (EDA)** multianual, identificando picos históricos y choques cambiarios.

### 2. Conjunto de Datos (`rows.csv`)
* **Fuente:** Datos Abiertos de Colombia ([datos.gov.co](https://www.datos.gov.co/)), publicado oficialmente por la Superintendencia Financiera de Colombia.
* **Volumen:** **8,379 cotizaciones diarias oficiales**.
* **Cobertura temporal:** Desde noviembre de 1991 hasta la actualidad.
* **Estructura original:**
  * `VALOR`: Cotización oficial de la divisa en pesos colombianos (COP).
  * `VIGENCIADESDE`: Fecha inicial de validez de la cotización (Formato DD/MM/AAAA).
  * `VIGENCIAHASTA`: Fecha final de vigencia.

---

## 🏛️ Arquitectura de la Solución

La solución se construyó sobre un esquema de cómputo distribuido de alto rendimiento:

1. **Capa de Almacenamiento Distribuido (Hadoop HDFS):**
   * Persistencia en el sistema de archivos distribuido `hdfs:///tarea3/rows.csv`.
   * Tolerancia a fallos por replicación de bloques y balanceo de carga administrado por el NameNode.
2. **Capa de Procesamiento en Memoria (Apache Spark - PySpark):**
   * Despliegue en modo **Standalone Cluster** con el Spark Master escuchando en el puerto `7077` y un nodo Worker dedicado.
   * Procesamiento en memoria RAM optimizado por el motor **Catalyst Optimizer** (Spark SQL) y **Project Tungsten**.
   * Supervisión de ejecución a través de la Web UI de Spark en los puertos `8080` (Cluster Master) y `4040` (Application Jobs/Stages).

---

## 📁 Contenido del Repositorio

```text
├── rows.csv                  # Dataset oficial de la TRM de Colombia (8,379 registros)
├── spark_batch_analysis.py   # Script PySpark completo: Ingesta HDFS, limpieza, RDDs y EDA
└── README.md                 # Documentación técnica e instrucciones de ejecución
```

---

## ⚙️ Prerrequisitos y Tecnologías

* **Sistema Operativo:** Ubuntu Linux (64-bit) en VirtualBox.
* **Java:** OpenJDK 11 (Eclipse Temurin 11.0.32).
* **Apache Hadoop:** Versión 3.3.6 (HDFS activo).
* **Apache Spark:** Versión 3.5.3 (PySpark / Spark SQL).
* **Python:** Versión 3.10 o superior.

---

## 🚀 Instrucciones de Ejecución Paso a Paso

### Paso 1: Iniciar los Servicios de Hadoop y Cargar el Dataset a HDFS
Inicie sesión en la máquina virtual o terminal SSH y cambie al usuario `hadoop` (dueño de los servicios de HDFS):

```bash
# Cambiar al usuario hadoop
su - hadoop

# Iniciar los daemons del clúster Hadoop
start-all.sh

# Verificar que los procesos estén activos
jps
# Salida esperada: NameNode, DataNode, SecondaryNameNode, ResourceManager, NodeManager

# Crear la carpeta de trabajo en HDFS y cargar el archivo CSV
hdfs dfs -mkdir -p /tarea3
hdfs dfs -put /home/vboxuser/rows.csv /tarea3/

# Confirmar la carga exitosa
hdfs dfs -ls /tarea3

# Regresar al usuario vboxuser
exit
```

### Paso 2: Iniciar Apache Spark (Master y Worker)
Bajo el usuario habitual (`vboxuser`):

```bash
# Iniciar el Spark Master
start-master.sh

# Iniciar el Worker apuntando a la URI del Master
start-worker.sh spark://bigdata:7077
```
> **Validación Web:** Abra en el navegador `http://<IP_DE_LA_VM>:8080` para comprobar que el Spark Master está operativo con 1 Worker en estado `ALIVE`.

### Paso 3: Ejecutar el Script Batch de PySpark
Ejecute el script analítico en caliente consumiendo directamente los datos alojados en HDFS:

```bash
python3 spark_batch_analysis.py
```

### Paso 4: Monitoreo en Tiempo Real de la Ejecución
Mientras el script se ejecuta, puede auditar el grafo de tareas y los micro-jobs generados en el navegador:
* **Spark Jobs y Stages:** `http://<IP_DE_LA_VM>:4040` (pestañas *Jobs*, *Stages* y *Environment*).

### Paso 5: Apagado Limpio de Servicios
Una vez finalizada la práctica, detenga ordenadamente los servicios:

```bash
# Detener Spark
stop-worker.sh
stop-master.sh

# Detener Hadoop
su - hadoop
stop-all.sh
exit
```

---

## 🔍 Explicación Detallada del Script (`spark_batch_analysis.py`)

1. **Inicialización y Conexión:**
   Crea la `SparkSession` optimizada para el clúster local y silencia logs innecesarios configurando el nivel de log a `WARN`.
2. **Ingesta desde HDFS:**
   Lee el archivo mediante `spark.read.option("header", "true").csv("hdfs:///tarea3/rows.csv")`.
3. **Auditoría de Calidad (Data Profiling):**
   Comprueba el esquema con `.printSchema()` y ejecuta una validación de nulos columna por columna, confirmando 0 valores nulos en el registro.
4. **Pipeline de Transformación y Limpieza:**
   * Convierte la columna `VALOR` a tipo numérico decimal (`DoubleType`).
   * Convierte `VIGENCIADESDE` a formato de fecha estándar (`DateType`).
5. **Ingeniería de Variables (Funciones de Ventana):**
   * Aplica `Window.orderBy("FECHA")` con `lag()` para calcular la variación diaria en pesos (`VALOR - LAG`).
   * Deriva la variación porcentual diaria: `((VALOR - LAG) / LAG) * 100`.
   * Calcula las medias móviles simples a 7 días (`SMA_7D`) y 30 días (`SMA_30D`).
6. **Computación con RDDs (Bajo Nivel):**
   Extrae el RDD subyacente para demostrar transformaciones funcionales distribuidas:
   * `.map()` para estructurar pares clave-valor `(Año, Cotización)`.
   * `.filter()` para descartar valores atípicos.
   * `.reduceByKey()` para agregaciones anuales.
   * Acciones `.collect()` y `.take()` para materializar los resultados en el Driver.
7. **Análisis Exploratorio de Datos (EDA en Consola):**
   Proyecta tablas formateadas con promedios, mínimos y máximos por año, localizando el pico histórico máximo ($4,924.94 COP) y los días de mayor impacto cambiario.

---

## 📊 Resultados y Hallazgos Principales

* **Evolución Histórica:** La TRM promedio anual ascendió desde **$640 COP (1991)** hasta superar los **$4,200 COP (2022-2023)**, reflejando las diferentes etapas económicas del país.
* **Pico Histórico:** Se identificó la cotización máxima histórica oficial de **$4,924.94 COP** registrada en noviembre de 2022.
* **Rendimiento:** El procesamiento de las 8,379 cotizaciones se completó en menos de **5 segundos** sobre el clúster Spark, demostrando la superioridad del cómputo en memoria frente al almacenamiento secundario tradicional.

---

## 👤 Autor

* **Estudiante:** Raquel Sofia Naranjo Gaviria
* **Curso:** Big Data (Código: 202016911)
* **Programa:** Ingeniería de Sistemas
* **Escuela:** ECBTI
