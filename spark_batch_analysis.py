"""
UNIVERSIDAD NACIONAL ABIERTA Y A DISTANCIA - UNAD
CURSO: BIG DATA (CODIGO: 202016911)
TAREA 3: PROCESAMIENTO EN BATCH CON APACHE SPARK Y HADOOP HDFS
"""

import os
import sys
import math
import builtins

try:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, year, month, dayofmonth, dayofweek, quarter,
    avg, max as spark_max, min as spark_min, stddev, count, sum as spark_sum,
    round as spark_round, lag, when, lit
)
from pyspark.sql.window import Window

def banner(titulo):
    print("\n" + "=" * 90)
    print(f" >>> {titulo.upper()} <<< ")
    print("=" * 90)

def seccion(titulo):
    print("\n" + "-" * 75)
    print(f" [*] {titulo}")
    print("-" * 75)

def render_ascii_chart(titulo, datos, ancho_max=40):
    print("\n" + "=" * 80)
    print(f"GRAFICO DE SALIDA EN CONSOLA: {titulo.upper()}")
    print("=" * 80)
    if not datos:
        return
    max_val = builtins.max(v for _, v in datos)
    for etiqueta, val in datos:
        longitud = int((val / max_val) * ancho_max) if max_val > 0 else 0
        barra = "█" * longitud
        print(f"{etiqueta:>12} | {barra:<{ancho_max}} ${val:,.2f} COP")
    print("=" * 80)

def main():
    banner("1. INICIALIZACION DEL CLUSTER APACHE SPARK")
    spark = SparkSession.builder \
        .appName("Analisis_Batch_TRM_Avanzado_UNAD") \
        .master("spark://bigdata:7077") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    print(f"[OK] Sesion Spark activa en cluster spark://bigdata:7077")

    banner("2. INGESTA DISTRIBUIDA DE DATOS")
    hdfs_path = "hdfs://localhost:9000/tarea3/rows.csv"
    local_path = "rows.csv"

    origen = hdfs_path
    if len(sys.argv) > 1 and sys.argv[1] == "--local":
        origen = local_path
        print(f"[MODO LOCAL] Forzado: '{origen}'")
    else:
        print(f"[MODO DISTRIBUIDO HDFS] Intentando: '{origen}'...")

    try:
        df_raw = spark.read.csv(origen, header=True, inferSchema=True)
        _ = df_raw.take(1)
        print(f"[EXITO] Lectura completada desde HDFS: {origen}")
    except Exception:
        origen = local_path
        print(f"[CONMUTACION] Leyendo archivo local: '{origen}'...")
        df_raw = spark.read.csv(origen, header=True, inferSchema=True)

    seccion("Estructura del Conjunto de Datos Original")
    df_raw.printSchema()
    total_crudo = df_raw.count()
    print(f"Total registros cargados inicialmente: {total_crudo:,}")
    df_raw.show(5, truncate=False)

    banner("3. AUDITORIA DE CALIDAD DE DATOS (DATA PROFILING)")
    conteo_nulos = df_raw.select([
        count(when(col(c).isNull() | (col(c) == ""), c)).alias(c)
        for c in df_raw.columns
    ])
    print("Conteo de nulos / vacios:")
    conteo_nulos.show()

    banner("4. PIPELINE DE LIMPIEZA Y TRANSFORMACION DE DATOS")
    df_temp = df_raw \
        .withColumn("FECHA", to_date(col("VIGENCIADESDE"), "dd/MM/yyyy")) \
        .withColumn("VALOR_COP", col("VALOR").cast("double")) \
        .filter(col("FECHA").isNotNull() & col("VALOR_COP").isNotNull() & (col("VALOR_COP") > 0)) \
        .dropDuplicates(["FECHA"]) \
        .sort("FECHA")

    # Tratamiento Outliers con IQR (usando builtins.max para evitar conflicto)
    cuartiles = df_temp.approxQuantile("VALOR_COP", [0.25, 0.75], 0.01)
    q1, q3 = cuartiles[0], cuartiles[1]
    iqr = q3 - q1
    lim_inferior = builtins.max(0.0, q1 - (1.5 * iqr))
    lim_superior = q3 + (1.5 * iqr)
    print(f"[ESTADISTICA IQR] Q1: ${q1:,.2f} | Q3: ${q3:,.2f} | Limites: [${lim_inferior:,.2f} - ${lim_superior:,.2f}]")
    df_clean = df_temp.filter((col("VALOR_COP") >= lim_inferior) & (col("VALOR_COP") <= lim_superior))

    # Feature Engineering con Funciones de Ventana
    w_fecha = Window.orderBy("FECHA")
    w_7d = Window.orderBy("FECHA").rowsBetween(-6, 0)
    w_30d = Window.orderBy("FECHA").rowsBetween(-29, 0)

    df_final = df_clean \
        .withColumn("ANIO", year(col("FECHA"))) \
        .withColumn("MES", month(col("FECHA"))) \
        .withColumn("VALOR_PREVIO", lag("VALOR_COP", 1).over(w_fecha)) \
        .withColumn("VARIACION_DIARIA_COP", spark_round(col("VALOR_COP") - col("VALOR_PREVIO"), 2)) \
        .withColumn("VARIACION_PCT", spark_round(((col("VALOR_COP") - col("VALOR_PREVIO")) / col("VALOR_PREVIO")) * 100, 3)) \
        .withColumn("MEDIA_MOVIL_7D", spark_round(avg("VALOR_COP").over(w_7d), 2)) \
        .withColumn("MEDIA_MOVIL_30D", spark_round(avg("VALOR_COP").over(w_30d), 2))

    df_final.cache()
    print(f"[OK] Total registros limpios y optimizados: {df_final.count():,}")
    df_final.select("FECHA", "VALOR_COP", "VARIACION_DIARIA_COP", "VARIACION_PCT", "MEDIA_MOVIL_7D", "MEDIA_MOVIL_30D").show(6, truncate=False)

    banner("5. COMPUTO DISTRIBUIDO CON RDDs")
    rdd_metricas = df_final.rdd.map(lambda r: (r["ANIO"], (float(r["VALOR_COP"]), 1, float(r["VALOR_COP"])**2)))
    rdd_filtrado = rdd_metricas.filter(lambda x: x[0] >= 2015)
    print(f"[ACCION RDD - count()] Cotizaciones procesadas (>=2015): {rdd_filtrado.count():,}")

    def sumar_tuplas(a, b):
        return (a[0] + b[0], a[1] + b[1], a[2] + b[2])

    def stats_rdd(val):
        suma_val, n, suma_cuad = val
        media = suma_val / n
        desv = math.sqrt(builtins.max(0.0, (suma_cuad / n) - (media**2)))
        return {"n": n, "media": round(media, 2), "desv": round(desv, 2)}

    rdd_resultado = rdd_filtrado.reduceByKey(sumar_tuplas).mapValues(stats_rdd).sortByKey()
    print("\n[ACCION RDD - collect()] Metricas con RDDs (reduceByKey):")
    for anio, stats in rdd_resultado.collect():
        print(f"   * Anio {anio}: {stats['n']} cotizaciones | Media: ${stats['media']:,.2f} COP | Desv: ${stats['desv']:,.2f}")

    banner("6. ANALISIS EXPLORATORIO DE DATOS (EDA / DATAFRAMES)")
    seccion("Estadisticas Anuales")
    stats_anuales = df_final.groupBy("ANIO") \
        .agg(
            spark_round(avg("VALOR_COP"), 2).alias("TRM_PROMEDIO"),
            spark_round(spark_max("VALOR_COP"), 2).alias("TRM_MAXIMA"),
            spark_round(spark_min("VALOR_COP"), 2).alias("TRM_MINIMA"),
            spark_round(stddev("VALOR_COP"), 2).alias("VOLATILIDAD_STD"),
            spark_round(spark_max("VARIACION_PCT"), 2).alias("MAX_SUBIDA_%"),
            count("VALOR_COP").alias("REGISTROS")
        ) \
        .orderBy("ANIO")
    stats_anuales.show(20, truncate=False)

    seccion("Picos Historicos y Mayor Choque Cambiario")
    print("Top 3 Cotizaciones mas altas:")
    df_final.orderBy(col("VALOR_COP").desc()).select("FECHA", "VALOR_COP", "VARIACION_PCT").show(3)

    banner("7. GENERACION DE EVIDENCIAS Y GRAFICAS")
    filas_recientes = stats_anuales.filter(col("ANIO") >= 2016).select("ANIO", "TRM_PROMEDIO").collect()
    render_ascii_chart("Evolucion TRM Promedio Anual (2016 - 2026)", [(str(r["ANIO"]), float(r["TRM_PROMEDIO"])) for r in filas_recientes])

    if HAS_MATPLOTLIB:
        os.makedirs("evidencias_batch", exist_ok=True)
        pdf_anual = stats_anuales.toPandas()
        plt.figure(figsize=(12, 5))
        plt.plot(pdf_anual["ANIO"], pdf_anual["TRM_PROMEDIO"], marker='o', color='#0F2A4A', linewidth=2)
        plt.title("Evolucion Historica TRM Colombia - Spark Batch", fontweight='bold')
        plt.xlabel("Ano")
        plt.ylabel("COP / USD")
        plt.grid(True, linestyle='--', alpha=0.5)
        plt.tight_layout()
        plt.savefig("evidencias_batch/grafica1_evolucion_trm.png", dpi=150)
        plt.close()
        print(" [OK] Imagen guardada en: evidencias_batch/grafica1_evolucion_trm.png")

    banner("EVIDENCIAS EN VIVO DE SPARK (PUERTO 4040 Y PUERTO 8080)")
    print("\n" + "*" * 80)
    print("📸 ATENCION: EL SCRIPT ESTA PAUSADO PARA QUE TOMES LAS CAPTURAS EN EL NAVEGADOR:")
    print("*" * 80)
    print(f"1. Abre en tu navegador de Windows: http://<IP_VM>:4040")
    print("   -> Pestaña 'Jobs': Veras los Jobs que Spark ejecuto (count, reduceByKey, groupBy).")
    print("   -> Pestaña 'Stages': Veras los DAGs de Spark.")
    print("   -> Pestaña 'Executors': Veras el uso de memoria y tareas del worker.")
    print(f"2. Abre en tu navegador de Windows: http://<IP_VM>:8080")
    print("   -> Veras en 'Running Applications' tu app: Analisis_Batch_TRM_Avanzado_UNAD.")
    print("*" * 80)

    input("\n>>> Presiona [ENTER] cuando ya hayas tomado las capturas para cerrar Spark...")

    banner("PROCESAMIENTO BATCH FINALIZADO")
    spark.stop()

if __name__ == "__main__":
    main()