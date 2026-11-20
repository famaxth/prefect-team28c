import os
import subprocess
import sys

from prefect import flow, task, get_run_logger
from prefect.cache_policies import NO_CACHE
from pyspark.sql import SparkSession, Window, functions as F


@task(name="1. Запуск Spark-сессии на YARN", cache_policy=NO_CACHE)
def start_spark_session(app_name: str, master: str):
    log = get_run_logger()
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .enableHiveSupport()
        .getOrCreate()
    )
    sc = spark.sparkContext
    log.info(f"Spark {spark.version}, master={sc.master}, application={sc.applicationId}")
    return spark


@task(name="2. Подключение к HDFS", cache_policy=NO_CACHE)
def connect_hdfs(spark, path: str):
    log = get_run_logger()
    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    p = jvm.org.apache.hadoop.fs.Path(path)
    fs = p.getFileSystem(conf)
    log.info(f"fs.defaultFS={conf.get('fs.defaultFS')}, файловая система: {fs.getUri().toString()}")
    if not fs.exists(p):
        raise FileNotFoundError(f"нет входных данных в HDFS: {path}")
    st = fs.getFileStatus(p)
    log.info(f"Найден {path}: {st.getLen()} байт, репликация {st.getReplication()}")
    return path


@task(name="3. Чтение данных из HDFS", cache_policy=NO_CACHE)
def read_data(spark, path: str):
    log = get_run_logger()
    df = spark.read.option("header", "true").csv(path)
    log.info(f"Прочитано {df.count()} строк, исходная схема: {df.dtypes}")
    return df


@task(name="4. Трансформации данных", cache_policy=NO_CACHE)
def transform(df):
    log = get_run_logger()
    typed = (
        df.select(
            F.col("id").cast("bigint").alias("id"),
            F.to_date("sale_date", "yyyy-MM-dd").alias("sale_date"),
            F.trim("region").alias("region"),
            F.lower(F.trim("product")).alias("product"),
            F.col("quantity").cast("int").alias("quantity"),
            F.col("price").cast("decimal(10,2)").alias("price"),
        )
        .filter(F.col("sale_date").isNotNull() & (F.col("quantity") > 0) & F.col("price").isNotNull())
        .withColumn("revenue", (F.col("quantity") * F.col("price")).cast("decimal(14,2)"))
    )
    summary = (
        typed.groupBy("region", "product")
        .agg(
            F.count("*").alias("orders"),
            F.sum("quantity").cast("bigint").alias("units"),
            F.sum("revenue").cast("decimal(16,2)").alias("revenue"),
            F.round(F.avg("price"), 2).cast("double").alias("avg_price"),
            F.min("sale_date").alias("first_sale"),
            F.max("sale_date").alias("last_sale"),
        )
        .withColumn(
            "revenue_share_in_region",
            F.round(F.col("revenue") / F.sum("revenue").over(Window.partitionBy("region")) * 100, 2).cast("double"),
        )
        .withColumn("rank_in_region", F.dense_rank().over(Window.partitionBy("region").orderBy(F.desc("revenue"))))
    )
    log.info(
        "Трансформации: приведение типов (bigint/date/int/decimal), очистка, revenue = quantity * price, "
        f"агрегация по региону и товару, доля в выручке региона, ранг -> {summary.count()} строк"
    )
    return summary


@task(name="5. Сохранение как таблицы", cache_policy=NO_CACHE)
def save_table(spark, df, db: str, table: str, path: str):
    log = get_run_logger()
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    df.write.mode("overwrite").format("orc").option("path", path).saveAsTable(f"{db}.{table}")
    n = spark.table(f"{db}.{table}").count()
    log.info(f"Сохранено как таблица {db}.{table} ({n} строк), данные в {path}")
    spark.table(f"{db}.{table}").orderBy("region", "rank_in_region").show(10, truncate=False)
    return f"{db}.{table}"


@task(name="6. Проверка чтения клиентом Hive", cache_policy=NO_CACHE)
def verify_with_hive(full_table: str, jdbc_url: str, user: str):
    log = get_run_logger()
    sql = (
        f"SELECT region, count(*) AS products, sum(orders) AS orders, sum(revenue) AS revenue "
        f"FROM {full_table} GROUP BY region ORDER BY region"
    )
    env = dict(os.environ, HADOOP_CLIENT_OPTS="-Xmx256m")
    res = subprocess.run(
        ["beeline", "-u", jdbc_url, "-n", user, "--silent=true", "--outputformat=table", "-e", sql],
        capture_output=True, text=True, env=env, timeout=900,
    )
    rows = [line for line in res.stdout.splitlines() if line.startswith(("|", "+"))]
    for line in rows:
        log.info(line)
    if res.returncode != 0 or len(rows) < 4:
        raise RuntimeError(f"beeline не смог прочитать таблицу: {res.stderr[-2000:]}")
    return len(rows)


@flow(name="team28c-sales-pipeline", log_prints=True)
def sales_pipeline(src: str, db: str, table: str, table_path: str, jdbc_url: str, user: str,
                   master: str = "yarn", verify_hive: bool = True):
    spark = start_spark_session("team28c-prefect-sales", master)
    try:
        path = connect_hdfs(spark, src)
        raw = read_data(spark, path)
        summary = transform(raw)
        full_table = save_table(spark, summary, db, table, table_path)
    finally:
        spark.stop()
    if verify_hive:
        verify_with_hive(full_table, jdbc_url, user)
    return full_table


if __name__ == "__main__":
    a = sys.argv[1:]
    sales_pipeline(
        src=a[0], db=a[1], table=a[2], table_path=a[3], jdbc_url=a[4], user=a[5],
        master=os.environ.get("SPARK_MASTER", "yarn"),
        verify_hive=os.environ.get("VERIFY_HIVE", "1") == "1",
    )
