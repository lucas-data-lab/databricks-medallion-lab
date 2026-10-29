# Databricks notebook source
dbutils.widgets.text("catalog", "dev")
catalog = dbutils.widgets.get("catalog")

print(f"Ambiente: {catalog}")

# COMMAND ----------

from pyspark.sql import functions as F

# Leitura da origem
df = spark.read.table("samples.tpch.orders")

# Colunas de auditoria (padrão da bronze)
df_bronze = (
    df
    .withColumn("_ingestion_ts", F.current_timestamp())
    .withColumn("_source", F.lit("samples.tpch.orders"))
)

# Gravação no catálogo do ambiente
tabela = f"{catalog}.bronze.pedidos"
df_bronze.write.mode("overwrite").saveAsTable(tabela)

print(f"Gravado em {tabela}: {spark.table(tabela).count()} linhas")