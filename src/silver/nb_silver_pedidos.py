# Databricks notebook source
# MAGIC %md
# MAGIC # Silver - Pedidos
# MAGIC Limpa e padroniza a bronze de pedidos.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

from pyspark.sql import functions as F

df_bronze = spark.table(f"{catalog}.bronze.pedidos")

df_silver = (
    df_bronze
    .select(
        F.col("o_orderkey").cast("bigint").alias("id_pedido"),
        F.col("o_custkey").cast("bigint").alias("id_cliente"),
        F.col("o_orderstatus").alias("status_pedido"),
        F.col("o_totalprice").cast("decimal(18,2)").alias("valor_total"),
        F.col("o_orderdate").cast("date").alias("data_pedido"),
        F.col("o_orderpriority").alias("prioridade"),
    )
    .filter(F.col("id_pedido").isNotNull())
    .dropDuplicates(["id_pedido"])
    .withColumn("_processed_ts", F.current_timestamp())
)

# COMMAND ----------

(
    df_silver.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.silver.pedidos")
)

print(f"Gravado em {catalog}.silver.pedidos: {df_silver.count()} linhas")