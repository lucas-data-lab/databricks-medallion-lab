# Databricks notebook source
# MAGIC %md
# MAGIC # Gold - Pedidos por dia
# MAGIC Métricas diárias de pedidos por status.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

from pyspark.sql import functions as F

df_silver = spark.table(f"{catalog}.silver.pedidos")

df_gold = (
    df_silver
    .groupBy("data_pedido", "status_pedido")
    .agg(
        F.count("id_pedido").alias("qtd_pedidos"),
        F.sum("valor_total").alias("valor_total"),
        F.round(F.avg("valor_total"), 2).alias("ticket_medio"),
    )
    .withColumn("_processed_ts", F.current_timestamp())
)

# COMMAND ----------

(
    df_gold.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.gold.pedidos_diario")
)

print(f"Gravado em {catalog}.gold.pedidos_diario: {df_gold.count()} linhas")