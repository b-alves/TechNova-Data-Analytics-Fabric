# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0d6e3aa2-59fa-4823-8372-ec2767661522",
# META       "default_lakehouse_name": "Data",
# META       "default_lakehouse_workspace_id": "da07b6a3-87b0-4fc8-96f9-74fc80950b62",
# META       "known_lakehouses": [
# META         {
# META           "id": "0d6e3aa2-59fa-4823-8372-ec2767661522"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

df_clientes = spark.read.format("csv").option("header","true").load("Files/data/clientes_simuladas.csv")
df_clientes.write.format("delta").mode("overwrite").saveAsTable("clientes")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_produtos = spark.read.format("csv").option("header","true").load("Files/data/produtos_simuladas.csv")
df_produtos.write.format("delta").mode("overwrite").saveAsTable("produtos")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_regioes = spark.read.format("csv").option("header","true").load("Files/data/regioes_simuladas.csv")
df_regioes.write.format("delta").mode("overwrite").saveAsTable("regioes")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_vendas = spark.read.format("csv").option("header","true").load("Files/data/vendas_simuladas.csv")
df_vendas.write.format("delta").mode("overwrite").saveAsTable("vendas")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
