# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Create sample data
data = [
    (1, "Alice", 29, 75000.0),
    (2, "Bob", 35, 85000.0),
    (3, "Charlie", 42, 95000.0),
    (4, "Diana", 31, 72000.0),
    (5, "Eve", 29, 78000.0)
]

# Define schema
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("age", IntegerType(), False),
    StructField("salary", DoubleType(), False)
])

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show the dataframe
df.show()

# Write to lakehouse table (Delta format)
table_name = "Demo_Store_Prod.LH_Demo_Base.edu.test_data"
df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(table_name)

print(f"Data successfully written to table: {table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("delta").table("Demo_Store_Prod.LH_Demo_Base.edu.test_data")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
