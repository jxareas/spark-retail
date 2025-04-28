#!/usr/bin/env python
# coding: utf-8

# # Procesamiento y Análisis de Datos con Apache Spark
# 
# En este proyecto, se utiliza Apache Spark para resolver una serie de tareas de análisis de datos relacionadas con una empresa global del sector retail, que tiene tanto tiendas físicas como ventas online.
# 
# El análisis se implementa utilizando [Apache Spark][Apache Spark], un motor de procesamiento de datos distribuido y de alto rendimiento, particularmente mediante [PySpark][PySpark], la API de Apache Spark para el entorno de Python.
# 
# <div align="center">
# <img src="../assets/logos/tools_apache_spark.svg" height="225" width="225"/>
# </div>
# 
# [Apache Spark]: https://spark.apache.org
# [PySpark]: https://spark.apache.org/docs/latest/api/python/index.html

# ## Descripción de la tarea

# Habéis sido contratados por una empresa perteneciente al sector del Retail.
# 
# Es una empresa con presencia a nivel mundial con sede en España. Tiene tanto tiendas físicas, como venta on-line.
# 
# Todos los días recibe un archivo llamado purchases.json con compras realizadas en todo el mundo.
# 
# Cada línea del fichero es una compra de una unidad del producto correspondiente.
# 
# <div align="center">
# <img src="../assets/images/purchases.png" height="600" width="600"/>
# </div>
# 
# La plataforma logística envía todos los días un archivo stock.csv con el stock de cada producto:
# 
# <div align="center">
# <img src="../assets/images/stocks.png" height="600" width="600"/>
# </div>
# 
# **IMPORTANTE**
# 
# Los datos se han generado de forma aleatoria.

# ## Cargando Spark

# Primeramente, importamos las librerías necesarias para ejecutar nuestro código, principalmente aquellas relacionadas con la API de PySpark.

# In[3]:


# Importing libraries

from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F


# Luego, inicializamos una sesión de Spark utilizando la API de PySpark. La función `SparkSession.builder` se usa para configurar y crear una nueva instancia de Spark.
# 
# El parámetro `appName` establece el nombre de la aplicación, que será útil para identificar la sesión de Spark en el monitor de Spark. Finalmente, el método `getOrCreate` se asegura de que se obtenga una sesión existente si ya está en ejecución, o se cree una nueva si no existe.
# 
# Esta sesión de Spark será el punto de entrada para trabajar con los datos y ejecutar las operaciones de análisis de datos.

# In[4]:


spark = (
    SparkSession.builder
    .appName("RetailAnalytics")
    .getOrCreate()
)

spark


# ## Cargando la data
# 
# Procedemos a cargar los archivos que contienen los datos sobre compras (`purchases.json`) e inventario (`stock.csv`).

# In[5]:


# Setting parameters, data file paths
PURCHASES_DATA_PATH = "../data/purchases.json"
STOCK_DATA_PATH = "../data/stock.csv"

# Loading the purchases json into a dataframe
purchases_df = spark.read.json(PURCHASES_DATA_PATH)
# Loading the stocks csv into a dataframe
stock_df = spark.read.csv(STOCK_DATA_PATH, header=True, inferSchema=True)


# Procedemos a mostrar los cinco primeros datos de nuestro dataframe de compras:

# In[6]:


purchases_df.show(n=5)


# Asimismo, mostramos el esquema del dataframe de compras:

# In[7]:


purchases_df.printSchema()


# Procedemos a mostrar los cinco primeros datos de nuestro dataframe de inventario:

# In[8]:


stock_df.show(n=5)


# Asimismo, mostramos el esquema del dataframe de inventario:

# In[9]:


stock_df.printSchema()


# ## Solución de la tarea
# 
# Debéis crear un programa Spark 2.x utilizando el lenguaje Python y resolver las siguientes tareas (usando tanto del DataFrame API como Spark SQL):

# ### 1. Los 10 productos más comprados.

# #### Spark DataFrame:

# In[10]:


top_10_products_df = (
    purchases_df.groupBy("product_id")
    .agg(F.count("*").alias("total_purchases"))
    .orderBy("total_purchases", ascending=False)
    .limit(10)
)

top_10_products_df.show()


# #### SQL:
# 
# Primeramente, debemos crear unas vistas temporales:

# In[11]:


purchases_df.createOrReplaceTempView("purchases")
stock_df.createOrReplaceTempView("stock")


# Una vez hecho esto, ejecutamos la consulta en SQL nativo:

# In[12]:


top_10_products_sql = spark.sql("""
                                SELECT product_id, COUNT(*) as total_purchases
                                FROM purchases
                                GROUP BY product_id
                                ORDER BY total_purchases DESC LIMIT 10
                                """)

top_10_products_sql.show()


# ### 2. Porcentaje de compra de cada tipo de producto (`item_type`).

# #### Spark DataFrame:

# In[13]:


item_type_percentage_df = (
    purchases_df.groupBy("item_type")
    .agg((F.count("*") / purchases_df.count() * 100).alias("purchase_percentage"))
    .orderBy("purchase_percentage", ascending=False)
    .withColumn("purchase_percentage", F.round(F.col("purchase_percentage"), 2))
)

item_type_percentage_df.show()


# #### SQL:

# In[14]:


item_type_percentage_sql = spark.sql("""
                                     SELECT item_type,
                                            ROUND((COUNT(*) * 100.0 / (SELECT COUNT(*) FROM purchases)), 2) AS percentage
                                     FROM purchases
                                     GROUP BY item_type
                                     ORDER BY percentage DESC
                                     """)

item_type_percentage_sql.show()


# ### 3. Obtener los 3 productos más comprados por cada tipo de producto.

# #### Spark DataFrame:

# In[15]:


window_spec = Window.partitionBy("item_type").orderBy(F.desc("purchase_count"))

top_3_most_purchased_products_df = (
    purchases_df.groupBy("product_id", "item_type")
    .agg(F.count("*").alias("purchase_count"))
    .withColumn("rank", F.row_number().over(window_spec))
    .filter(F.col("rank") <= 3)
)

top_3_most_purchased_products_df.show()


# #### Spark SQL Nativo:

# In[16]:


top_3_most_purchased_products_sql = spark.sql("""
                                              WITH ranked_products AS (SELECT product_id,
                                                                              item_type,
                                                                              COUNT(*)                                                          AS purchase_count,
                                                                              ROW_NUMBER() OVER (PARTITION BY item_type ORDER BY COUNT(*) DESC) AS rank
                                                                       FROM purchases
                                                                       GROUP BY product_id, item_type)
                                              SELECT *
                                              FROM ranked_products
                                              WHERE rank <= 3
                                              """)

top_3_most_purchased_products_sql.show()


# ### 4. Obtener los productos que son más caros que la media del precio de los productos.

# #### Spark DataFrame:

# In[17]:


mean_price = purchases_df.agg(F.mean('price')).collect()[0][0]

products_above_avg_df = (
    purchases_df
    .select('product_id', 'item_type', 'price')
    .filter(F.col('price') > mean_price)
    .distinct()
    .orderBy(F.asc('price'))
    .withColumn('average_price', F.lit(round(mean_price, 2)))
)

products_above_avg_df.show()


# #### Spark SQL Nativo:

# In[ ]:





# In[18]:


products_above_avg_sql = spark.sql("""
                                   WITH average_price AS (SELECT AVG(price) AS avg_price
                                                          FROM purchases)
                                   SELECT DISTINCT p.product_id,
                                                   p.item_type,
                                                   p.price,
                                                   ROUND(a.avg_price, 2) AS average_price
                                   FROM purchases p
                                            CROSS JOIN
                                        average_price a
                                   WHERE p.price > a.avg_price
                                   ORDER BY p.price ASC
                                   """)

products_above_avg_sql.count()


# 
# ### 5. Indicar la tienda que ha vendido más productos.

# #### Spark DataFrame:

# In[19]:


top_store_by_sales_df = (
    purchases_df
    .groupBy('shop_id')
    .agg(F.count('*').alias('total_sales'))
    .orderBy(F.desc('total_sales'))
    .limit(1)
)

top_store_by_sales_df.show()


# #### Spark SQL Nativo:

# In[20]:


top_store_by_sales_sql = spark.sql("""
                                   SELECT shop_id,
                                          COUNT(*) AS total_sales
                                   FROM purchases
                                   GROUP BY shop_id
                                   ORDER BY total_sales DESC LIMIT 1
                                   """)

top_store_by_sales_sql.show()


# ### 6. Indicar la tienda que ha facturado más dinero.

# #### Spark DataFrame:

# In[21]:


top_store_by_revenue_df = (
    purchases_df
    .groupBy('shop_id')
    .agg(F.round(F.sum('price'), 2).alias('revenue'))
    .orderBy(F.desc('revenue'))
    .limit(1)
)

top_store_by_revenue_df.show()


# #### Spark SQL Nativo:

# In[22]:


top_store_by_revenue_sql = spark.sql("""
SELECT
    shop_id,
    ROUND(SUM(price), 2) AS revenue
FROM purchases
GROUP BY shop_id
ORDER BY revenue DESC
LIMIT 1
""")

top_store_by_revenue_sql.show()


# ### 7. Dividir el mundo en 5 áreas geográficas iguales según la longitud (location.lon) y agregar una columna con el nombre del área geográfica (Area1: - 180 a - 108, Area2: - 108 a - 36, Area3: - 36 a 36, Area4: 36 a 108, Area5: 108 a 180), ...

# #### Spark DataFrame:

# In[23]:


def assign_area(longitude: int) -> str:
    """
    Assigns a geographic area label based on the given longitude value.

    Longitude ranges are divided into the following areas:
        - Area1: [-180, -108)
        - Area2: [-108, -36)
        - Area3: [-36, 36)
        - Area4: [36, 108)
        - Area5: [108, 180]
        - Area6: For values outside the standard longitude range

    Args:
        longitude (int): The longitude value to classify.

    Returns:
        str: The name of the area corresponding to the given longitude.
    """
    if -180 <= longitude < -108: return "Area1"
    elif -108 <= longitude < -36: return "Area2"
    elif -36 <= longitude < 36: return "Area3"
    elif 36 <= longitude < 108: return "Area4"
    elif 108 <= longitude <= 180: return "Area5"
    else: return "Area6"


# In[24]:


area_udf = F.udf(assign_area)

purchases_with_area = purchases_df.withColumn(
    "area",
    area_udf(F.col("location.lon"))
)


# #### Spark SQL Nativo:

# In[25]:


spark.sql("""
SELECT *,
  CASE
    WHEN location.lon >= -180 AND location.lon < -108 THEN 'Area1'
    WHEN location.lon >= -108 AND location.lon < -36 THEN 'Area2'
    WHEN location.lon >= -36 AND location.lon < 36 THEN 'Area3'
    WHEN location.lon >= 36 AND location.lon < 108 THEN 'Area4'
    WHEN location.lon >= 108 AND location.lon <= 180 THEN 'Area5'
    ELSE 'Area6'
  END AS area
FROM purchases
""").createOrReplaceTempView("purchases_with_area")


# #### 7.1. ¿En qué área se utiliza más PayPal?

# #### Spark DataFrame:

# In[26]:


top_paypal_by_area_df = (
    purchases_with_area
    .filter(F.col("payment_type") == "paypal")
    .groupBy("area")
    .count()
    .orderBy(F.desc("count"))
    .limit(1)
)

top_paypal_by_area_df.show()


# #### Spark SQL Nativo:

# In[27]:


top_paypal_by_area_sql = spark.sql("""
SELECT area, COUNT(*) as paypal_count
FROM purchases_with_area
WHERE payment_type = 'paypal'
GROUP BY area
ORDER BY paypal_count DESC
LIMIT 1
""")

top_paypal_by_area_sql.show()


# #### 7.2. ¿Cuáles son los 3 productos más comprados en cada área?

# #### Spark DataFrame:

# In[28]:


window_spec = Window.partitionBy("area").orderBy(F.desc("count"))

top_products_by_area_df = (
    purchases_with_area
    .groupBy("area", "product_id")
    .count()
    .withColumn("rank", F.row_number().over(window_spec))
    .filter(F.col("rank") <= 3)
    .orderBy("area", "rank")
)

top_products_by_area_df.show()


# #### Spark SQL Nativo:

# In[29]:


top_products_by_area_sql = spark.sql("""
WITH ranked_products_area AS (
    SELECT
        area,
        product_id,
        COUNT(*) AS count,
        ROW_NUMBER() OVER (PARTITION BY area ORDER BY COUNT(*) DESC) AS rank
    FROM purchases_with_area
    GROUP BY area, product_id
)
SELECT *
FROM ranked_products_area
WHERE rank <= 3
""")

top_products_by_area_sql.show()


# #### 7.3. ¿Qué área ha facturado menos dinero?

# #### Spark DataFrame:

# In[30]:


lowest_revenue_area_df = (
    purchases_with_area
    .groupBy("area")
    .agg(F.round(F.sum("price"), 2).alias("revenue"))
    .orderBy("revenue")
    .limit(1)
)

lowest_revenue_area_df.show()


# #### Spark SQL Nativo:

# In[34]:


lowest_revenue_area_sql = spark.sql("""
SELECT area, ROUND(SUM(price), 2) as revenue
FROM purchases_with_area
GROUP BY area
ORDER BY revenue ASC
LIMIT 1
""")

lowest_revenue_area_sql.show()


# ### 8. Indicar los productos que no tienen stock suficiente para las compras realizadas.

# #### Spark DataFrame:

# In[32]:


purchases_count_df = (
    purchases_df
    .groupBy("product_id")
    .count()
    .withColumnRenamed("count", "purchases_made")
)

insufficient_stock_df = (
    stock_df
    .join(purchases_count_df, "product_id")
    .filter(F.col("quantity") < F.col("purchases_made"))
    .withColumnRenamed("quantity", "quantity_in_stock")
)

insufficient_stock_df.show()


# #### Spark SQL Nativo:

# In[33]:


insufficient_stock_sql = spark.sql("""
    WITH purchase_counts AS (
        SELECT
            product_id,
            COUNT(*) AS purchases
        FROM purchases
        GROUP BY product_id
    )
    SELECT
        s.product_id,
        s.quantity AS quantity_in_stock,
        p.purchases AS purchases_made
    FROM stock s
    JOIN purchase_counts p ON s.product_id = p.product_id
    WHERE s.quantity < p.purchases
""")

insufficient_stock_sql.show()

