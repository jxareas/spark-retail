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

# In[2]:


# Importing libraries

from pyspark.sql import SparkSession


# Luego, inicializamos una sesión de Spark utilizando la API de PySpark. La función `SparkSession.builder` se usa para configurar y crear una nueva instancia de Spark.
# 
# El parámetro `appName` establece el nombre de la aplicación, que será útil para identificar la sesión de Spark en el monitor de Spark. Finalmente, el método `getOrCreate` se asegura de que se obtenga una sesión existente si ya está en ejecución, o se cree una nueva si no existe.
# 
# Esta sesión de Spark será el punto de entrada para trabajar con los datos y ejecutar las operaciones de análisis de datos.

# In[35]:


spark = (
    SparkSession.builder
    .appName("RetailAnalytics")
    .getOrCreate()
)


# ## Cargando la data

# In[4]:


# Setting parameters, data file paths
PURCHASES_DATA_PATH = "../data/purchases.json"
STOCKS_DATA_PATH = "../data/stocks.csv"

# Loading the purchases json into a dataframe
purchases_df = spark.read.json(PURCHASES_DATA_PATH)
# Loading the stocks csv into a dataframe
stock_df = spark.read.csv(STOCKS_DATA_PATH, header=True, inferSchema=True)


# In[5]:


purchases_df.show(5)


# In[6]:


stock_df.show(5)


# ## Solución de la tarea
# 
# Debéis crear un programa Spark 2.x utilizando el lenguaje Python y resolver las siguientes tareas (usando tanto del DataFrame API como Spark SQL):

# ### 1. Los 10 productos más comprados.

# #### Spark DataFrame:

# #### SQL:
# 
# Primeramente, debemos crear una vista temporal:

# In[7]:


purchases_df.createOrReplaceTempView("purchases")


# Una vez hecho esto, ejecutamos la consulta en SQL nativo:

# In[8]:


spark.sql("""
    SELECT product_id, COUNT(*) as count
    FROM purchases
    GROUP BY product_id
    ORDER BY count DESC
    LIMIT 10
""").show()


# ### 2. Porcentaje de compra de cada tipo de producto (`item_type`).

# #### Spark DataFrame:

# #### SQL:

# In[9]:


spark.sql("""
    SELECT item_type,
           (COUNT(*) / (SELECT COUNT(*) FROM purchases)) * 100 as percentage
    FROM purchases
    GROUP BY item_type
    ORDER BY percentage DESC
""").show()


# ### 3. Obtener los 3 productos más comprados por cada tipo de producto.

# #### Spark DataFrame:

# #### Spark SQL Nativo:

# In[30]:


spark.sql("""
    WITH product_counts AS (
        SELECT
            product_id,
            item_type,
            COUNT(*) AS purchase_count
        FROM
            purchases
        GROUP BY
            item_type, product_id
    ),
    ranked_products AS (
        SELECT
            product_id,
            item_type,
            purchase_count,
            ROW_NUMBER() OVER (PARTITION BY item_type ORDER BY purchase_count DESC) AS rank
        FROM
            product_counts
    )
    SELECT
        product_id,
        item_type,
        purchase_count,
        rank
    FROM
        ranked_products
    WHERE
        rank <= 3
    ORDER BY
        item_type, rank
""").show()


# ### 4. Obtener los productos que son más caros que la media del precio de los productos.

# #### Spark DataFrame:

# #### Spark SQL Nativo:

# In[33]:


spark.sql("""
    WITH average_price AS (
        SELECT AVG(price) AS avg_price FROM purchases
    )
    SELECT
        DISTINCT p.product_id,
        p.item_type,
        p.price,
        ROUND(a.avg_price, 2) AS average_price
    FROM
        purchases p
    CROSS JOIN
        average_price a
    WHERE
        p.price > a.avg_price
    ORDER BY
        p.price ASC
""").show()


# ### 5. Indicar la tienda que ha vendido más productos.

# ### 6. Indicar la tienda que ha facturado más dinero.

# ### 7. Dividir el mundo en 5 áreas geográficas iguales según la longitud (location.lon) y agregar una columna con el nombre del área geográfica (Area1: - 180 a - 108, Area2: - 108 a - 36, Area3: - 36 a 36, Area4: 36 a 108, Area5: 108 a 180), ...

# #### 7.1. ¿En qué área se utiliza más PayPal?

# #### 7.2. ¿Cuáles son los 3 productos más comprados en cada área?

# #### 7.3. ¿Qué área ha facturado menos dinero?

# ### 8. Indicar los productos que no tienen stock suficiente para las compras realizadas.
