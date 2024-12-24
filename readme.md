# DataLake with ICEBERG, NESSIE, DREMIO & MINIO usin PySpark


```
En este procyecto de Intro de ICEBERG vamos a usar Nessie y Dremio para crear un Delta Lake Local con Docker-Compose e ingesta de datos con PySpark
```

__Links importantes__

1. [Documentación]()
2. [Apache Iceberg 101](https://www.dremio.com/blog/apache-iceberg-101-your-guide-to-learning-apache-iceberg-concepts-and-practices/)
3. [Lessons Code](https://github.com/developer-advocacy-dremio/iceberg-intro-lessons)
4. [Tutorial para construir un Lake House Local](https://dev.to/alexmercedcoder/data-engineering-create-a-apache-iceberg-based-data-lakehouse-on-your-laptop-41a8)

Para poder hacer este Tutorial es necesario __clonar__ el repositorio del punto 3 en un proyecto propio, ya que el mismo contiene el código necesario.
__FORK__ a nuestro repositorio y luego __GIT CLONE URL:REPO__

## Contenido

1. [Creacion del entorno]()
    1. [Estructura de directorios]()
2. [Ejecución de una notebook]()
    - [Ejecutar desde un cluster de Spark]()


## 1. Creación del entorno

Usamos el Docker-Compose que viene creado junto con el repositorio.
En el mismo tebenos las imagenes de Spark-Notebooks, Dremio, MINIO y Nessie.
__Para tener un mayor control de cada contenedor no vamos a inicializar este proyecto por docker-compose up__ sino que vamos a incializar cada contenedor por __Separado ya que vamos a necesitar las URL de cada uno con el nombre de cada servicio__

creamos tres tabs en el shell
```
docker-compose up notebook
docker-compose up nessie
docker-compose up dremio
docker-compose up minio
```

![](./img/iceberg-01.png)


La estructura de los directorios es.

|directorio|descripcion|
|-|-|
datasets| Contiene los archivos .csv
lesson_code| Contiene el codigo que vamos a ejecutar
notebooks|Contiene las notebooks
warehouse|Es un volumen mapeado en el cluster de Sparr donde podemos guardar data. __En un entorno real usariamos un Storage como MINIO__

Desde una notebbok con spark podemos guardar un archivo en uno de los volumenes mapeados con la siguiente sentencia.

```python
csv_df.write.format('csv').save('/home/docker/warehouse/miData.csv')
```

## 2. Ejecución de notebook.

Para ejecutar una notebook vamos a usar la URL que nos devuelve DOCKER.

```
 http://127.0.0.1:8888/?token=a45239bac50aa5f26d9fb51f6f8fd56ddafe3753ee8a9cc2
 ```

 ![](./img/iceberg-02.png)

 

 __Recordar que la misma puede cambiar con el tiempo__

 Cualquier código que ejecutemos desde esta notebook va a estar conectado al cluster de Spark.

 ## Ejecutar desde un Cluster de Spark

 Si bien la Notebook ejecuta sobre un cluster de Spark y el mismo está configuardo para usar el catalogo Nessie para acceder a las tablas de Apache Iceberg tambien podemos ejecutar un __archivo .py__ directamente dentro del cluster con el mismo resultado.

 - Paso 1.

 Creamos un archivo .py llamado ejecucion_sobre_cluster.py y lo vamos a copiar al contenedor de Spark.

Este archivo hace lo mismo que la notebook pero creamos una nueva tabla en Iceberg. __2024__

- Paso 2

Debemos copiar el archivo desde nuestro equipo al contenedor de Docker donde se guardan los scripts ejecutables.

Ingresamos al contenedor de Spark
---------------------------------

```
docker ps

docker exec -it 5ce3febe3f95 /bin/bash
```

Copiamos el archivo
-------------------

desde nuestro directorio local al contenedor de Spark.

```
docker cp ejecucion_sobre_cluster.py 5ce3febe3f95:/home/docker/notebooks/ejecucion_sobre_cluster.py
```

- Paso 3.

Con el archivo copiado podemos ejecutarlo dentro del cluster de Spark

```
python3 ejecucion_sobre_cluster.py
```

![](./img/iceberg-03.png)

## Directions


1. [Find Guides and Tutorials Here](https://github.com/developer-advocacy-dremio/quick-guides-from-dremio)
2. [Cómo contruir un Lake House Local](https://dev.to/alexmercedcoder/data-engineering-create-a-apache-iceberg-based-data-lakehouse-on-your-laptop-41a8)
