-- Databricks notebook source
-- MAGIC 
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC 
-- MAGIC %md
-- MAGIC ##### LABIAD Soukaina
-- MAGIC 
-- MAGIC ##### M2 SID 
-- MAGIC 
-- MAGIC ___________________________________________________________________________________________________________________________________________________________________________
-- MAGIC ___________________________________________________________________________________________________________________________________________________________________________
-- MAGIC # Plan:
-- MAGIC 
-- MAGIC ### Exemple de plan d'optimisation des performances appliqué sur les données de BikeShare
-- MAGIC 
-- MAGIC exemple: FordBike 02-2018
-- MAGIC 
-- MAGIC ### Optimisation des performances avec la gestion des fichiers (Databricks Delta)
-- MAGIC 
-- MAGIC exemple: FordBike 02-2018
-- MAGIC 
-- MAGIC Dans cette méthode nous allons voir comment Databricks Delta peut optimiser les performances des requêtes. Nous créons une table standard au format Parquet et exécutons une requête rapide pour observer sa latence. Nous exécutons ensuite une deuxième requête sur la version Databricks Delta de la même table pour voir la différence de performance entre les tables standards et les tables Databricks Delta.
-- MAGIC 
-- MAGIC Pour se faire, nous allons suivre les 4 étapes ci-dessous :
-- MAGIC 
-- MAGIC * __Étape 1__ : Créez une table standard basée sur Parquet en utilisant les données des horaires de vols basés aux États-Unis.
-- MAGIC * __Étape 2__ : Exécutez une requête pour calculer le nombre de vols par mois, par aéroport d'origine sur une année.
-- MAGIC * __Étape 3__ : Créez la table des vols en utilisant Databricks Delta et optimisez la table.
-- MAGIC * __Étape 4__ : Ré-exécutez la requête de l'étape 2 et observez la latence.

-- COMMAND ----------

DROP TABLE IF EXISTS FordBike;
CREATE TABLE FordBike
USING csv
OPTIONS (
path "/FileStore/tables/201802_fordgobike_tripdata.csv",
header "true");

-- COMMAND ----------

SELECT * FROM FordBike;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Catalog Error

-- COMMAND ----------

SELECT
  start_station_name,
  end_station_name,
  duration_sec,
  bike_id
  
FROM
  FordBike
WHERE
  user_type LIKE 'Subscriber'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exemple de plan d'optimisation

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW fordBikeFiltered AS
SELECT start_station_name, count(bike_id)
FROM FordBike
WHERE
  duration_sec >= 1000
GROUP BY
  start_station_name, duration_sec;

-- COMMAND ----------

SELECT * FROM fordBikeFiltered;

-- COMMAND ----------

CACHE TABLE fordBikeFiltered;

-- COMMAND ----------

SELECT * FROM fordBikeFiltered;

-- COMMAND ----------

SELECT * FROM fordBikeFiltered WHERE start_station_name = "The Embarcadero at Sansome St";

-- COMMAND ----------

UNCACHE TABLE IF EXISTS fordBikeFiltered;

-- COMMAND ----------

SELECT * FROM fordBikeFiltered WHERE start_station_name = "The Embarcadero at Sansome St";

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Set Partitions

-- COMMAND ----------

DROP TABLE IF EXISTS FordBike;
CREATE TABLE FordBike
USING csv
OPTIONS (
  path "/FileStore/tables/201802_fordgobike_tripdata.csv",
  header "true")

-- COMMAND ----------

SELECT
  *
FROM
  FordBike
WHERE
  start_station_name = "The Embarcadero at Sansome St"

-- COMMAND ----------

DROP TABLE IF EXISTS fordBike_partition;
CREATE TABLE fordBike_partition
PARTITIONED BY (p_start_station_name)
  AS
SELECT
  duration_sec,
  start_time, 
  end_time,
  start_station_id,
  start_station_name as p_start_station_name,
  start_station_latitude,
  start_station_longitude, 
  end_station_id,
  end_station_name
FROM
  FordBike

-- COMMAND ----------

SELECT * FROM fordBike_partition WHERE p_start_station_name = "The Embarcadero at Sansome St"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Optimisation des performances avec la gestion des fichiers

-- COMMAND ----------

DROP TABLE IF EXISTS fordBike_parq;
CREATE TABLE fordBike_parq
PARTITIONED BY (pa_start_station_name)
  AS
SELECT
  duration_sec,
  start_time, 
  end_time,
  start_station_name as pa_start_station_name,
  end_station_name,
  bike_id,
  user_type
  
FROM
  FordBike

-- COMMAND ----------

SELECT duration_sec, start_time, end_time
FROM fordBike_parq
WHERE pa_start_station_name = "The Embarcadero at Sansome St"
ORDER BY duration_sec ASC
LIMIT 20;

-- COMMAND ----------

DROP TABLE IF EXISTS fordBike_delt;
CREATE TABLE fordBike_delt
USING delta
PARTITIONED BY (d_start_station_name)
  AS
SELECT
  duration_sec,
  start_time, 
  end_time,
  start_station_name as d_start_station_name,
  end_station_name,
  bike_id,
  user_type
  
FROM
  FordBike

-- COMMAND ----------

OPTIMIZE fordBike_delt ZORDER BY (duration_sec);

-- COMMAND ----------

SELECT * FROM fordBike_delt

-- COMMAND ----------

SELECT duration_sec, start_time, end_time
FROM fordBike_delt
--WHERE d_start_station_name = "The Embarcadero at Sansome St"
WHERE duration_sec >= 1000
ORDER BY duration_sec ASC
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC On remarque que la requête sur la table __Databricks Delta__ s'est exécuté beaucoup plus rapidement après l'exécution d'OPTIMIZE. 
-- MAGIC 
-- MAGIC La vitesse d'exécution de la requête est 5 à 10 fois plus rapide que celle de la table standard.
