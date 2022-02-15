package esgi.circulation

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, date_format, split}


object Jointure {
  def main(args: Array[String]): Unit = {
    // TODO : créer son SparkSession
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("TP Automatisation")
      .getOrCreate()

    val inputFile = args(0)
    val joinFile = args(1)
    val outputFile = args(2)

    // TODO : lire son fichier d'input et son fichier de jointure
    val df = spark
      .read
      .parquet(inputFile)

    val joinDf = spark
      .read
      .option("header", true)
      .option("delimiter", ";")
      .csv(joinFile)
      .withColumn("date_meteo", split(col("Forecast timestamp"), "T").getItem(0))
      .filter(col("date_meteo") === "2021-12-15")
      .filter(col("Position") === "48.875,2.5")
      .withColumn("hour_meteo", date_format(col("Forecast timestamp"), "HH"))

    // TODO : ajouter ses transformations Spark avec au minimum une jointure et une agrégation
    val finaldf =
      df
      .join(
        joinDf,
        col("hour_meteo") === col("hour"),
        "inner"
      )

    finaldf.show(false)

    // TODO : écrire le résultat dans un format pratique pour la dataviz
    finaldf
      .repartition(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ";")
      .save(outputFile)
  }
}