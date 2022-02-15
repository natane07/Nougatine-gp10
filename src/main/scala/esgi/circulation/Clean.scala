package esgi.circulation

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, split}

object Clean {
  def main(args: Array[String]): Unit = {
    // TODO : créer son SparkSession
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("TP Automatisation")
      .getOrCreate()

    val inputFile = args(0)
    val outputFile = args(1)
    // TODO : lire son fichier d'input
    val df =
      spark
        .read
        .option("header", true)
        .option("delimiter", ";")
        .csv(inputFile)

    // TODO : ajouter 3 colonnes à votre dataframe pour l'année, le mois et le jour
    val dfResult =
      df
        .withColumn("date", split(col("t_1h"), "T").getItem(0))
        .withColumn("hour", split(split(col("t_1h"), "T").getItem(1), ":").getItem(0))
        .withColumn("year", split(col("date"), "-").getItem(0))
        .withColumn("month", split(col("date"), "-").getItem(1))
        .withColumn("day", split(col("date"), "-").getItem(2))

    // TODO : écrire le fichier en parquet et partitionné par année / mois / jour

    dfResult
      .write
      .partitionBy("year", "month", "day")
      .parquet(outputFile)
  }
}