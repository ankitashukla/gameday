package graph

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._

object Source_0 {

  def apply(spark: SparkSession): DataFrame = {
    Config.fabricName match {
      case "pubsubfabric" =>
        spark.read
          .format("csv")
          .option("header", true)
          .option("sep",    ",")
          .schema(
            StructType(
              Array(
                StructField("order_id",       IntegerType,   true),
                StructField("customer_id",    IntegerType,   true),
                StructField("order_status",   StringType,    true),
                StructField("order_category", StringType,    true),
                StructField("order_date",     TimestampType, true),
                StructField("amount",         DoubleType,    true)
              )
            )
          )
          .load("dbfs:/Prophecy/12arpan@prophecy.io/OrdersDatasetInput.csv")
          .cache()
      case _ =>
        throw new Exception("No valid dataset present to read fabric")
    }
  }

}
