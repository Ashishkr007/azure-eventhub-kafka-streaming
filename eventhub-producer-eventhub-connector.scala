// Databricks notebook source
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import spark.implicits._
import org.apache.spark.eventhubs.ConnectionStringBuilder
import org.apache.spark.eventhubs.EventHubsConf
import org.apache.spark.eventhubs.EventPosition

// COMMAND ----------

val ehNamespace = "ehns-dev-eastus2-demo"
val ehName="test-eventhub"
val ehSasKeyName= "send-listen"
val ehSasKey= dbutils.secrets.get("dev-keyvault-scope", "send-listen-secret")
val ConStr = "Endpoint=sb://" + ehNamespace + ".servicebus.windows.net/;SharedAccessKeyName=" + ehSasKeyName + ";SharedAccessKey=" + ehSasKey+";EntityPath="+ehName

// COMMAND ----------

val eventHubsConfWrite = EventHubsConf(ConStr)
val df_test = Seq(
      ("Ashish",30),
      ("Raj",40)
    ).toDF("name","age")

df_test
.select(to_json(struct(df_test.columns.map(column):_*)).alias("body"))
.write
.format("eventhubs")
.options(eventHubsConfWrite.toMap)
.save()

