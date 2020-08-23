// Databricks notebook source
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.eventhubs.ConnectionStringBuilder
import org.apache.spark.eventhubs.EventHubsConf
import org.apache.spark.eventhubs.EventPosition
import org.apache.spark.sql.functions.{ explode, split }

// COMMAND ----------

// Eventhub access config
val ehNamespace = "ehns-dev-eastus2-demo"
val ehName="test-eventhub"
val ehSasKeyName= "send-listen"
val ehSasKey= dbutils.secrets.get("dev-keyvault-scope", "send-listen-secret")


// COMMAND ----------

def streamEventHub (ehName:String): DataFrame = {
  val eventHubConnectionString = ConnectionStringBuilder()
      .setNamespaceName(ehNamespace)
      .setEventHubName(ehName)
      .setSasKeyName(ehSasKeyName)
      .setSasKey(ehSasKey)
      .build  
  val eventHubsConf = EventHubsConf(eventHubConnectionString)
      .setStartingPosition(EventPosition.fromStartOfStream) 
  val df_events = spark
      .readStream
      .format("eventhubs")
      .options(eventHubsConf.toMap)
      .load() 
  df_events
}

val streamDF = streamEventHub(ehName)
display(streamDF.select($"body".cast("string").alias("value")))


// COMMAND ----------

streamDF.select($"body".cast("string"))
.writeStream
.format("delta")
.outputMode("append")
.option("checkpointLocation", "dbfs:/FileStore/db_log_chkpoint")
.start("/mnt/target/riveriq/testStream") 

