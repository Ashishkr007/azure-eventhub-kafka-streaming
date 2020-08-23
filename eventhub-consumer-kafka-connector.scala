// Databricks notebook source
import org.apache.spark.sql.streaming.Trigger
import java.util.concurrent.TimeUnit
import org.apache.spark.sql.avro._
import org.apache.avro.SchemaBuilder
import java.sql.Timestamp
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

// COMMAND ----------

val ehNamespace = "ehns-dev-eastus2-demo"
val ehName="test-eventhub"
val ehSasKeyName= "send-listen"
val ehSasKey= dbutils.secrets.get("dev-keyvault-scope", "send-listen-secret")
val ConStr = "Endpoint=sb://" + ehNamespace + ".servicebus.windows.net/;SharedAccessKeyName=" + ehSasKeyName + ";SharedAccessKey=" + ehSasKey
val EH_SASL = "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"" + ConStr + "\";"
val BOOTSTRAP_SERVERS = s"$ehNamespace.servicebus.windows.net:9093"
//val offsetSetting="latest"
val offsetSetting="earliest"

// COMMAND ----------

def streamEventHub (ehName:String, EH_SASL:String, BOOTSTRAP_SERVER:String, offsetSetting:String) : DataFrame = {
  val df = spark.readStream
    .format("kafka")
    .option("subscribe", ehName)
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER)
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.jaas.config", EH_SASL)
    .option("kafka.request.timeout.ms", "60000")
    .option("kafka.session.timeout.ms", "60000")
    .option("startingOffsets", offsetSetting)
    .option("failOnDataLoss", "false")
    .load()
  df
}


// COMMAND ----------

val streamDF = streamEventHub(ehName, EH_SASL, BOOTSTRAP_SERVERS, offsetSetting)
display(streamDF.select($"value".cast("string").alias("value")))

// COMMAND ----------

streamDF.select($"body".cast("string"))
.writeStream
.format("delta")
.outputMode("append")
.option("checkpointLocation", "dbfs:/FileStore/db_log_chkpoint")
.start("/mnt/target/riveriq/testStream") 
