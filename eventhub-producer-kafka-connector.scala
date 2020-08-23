// Databricks notebook source
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import spark.implicits._

// COMMAND ----------

val ehNamespace = "ehns-dev-eastus2-demo"
val ehName="test-eventhub"
val ehSasKeyName= "send-listen"
val ehSasKey= dbutils.secrets.get("dev-keyvault-scope", "send-listen-secret")
val ConStr = "Endpoint=sb://" + ehNamespace + ".servicebus.windows.net/"+ehName+";SharedAccessKeyName=" + ehSasKeyName + ";SharedAccessKey=" + ehSasKey+";EntityPath="+ehName
val EH_SASL = "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"" + ConStr + "\";"
val BOOTSTRAP_SERVERS = s"$ehNamespace.servicebus.windows.net:9093"

// COMMAND ----------

val df_test = Seq(
      ("Ashish",30),
      ("Raj",40)
    ).toDF("name","age")

df_test
.select(to_json(struct(df_test.columns.map(column):_*)).alias("value"))
.write
.format("kafka")
.option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
.option("kafka.sasl.mechanism", "PLAIN")
.option("kafka.security.protocol", "SASL_SSL")
.option("kafka.sasl.jaas.config", EH_SASL)
.option("kafka.request.timeout.ms", "60000")
.option("kafka.session.timeout.ms", "60000")
.option("topic", ehName)
.save()
