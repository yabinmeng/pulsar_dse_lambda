package com.example

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.cassandra.{DataFrameReaderWrapper, DataFrameWriterWrapper}
import org.apache.spark.sql.functions.avg

object dailybatch extends  App {
  val config = ConfigFactory.load("application.conf").getConfig("conf")

  val dseCfg = config.getConfig("dse")
  val srcCfg = config.getConfig("source")
  val tgtCfg = config.getConfig("target")

  val numRecordPerPartition = config.getInt("num_record_per_partition")
  val cassSrvIp = dseCfg.getString("contact_point_ip")
  val cassSrvPort = dseCfg.getString("contact_point_port")
  val srcKsName = srcCfg.getString("ks_name")
  val srcTblName = srcCfg.getString("tbl_name")
  val tgtKsName = tgtCfg.getString("ks_name")
  val tgtTblName = tgtCfg.getString("tbl_name")
  // Spark master address URL
  val dseSparkMasterUrl = "dse://" + cassSrvIp + ":" + cassSrvPort
  //val dseSparkMasterUrl = "spark://" + cassSrvIp + ":7077"

  val sparkConf = new SparkConf()
    .setMaster(dseSparkMasterUrl)
    .setAppName("dailybatch")
    .set("spark.cassandra.connection.host", cassSrvIp)
    .set("spark.cassandra.connection.port", cassSrvPort)
    // -- Needed for submitting a job in client deployment mode
    //.set("spark.driver.host", sparkDriverIp)
    //.set("spark.driver.port", sparkDriverPort.toString)

  val spark = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()
  import spark.implicits._

  // Use Spark-Cassandra connector to read from the source keyspace and table
  val masterDF = spark.read
    .cassandraFormat(srcTblName, srcKsName)
    .load()

  // Get average speed and temperature per drill per day
  val avgValueDF = masterDF
    .groupBy("drill_id", "reading_date", "sensor_type")
    .agg(avg("reading_value").as("avg_value"))
    .sort("drill_id", "reading_date", "sensor_type")

  // Use Spark-Cassandra connector to write to the target keyspace and table
  avgValueDF.write
    .cassandraFormat(tgtTblName, tgtKsName)
    .mode("append")
    .save()

  spark.close()
}
