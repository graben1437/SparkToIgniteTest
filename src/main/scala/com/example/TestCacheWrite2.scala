package com.example

import java.io.PrintWriter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SaveMode

/**
  *
  */
object TestCacheWrite2 {
  def main(args: Array[String]): Unit =  {

    val sparkSession = org.apache.spark.sql.SparkSession.builder().master("yarn").appName("TestCacheWrite").getOrCreate()

    // Testing for the JDBC Driver Class
    Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")

    // Define the credentials and parameters
    val user = "test"
    val passwd = ""
    val hostname = "10.100.150.45"
    val dbName = "SDStaging_CDC"
    val jdbcPort = 1433

    val jdbcUrl = (s"jdbc:sqlserver://${hostname}:${jdbcPort};database=${dbName};user=${user};password=${passwd}")

    val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    val connectionProperties = new java.util.Properties()
    connectionProperties.setProperty("Driver",driverClass)

    val tableToRead = "dbo.woAcctSettings"
    val df1 = sparkSession.read.jdbc(jdbcUrl, tableToRead, connectionProperties).select("AcctNum", "AdminUserName", "OrgName")
    // df1.write.format("com.databricks.spark.csv").save("testcachedataloaded2")
    // All works up to here.....


    try {
      Class.forName("org.apache.ignite.IgniteJdbcDriver")
      // val igniteUrl = "jdbc:ignite://10.100.80.109:11211/TESTCACHE"
      // val igniteUrl = "jdbc:ignite:cfg://[<params>@]<config_url>"
      // val igniteUrl = "jdbc:ignite:cfg://cache=TESTCACHE:streaming=true@file:///etc/config/ignite-jdbc.xml"
      val igniteUrl = "jdbc:ignite:cfg://cache=TESTCACHE@file:///etc/config/ignite-jdbc.xml"
      val igniteTable = "SAMPLETABLE"
      // Saving data to a JDBC source
      // many of these options were added in "after the fact" to try to get this to work
      // but still doesn't work.
      /* df1.write
        .format("jdbc")
        .option("url", igniteUrl)
        .option("dbName", "TESTCACHE")
        .option("dbtable", "SAMPLETABLE")
        .option("hostname", "10.100.80.109")
        .option("port", 11211)
        .mode(SaveMode.Overwrite)
        .save() */
      df1.write
        .format("jdbc")
        .option("url", igniteUrl)
        .option("dbtable", igniteTable)
        .mode(SaveMode.Overwrite)
        .save()
    }
    catch {
      case e : Exception => {
        val conf = new Configuration()
        conf.set("fs.defaultFS", "hdfs://10.100.80.106:8020")
        val fs= FileSystem.get(conf)
        val output = fs.create(new Path("/user/hive/debugignitewrite/ignitewritedebug"))

        val writer = new PrintWriter(output)
        writer.write(e.toString())
        writer.close()
      }
    }
  }
}
