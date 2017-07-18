package com.example


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.io.PrintWriter


/**
  * DO NOT USE.  Use TestCacheWrite2 for test case
  */
object TestCacheWrite {

  def main(args: Array[String])  {

    val sparkSession = org.apache.spark.sql.SparkSession.builder().master("yarn").appName("TestCacheWrite").getOrCreate()
    try {
      // reading from MS SQL Server, so use this jdbc driver
      Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
      // microsoft sql server jdbc driver is in jar directory for spark worker nodes
      // the IP in here is to the development box.
      // the following is to Mayhews box in orange:  TPSDPDDBSTG12.prod.sd.local
      val url = "jdbc:sqlserver://10.100.150.45:1433;database=SDStaging_CDC;user=test;password="
      // val dbtable = "dbo.workOrder"
      val dbtable = "dbo.woAcctSettings"

      val jdbcDF = sparkSession.read
        .format("jdbc")
        .option("url", url)
        .option("dbtable", dbtable)
        .option("user", "test")
        .option("password", "")
        .load()

      // temporary hacking
      val adjustedJdbcDF1 = jdbcDF.drop("databaseID").drop("OrgAddress").drop("OrgCity")
      val adjustedJdbcDF2 = adjustedJdbcDF1.drop("OrgStateProvidence").drop("OrgPostalCode").drop("OrgCountry").drop("LastWONum").drop("SurveyEveryNumWO")
      val adjustedJdbcDF3 = adjustedJdbcDF2.drop("SurveyNoMoreThanNum").drop("FiscalYearBeginDate").drop("FiscalYearEndDate")
      val adjustedJdbcDF4 = adjustedJdbcDF3.drop("RequestFormTitle").drop("RequestFormIntroPara").drop("RequestEmergContactInfo")
      val adjustedJdbcDF5 = adjustedJdbcDF4.drop("OrgLogo").drop("RequestEmergContactPhone").drop("TermsAndConditionsAcceptedOn")
      val adjustedJdbcDF6 = adjustedJdbcDF5.drop("TermsAndConditionsAcceptedByUserName")
      val adjustedJdbcDF7 = adjustedJdbcDF6.drop("TermsAndConditionVersionNumberAccepted").drop("ComptrollerUserName")
      val adjustedJdbcDF8 = adjustedJdbcDF7.drop("RequestPassword").drop("CustomCategoryLabel").drop("OrgWebSiteURL")
      val adjustedJdbcDF9 = adjustedJdbcDF8.drop("OrgWebLabel").drop("CreateDate").drop("PMPurposeCodeID").drop("DefaultBudgetID")
      val adjustedJdbcDF10 = adjustedJdbcDF9.drop("BudgetIDRequired").drop("ProvideBudgetID").drop("sysSkipUpdateTrigger")
      val adjustedJdbcDF11 = adjustedJdbcDF10.drop("NJAC624").drop("UsingUniformat").drop("PMSystemSchemaID").drop("TestAccount")
      val adjustedJdbcDF12 = adjustedJdbcDF11.drop("HideReqCompletionDate").drop("FSPurposeCodeID").drop("HideEmergencyInMSB")
      val adjustedJdbcDF13 = adjustedJdbcDF12.drop("SVDPurposeCodeID").drop("ShowAssignedTo").drop("RequestedPhoneNumber")
      val adjustedJdbcDF14 = adjustedJdbcDF13.drop("RequestedAreaNumber").drop("DefaultPurposeID").drop("SuperIINonDirectLaborEntry")
      val adjustedJdbcDF15 = adjustedJdbcDF14.drop("PDPurposeCode").drop("RequesterCraftFormat").drop("BudgetEstimateNoChange")
      val adjustedJdbcDF16 = adjustedJdbcDF15.drop("RequireBuilding").drop("PrintBarCodeEnabled").drop("DefaultEstimatedStartDate")
      val adjustedJdbcDF17 = adjustedJdbcDF16.drop("PrintWOFormType").drop("SalesTaxPercentage").drop("SysAllowMSBTrackByIP")
      val adjustedJdbcDF18 = adjustedJdbcDF17.drop("MSBTrackByIP").drop("MSBTrackByPassword").drop("WOForm").drop("JournalEmailClearedOn")
      val adjustedJdbcDF19 = adjustedJdbcDF18.drop("Admin_NotifyRequesterAddition").drop("sysDateLastUpdated").drop("ProblemContactEmail")
      val adjustedJdbcDF20 = adjustedJdbcDF19.drop("ShowAttachNewFile").drop("TDPurposeId").drop("EmailSeparator")
      val adjustedJdbcDF21 = adjustedJdbcDF20.drop("LastDTSProductUsageMDW").drop("LastDTSProductUsageMD")
      // save to HDFS for debugging
      adjustedJdbcDF21.write.format("com.databricks.spark.csv").save("testcachedataloaded")

      val colNames : Array[String] = adjustedJdbcDF21.columns
      val conf = new Configuration()
      //conf.set("fs.defaultFS", "hdfs://quickstart.cloudera:8020")
      conf.set("fs.defaultFS", "hdfs://10.100.80.106:8020")
      val fs= FileSystem.get(conf)
      val output = fs.create(new Path("/user/hive/columns/columnnames.txt"))

      val writer = new PrintWriter(output)
      writer.write("Here Are The Column Names\n")
      try {
        for (aColName <- colNames) {
          writer.write("columnName: ")
          writer.write(aColName)
          writer.write("\n")
        }
      }
      finally {
        writer.close()
      }

      // here are the only columns we keep for writing into this Ignite Test
      /* uniqueid: String
         acctnum: Int =
         adminUserName: String =
         orgName: String = */


      // writing data frame to Ignite, so use this JDBC Driver
      Class.forName("org.apache.ignite.IgniteJdbcDriver")
      val igniteUrl =  "jdbc:ignite://10.100.80.106:11211/TESTCACHE"
      val igniteTable = "SampleTable"
      // Saving data to a JDBC source
      adjustedJdbcDF21.write
        .format("jdbc")
        .option("url", igniteUrl)
        .option("dbtable", igniteTable)
        .save()
    }
    catch {
      case e: Exception => e.printStackTrace()
    }
    finally {
      sparkSession.close()
    }
  }
}
