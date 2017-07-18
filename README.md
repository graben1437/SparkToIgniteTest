# SparkToIgniteTest
Test code that uses Spark 2.1 to select from MSSQL server and write to Apache Ignite
Currently having issues writing to Ignite.
Regardless of the settings in TestCacheWrite2, I receive the following exception:
   >> java.sql.SQLFeatureNotSupportedException: Updates are not supported.
   
Since I am using Ignite 2.0, I assumed that updates were supported, according to the Apache Ignite documentation.

