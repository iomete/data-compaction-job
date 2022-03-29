package com.iomete.spark.datacompaction

import org.apache.commons.logging.LogFactory
import org.apache.spark.sql.SparkSession

import java.util.concurrent.TimeUnit

object SqlCompaction {
  private val log = LogFactory.getLog(this.getClass)

  def main(args: Array[String]): Unit = {
    log.info("Data Compaction: 0.1.1")

    val spark = SparkSession.builder().appName("Data Compaction Job").getOrCreate()
    try {
      runCompaction(spark)
    } finally {
      spark.close()
    }
  }

  def runCompaction(spark: SparkSession): Unit = {
    spark.catalog.listDatabases().collectAsList().forEach(db => {
      log.info(s"Introspecting ${db.name} database")
      val tables = spark.sql(s"show tables from ${db.name}").collectAsList()

      tables.forEach(table => {
        log.debug(s"Looking at table=${table.getString(1)}")
        try {
          processTable(spark, table.getString(0), table.getString(1))
        } catch {
          case e: NoSuchElementException =>
            log.debug(s"errorProcessing table=${table.getString(0)}.${table.getString(1)} is not iceberg type, msg=${e.getLocalizedMessage}")
          case e: Exception =>
            log.error(s"errorProcessing table=${table.getString(0)}.${table.getString(1)} msg=${e.getLocalizedMessage}")
            e.printStackTrace()
        }
      })
    })
  }

  def processTable(spark: SparkSession, namespace: String, tableName: String): Unit = {
    spark.sql(s"describe extended $namespace.$tableName")
      .collectAsList()
      .stream()
      .filter(r => r.getString(0).equals("Provider") && r.getString(1).equals("iceberg"))
      .findFirst()
      .orElseThrow()

    log.info(s"table=$namespace.$tableName type=iceberg. Starting data compaction...")

    expireSnapshots(spark, namespace, tableName)
    removeOrphanFiles(spark, namespace, tableName)
    rewriteDataFiles(spark, namespace, tableName)
    rewriteManifest(spark, namespace, tableName)
  }

  def expireSnapshots(spark: SparkSession, namespace: String, tableName: String): Unit = {
    log.debug(s"expireSnapshots.start: table=$namespace.$tableName")
    val startTime = System.currentTimeMillis()

    spark.sql(s"CALL spark_catalog.system.expire_snapshots(table => '$namespace.$tableName')")

    val duration = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - startTime)
    log.info(s"expireSnapshots.completed: table=$namespace.$tableName duration=$duration seconds")

  }

  def removeOrphanFiles(spark: SparkSession, namespace: String, tableName: String): Unit = {
    log.debug(s"removeOrphanFiles.start: table=$namespace.$tableName")
    val startTime = System.currentTimeMillis()

    spark.sql(s"CALL spark_catalog.system.remove_orphan_files(table => '$namespace.$tableName')")

    val duration = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - startTime)
    log.info(s"removeOrphanFiles.completed: table=$namespace.$tableName duration=$duration seconds")
  }

  def rewriteManifest(spark: SparkSession, namespace: String, tableName: String): Unit = {
    log.debug(s"rewriteManifest.start: table=$namespace.$tableName")
    val startTime = System.currentTimeMillis()

    spark.sql(s"CALL spark_catalog.system.rewrite_manifests(table => '$namespace.$tableName')")

    val duration = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - startTime)
    log.info(s"rewriteManifest.completed: table=$namespace.$tableName duration=$duration seconds")
  }

  def rewriteDataFiles(spark: SparkSession, namespace: String, tableName: String): Unit = {
    log.debug(s"rewriteDataFiles.start: table=$namespace.$tableName")
    val startTime = System.currentTimeMillis()

    spark.sql(s"CALL spark_catalog.system.rewrite_data_files(table => '$namespace.$tableName')")

    val duration = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - startTime)
    log.info(s"rewriteDataFiles.completed: table=$namespace.$tableName duration=$duration seconds")
  }
}
