package nz.co.glidden.spark.delta.scd
/**
  * Provides a classes for dealing generating Slowly Changing Dimensions using Delta Lake.
  *  - Delta Lake is an open-source storage layer that brings ACID transactions to
  *  Apache Spark and big data workloads. (https://delta.io/)
  */

import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Scd2 Object - containts static methods for generating Slowly Changing Dimension Delta Tables.
  *
  *  @constructor create a new person with a name and age.
  *  @param name the person's name
  *  @param age the person's age in years
  */
object Scd2 {

  // Define the scd attribute columns required for the target table
  private val ScdNameFlagLatest: String = "scd_latest_flag"
  private val ScdNameFlagDeleted: String = "scd_deleted_flag"
  private val ScdNameDateStart: String = "scd_start_date"
  private val ScdNameDateEnd: String = "scd_end_date"
  private val ScdNameHashKey: String = "scd_hash_key"

  // Define the default data values
  private val ScdValueFlagActive: String = "Y"
  private val ScdValueFlagDeleted: String = "Y"
  private val ScdValueFlagHistory: String = "N"
  private val ScdValueFlagNotDeleted: String = "N"
  private val ScdValueDefaultDateEnd: String = "9999-12-31 00:00:00"

  /** Creates a target delta table for Tracking slowly changing dimensions.
    *
    *  @param sourceDataFrame A source DataFrame to write data from
    *  @param targetPath The path which the data should be written to
    *  @param deleteRows Boolean values when true will add ScdNameFlagDeleted for tracking deleted rows.
    */
  def createDeltaTable(sourceDataFrame: DataFrame, targetPath: String, deleteRows: Boolean = false) {
    var targetDataFrame = sourceDataFrame
      .withColumn(ScdNameDateStart, current_timestamp())
      .withColumn(ScdNameDateEnd, to_timestamp(lit(ScdValueDefaultDateEnd)))
      .withColumn(ScdNameFlagLatest, lit(ScdValueFlagActive))

    // Add SCD Delete Flag columns
    if (deleteRows) {
      targetDataFrame = targetDataFrame.withColumn(ScdNameFlagDeleted, lit(ScdValueFlagNotDeleted))
    }
    // Add SCD Hash Key columns
    targetDataFrame = targetDataFrame.withColumn(ScdNameHashKey,md5Concat(sourceDataFrame))

    targetDataFrame.coalesce(1).write.format("delta").option("overwriteSchema", "true").partitionBy(ScdNameFlagLatest).mode("overwrite").save(path=targetPath)
  }
  /** Util function for rewriting a vacuuming the delta target to one file.
    *
    *  @param spark A source SparkSession to write data from
    *  @param deltaTablePath A target path to optimise
    */
  def optimise(spark: SparkSession, deltaTablePath: String) {
    val numFiles = 1

    spark.read
      .format("delta")
      .load(deltaTablePath)
      .repartition(numFiles)
      .write
      .option("dataChange", "false")
      .format("delta")
      .mode("overwrite")
      .save(deltaTablePath)

    val deltaTable = DeltaTable.forPath(spark, deltaTablePath)
    deltaTable.vacuum(0)

  }
  /** Private function for generating MD5 for the the concated result of all columns.
    *
    *  @param sourceDataFrame A source DataFrame to write data from
    */
  private def md5Concat(df: org.apache.spark.sql.DataFrame ): org.apache.spark.sql.Column = {
    val allColumnsList = for( w <- df.schema) yield w.name
    val columnList = for( columnName <- allColumnsList) yield when(df.col(columnName).isNull,lit(":")).otherwise(concat(df.col(columnName),lit(":")))
    return md5(concat(columnList:_*))
  }

  /** Executes Megre into a target delta table for Tracking slowly changing dimensions.
    *
    *  @param sourceDataFrame A source DataFrame to write data from
    *  @param targetPath The path which the data should be written to
    *  @param primaryKeyColumns A Seq() Object which contains a String list of primary
    *  @param deleteRows Boolean values when true will add ScdNameFlagDeleted for tracking deleted rows.
    */
  def execute(sourceDataFrame: org.apache.spark.sql.DataFrame, targetPath: String, primaryKeyColumns: Seq[String], deleteRows: Boolean) {
    val spark: SparkSession = sourceDataFrame.sparkSession
    val sourceUpdatesDataFrame = sourceDataFrame
      .withColumn(ScdNameHashKey,md5Concat(sourceDataFrame))
      .withColumn(ScdNameFlagDeleted,lit(ScdValueFlagNotDeleted))

    // Get the Target Delta Table
    val targetTable: DeltaTable = DeltaTable.forPath(spark, targetPath)

    val update_primary_keys = for( c <- primaryKeyColumns) yield (s"source.${c} as ${c}_key")
    // Add Hash Key for data in updates
    val updatesDataFrame: DataFrame = sourceUpdatesDataFrame.as("source")
      .selectExpr(update_primary_keys :+ "source.*":_*)
      .withColumn(ScdNameDateStart,current_timestamp())


    val insert_join_type = if (deleteRows) "left" else "inner"

    val nullPrimaryKeys: Seq[String] = for( c <- primaryKeyColumns) yield (s"nvl2(source.${ScdNameFlagDeleted},null,target.${c}) as ${c}_key")
    val insertsDataFrame: DataFrame = targetTable.toDF.as("target")
      .filter(s"target.${ScdNameFlagLatest} = '${ScdValueFlagActive}'")
      .join(sourceUpdatesDataFrame.as("source"), primaryKeyColumns, insert_join_type)
      .where(s"target.${ScdNameHashKey} <> source.${ScdNameHashKey} or source.${primaryKeyColumns(0)} is null")
      .selectExpr(nullPrimaryKeys ++ primaryKeyColumns :+ "source.*":_*)
      .withColumn(ScdNameDateStart,current_timestamp())
      .withColumn(ScdNameFlagDeleted,coalesce(col(ScdNameFlagDeleted),lit(ScdValueFlagDeleted)))

    val finalDataFrame =  updatesDataFrame.union(insertsDataFrame)


    //Build a map - for values to be inserted
    var insertMap: Map[String, String] = Map[String, String]()

    for (i <- sourceDataFrame.schema) {
      insertMap += (i.name -> ("staged_updates." + i.name) )
    }
    insertMap += (ScdNameFlagLatest -> s"'${ScdValueFlagActive}'")
    insertMap += (ScdNameDateStart -> s"staged_updates.${ScdNameDateStart}")
    insertMap += (ScdNameDateEnd -> s"'${ScdValueDefaultDateEnd}'")
    insertMap += (ScdNameHashKey ->  s"staged_updates.${ScdNameHashKey}")
    if (deleteRows)
      insertMap += (ScdNameFlagDeleted ->  s"staged_updates.${ScdNameFlagDeleted}")

    var updateMap: Map[String, String] = Map[String, String]()
    updateMap += (ScdNameFlagLatest -> s"'${ScdValueFlagHistory}'")
    updateMap += (ScdNameDateEnd -> s"staged_updates.${ScdNameDateStart}" )
    if (deleteRows)
      updateMap += (ScdNameFlagDeleted ->  s"staged_updates.${ScdNameFlagDeleted}")

    // Apply SCD Type 2 operation using merge
    val mergePrimaryKeys: Seq[String] = for( c <- primaryKeyColumns) yield (s"target.${c} = ${c}_key")

    targetTable.as("target")
      .merge(
        finalDataFrame.as("staged_updates"),
        mergePrimaryKeys.mkString(" and ")
      )
      .whenMatched(s"target.${ScdNameFlagLatest} = '${ScdValueFlagActive}' AND target.${ScdNameHashKey} <> staged_updates.${ScdNameHashKey} or staged_updates.${ScdNameFlagDeleted} = '${ScdValueFlagDeleted}'")
      .updateExpr(updateMap)
      .whenNotMatched()
      .insertExpr(insertMap)
      .execute()
  }

}