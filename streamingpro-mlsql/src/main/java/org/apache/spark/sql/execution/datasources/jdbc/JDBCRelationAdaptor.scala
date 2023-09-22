/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.jdbc

import org.apache.spark.Partition
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

private[sql] object JDBCRelationAdaptor extends Logging {
  /**
   * Given a partitioning schematic (a column of integral type, a number of
   * partitions, and upper and lower bounds on the column's value), generate
   * WHERE clauses for each partition so that each row in the table appears
   * exactly once.  The parameters minValue and maxValue are advisory in that
   * incorrect values may cause the partitioning to be poor, but no data
   * will fail to be represented.
   *
   * Null value predicate is added to the first partition where clause to include
   * the rows with null value for the partitions column.
   *
   * @param schema resolved schema of a JDBC table
   * @param resolver function used to determine if two identifiers are equal
   * @param timeZoneId timezone ID to be used if a partition column type is date or timestamp
   * @param jdbcOptions JDBC options that contains url
   * @return an array of partitions with where clause for each partition
   */
  def columnPartition(
                       schema: StructType,
                       resolver: Resolver,
                       timeZoneId: String,
                       jdbcOptions: JDBCOptions): Array[Partition] = {
    JDBCRelation.columnPartition(schema, resolver, timeZoneId, jdbcOptions)
  }

  /**
   * Takes a (schema, table) specification and returns the table's Catalyst schema.
   * If `customSchema` defined in the JDBC options, replaces the schema's dataType with the
   * custom schema's type.
   *
   * @param resolver function used to determine if two identifiers are equal
   * @param jdbcOptions JDBC options that contains url, table and other information.
   * @return resolved Catalyst schema of a JDBC table
   */
  def getSchema(resolver: Resolver, jdbcOptions: JDBCOptions): StructType = {
    val tableSchema = JDBCRDDAdaptor.resolveTable(jdbcOptions)
    jdbcOptions.customSchema match {
      case Some(customSchema) => JdbcHelper.getCustomSchema(
        tableSchema, customSchema, resolver)
      case None => tableSchema
    }
  }

  /**
   * Resolves a Catalyst schema of a JDBC table and returns [[JDBCRelationAdaptor]] with the schema.
   */
  def apply(
             parts: Array[Partition],
             jdbcOptions: JDBCOptions)(
             sparkSession: SparkSession): JDBCRelationAdaptor = {
    val schema = JDBCRelationAdaptor.getSchema(sparkSession.sessionState.conf.resolver, jdbcOptions)
    JDBCRelationAdaptor(schema, parts, jdbcOptions)(sparkSession)
  }
}

private[sql] case class JDBCRelationAdaptor(
    override val schema: StructType,
    parts: Array[Partition],
    jdbcOptions: JDBCOptions)(@transient val sparkSession: SparkSession)
  extends BaseRelation
  with PrunedFilteredScan
  with InsertableRelation {

  override def sqlContext: SQLContext = sparkSession.sqlContext

  override val needConversion: Boolean = false

  // Check if JDBCRDD.compileFilter can accept input filters
  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    if (jdbcOptions.pushDownPredicate) {
      filters.filter(JDBCRDDAdaptor.compileFilter(_, JdbcDialects.get(jdbcOptions.url)).isEmpty)
    } else {
      filters
    }
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    // Rely on a type erasure hack to pass RDD[InternalRow] back as RDD[Row]
    JDBCRDDAdaptor.scanTable(
      sparkSession.sparkContext,
      schema,
      requiredColumns,
      filters,
      parts,
      jdbcOptions).asInstanceOf[RDD[Row]]
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    data.write
      .mode(if (overwrite) SaveMode.Overwrite else SaveMode.Append)
      .jdbc(jdbcOptions.url, jdbcOptions.tableOrQuery, jdbcOptions.asProperties)
  }

  override def toString: String = {
    val partitioningInfo = if (parts.nonEmpty) s" [numPartitions=${parts.length}]" else ""
    // credentials should not be included in the plan output, table information is sufficient.
    s"JDBCRelationAdaptor(${jdbcOptions.tableOrQuery})" + partitioningInfo
  }
}
