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

package org.apache.spark.sql

import java.util.Properties

import org.apache.spark.Partition
import org.apache.spark.annotation.Stable
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCPartition, JDBCRelationAdaptor, ReflectHelper}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

/**
 * DataFrameReader Adaptor
 *
 */
@Stable
class DataFrameReaderAdaptor private[sql](sparkSession: SparkSession) extends DataFrameReader(sparkSession: SparkSession) with Logging {

  override def jdbc(
            url: String,
            table: String,
            predicates: Array[String],
            connectionProperties: Properties): DataFrame = {
    assertNoSpecifiedSchema("jdbc")
    // connectionProperties should override settings in extraOptions.
    val extraOptions: CaseInsensitiveMap[String] = ReflectHelper.reflectValue(this, "extraOptions", isSuper = true)
    val params = extraOptions.toMap ++ connectionProperties.asScala.toMap
    val options = new JDBCOptions(url, table, params)
    val parts: Array[Partition] = predicates.zipWithIndex.map { case (part, i) =>
      JDBCPartition(part, i) : Partition
    }
    val relation = JDBCRelationAdaptor(parts, options)(sparkSession)
    sparkSession.baseRelationToDataFrame(relation)
  }

  private def assertNoSpecifiedSchema(operation: String): Unit = {
    val userSpecifiedSchema: Option[StructType] = ReflectHelper.reflectValue(this, "userSpecifiedSchema", isSuper = true)
    if (userSpecifiedSchema.nonEmpty) {
      throw new AnalysisException(s"User specified schema not supported with `$operation`")
    }
  }

}
