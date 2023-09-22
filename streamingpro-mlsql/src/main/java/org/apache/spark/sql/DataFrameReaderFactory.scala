package org.apache.spark.sql

/**
 * @author Pan Jiebin
 * @date 2021-03-05 11:13
 */
object DataFrameReaderFactory {

  def create(sparkSession: SparkSession): DataFrameReader = new DataFrameReaderAdaptor(sparkSession)
}
