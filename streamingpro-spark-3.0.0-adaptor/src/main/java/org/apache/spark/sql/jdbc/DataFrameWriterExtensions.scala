/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.jdbc

import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcOptionsInWrite, JdbcUtils}
import java.io.{BufferedReader, ByteArrayInputStream, ByteArrayOutputStream, File, InputStream, InputStreamReader, OutputStream, PrintWriter}
import java.sql.{Connection, DriverManager, SQLException, Statement}
import java.text.SimpleDateFormat
import java.util
import java.util.{ArrayList, Date, Iterator, List, Map}

import com.vertica.jdbc.{VerticaConnection, VerticaCopyStream}

import scala.util.control.Breaks._
import org.apache.commons.lang3.tuple.Pair
import org.apache.spark.api.java.function.ForeachPartitionFunction
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer
/**
 * Created by allwefantasy on 26/4/2018.
 */
object DataFrameWriterExtensions extends Serializable {


  /**
   * 重写Overwirte方式
   * SaveMode.Overwrite 重写模式，其本质是先将已有的表及其数据全都删除，再重新创建该表，然后插入新的数据；
   *
   * @param
   */
  implicit class Save(@transient w: DataFrameWriter[Row]) extends Serializable {


    def save_(format: String, jdbcOptions: JDBCOptions, saveMode: SaveMode): Unit = {
      val table = jdbcOptions.tableOrQuery
      val conn = JdbcUtils.createConnectionFactory(jdbcOptions)()
      val url = jdbcOptions.url
      val writeOption = new JdbcOptionsInWrite(url, table, jdbcOptions.parameters)
      if (saveMode == SaveMode.Overwrite) {
        JdbcUtils.truncateTable(conn, writeOption)
        w.mode(SaveMode.Append)
      }
      w.format(format).save(table)
    }




    def save_csv(format: String, jdbcOptions: JDBCOptions, saveMode: SaveMode, dataFrame: DataFrame): Unit = {

      //val spark = SparkSession.builder().appName("MyApp").master("local[*]").getOrCreate()

      val table = jdbcOptions.tableOrQuery
      val conn = JdbcUtils.createConnectionFactory(jdbcOptions)()
      var realTableName = table
      var schemaName=""
      if (table.contains(".")){
        schemaName=table.split("\\.")(0)
        realTableName = table.split("\\.")(1)
        //table.substring(table.lastIndexOf(".") + 1,table.length)
      }
      val set = conn.getMetaData.getColumns(null,schemaName,realTableName,null)
      var columnNames = new util.ArrayList[String]()
      while (set.next()) {
        var columnName = set.getString("COLUMN_NAME")
        if (!columnNames.contains(columnName)) {
          columnNames.add(columnName)
        }
      }


      val url = jdbcOptions.url
      val writeOption = new JdbcOptionsInWrite(url, table, jdbcOptions.parameters)
      if (saveMode == SaveMode.Overwrite) {
        JdbcUtils.truncateTable(conn, writeOption)
        w.mode(SaveMode.Append)
      }
      // conn.setAutoCommit(false)

      val copySql = "COPY " + table + " FROM STDIN DELIMITER ','"


      val rowRdd: RDD[Row] = dataFrame.rdd
      //rowRdd.persist(StorageLevel.MEMORY_AND_DISK)

      val defCommitBatchSize: Int = 100000
      /* val numPartitions = 5


       val newRdd = rowRdd.repartition(numPartitions)*/
      rowRdd.foreachPartition(part => {
        var batch = ListBuffer[Row]()
        val connect = JdbcUtils.createConnectionFactory(jdbcOptions)()
        printf("新建一个连接对象\n")
        connect.setAutoCommit(false)
        var stream = new VerticaCopyStream(connect.asInstanceOf[VerticaConnection], copySql)
        stream.start()
        part.foreach(row => {
          batch.append(row)
          if (batch.size >= defCommitBatchSize){
            printf("执行sql批次提交。。。\n")
            val pair = this.rows2CsvStr(batch, ",", columnNames)
            var in = new ByteArrayInputStream(pair.getLeft.getBytes)
            stream.addStream(in)
            stream.execute()
            connect.commit()
            if (in != null) in.close()
            batch.clear()
          }

        })

        if (batch.nonEmpty) {
          val pair = this.rows2CsvStr(batch, ",", columnNames)
          var in = new ByteArrayInputStream(pair.getLeft.getBytes)
          val fieldNames = pair.getRight
          //val copySql = this.getCopySql(table, ",", fieldNames)
          //cm.copyIn(copySql, in)
          stream.addStream(in)
          stream.execute()
          connect.commit()
          if (in != null) in.close()
          batch.clear()
        }
        stream.finish()
        if (connect != null) connect.close()
      })
      //rowRdd.unpersist()


    }




    private def getCopySql(tbName: String, delimiter: String, fieldNames: util.List[String]): String = {
      val sb = new StringBuffer
      sb.append("copy ")
      sb.append(tbName).append(" (")
      var idx = 0
      import scala.collection.JavaConversions._
      for (fieldName <- fieldNames) {
        if (idx > 0) sb.append(",")
        sb.append(fieldName)
        idx += 1
      }
      sb.append(") from STDIN csv ")
      sb.append("DELIMITER '")
      sb.append(delimiter)
      sb.append("' NULL '' quote '\"'")
      val sql = sb.toString
      sql
    }



    def rows2CsvStr(rows: ListBuffer[Row], delimiter: String, fieldNames: util.ArrayList[String]) = {
      val sb = new StringBuilder
      var i = 0
      //var fieldNames = new util.ArrayList[String]()
      import scala.collection.JavaConversions._
      for (row <- rows) {
        /*if (fieldNames == null || fieldNames.size() == 0) {
         val schema = row.schema
          for (elem <- schema) {
            fieldNames.add(elem.name)
          }
        }*/
        if (i > 0) sb.append("\n")
        var j = 0
        import scala.collection.JavaConversions._
        var index = 0;
        for (fieldName <- fieldNames) {
          if (j > 0) sb.append(delimiter)
          var value = ""
          if (row.schema.fieldNames.contains(fieldName)) {
            val valueSome = row.getValuesMap(row.schema.fieldNames).get(fieldName)
            if (valueSome == null || valueSome.get == null) {
              value = null
            } else  {
              value = valueSome.get.toString
            }
          } else {
            value = null
          }



          sb.append(this.val2CopyStr(value, delimiter))
          j += 1
          index += 1
        }
        i += 1
      }
      Pair.of(sb.toString, fieldNames)
    }

    private val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    private def val2CopyStr(`val`: String, delimiter: String): String = {
      if (`val` == null) return ""
      if (`val`.isInstanceOf[Date]) return sdf.format(`val`.asInstanceOf[Date])
      if (`val`.isInstanceOf[Date]) {
        val dt = new Date(`val`.asInstanceOf[Date].getTime)
        return sdf.format(dt)
      }
      escapeCsvSpecialCharacters(`val`, delimiter)
    }

    private def escapeCsvSpecialCharacters(data: String, delimiter: String) = { //        String escapedData = data.replaceAll("\\R", " ");
      var escapedData = data
      if (data.contains(delimiter) || data.contains("\"") || data.contains("'")) {
        val str = data.replace("\"","\"\"")
        escapedData = "\"" + str + "\""
      }
      escapedData
    }

  }
  implicit class Upsert(w: DataFrameWriter[Row]) extends Serializable {
    def upsert(idCol: Option[String], jdbcOptions: JDBCOptions, df: DataFrame): Unit = {
      val idColumn = idCol.map(f => df.schema.filter(s => f.split(",").contains(s.name)))
      val url = jdbcOptions.url
      val table = jdbcOptions.tableOrQuery
      val modeF = w.getClass.getDeclaredField("mode")
      modeF.setAccessible(true)
      val mode = modeF.get(w).asInstanceOf[SaveMode]
      val conn = JdbcUtils.createConnectionFactory(jdbcOptions)()
      val isCaseSensitive = df.sqlContext.conf.caseSensitiveAnalysis
      val writeOption = new JdbcOptionsInWrite(url, table, jdbcOptions.parameters)
      try {

        var tableExists = JdbcUtils.tableExists(conn, writeOption)

        if (mode == SaveMode.Ignore && tableExists) {
          return
        }

        if (mode == SaveMode.ErrorIfExists && tableExists) {
          sys.error(s"Table $table already exists.")
        }

        if (mode == SaveMode.Overwrite && tableExists) {
          //          JdbcUtils.dropTable(conn, table, writeOption)
          //          tableExists = false
          JdbcUtils.truncateTable(conn, writeOption)
          tableExists = true
        }

        // Create the table if the table didn't exist.
        if (!tableExists) {
          val isCaseSensitive = df.sparkSession.sqlContext.conf.caseSensitiveAnalysis
          val schema = JdbcUtils.schemaString(df.schema, isCaseSensitive, url, jdbcOptions.createTableColumnTypes)
          val dialect = JdbcDialects.get(url)
          val pk = idColumn.map { f =>
            val key = f.map(c => s"${dialect.quoteIdentifier(c.name)}").mkString(",")
            s", primary key(${key})"
          }.getOrElse("")
          val sql = s"CREATE TABLE $table ( $schema $pk )"
          val statement = conn.createStatement
          try {
            statement.executeUpdate(sql)
          } finally {
            statement.close()
          }
        }
      } finally {
        conn.close()
      }

      //todo: make this a single method
      idColumn match {
        case Some(id) => UpsertUtils.upsert(df, idColumn, jdbcOptions, isCaseSensitive)
        case None => JdbcUtils.saveTable(df, Some(df.schema), isCaseSensitive, options = writeOption)
      }

    }

  }

  class VerticaCopyStreamImpl(connection: Connection, copySql: String) {

    private var statement: Statement = _
    private var inputStream: InputStream = _
    private var outputStream: OutputStream = _

    def startCopy(inputStream: InputStream, outputStream: OutputStream): Int = {
      if (statement eq null) statement = connection.createStatement()
      this.inputStream = inputStream
      this.outputStream = outputStream
      try {
        statement.execute(copySql)
        outputStream.flush()

        val reader = new BufferedReader(new InputStreamReader(inputStream))
        val writer = new PrintWriter(outputStream)

        var line: String = reader.readLine()
        while (line != null) {
          writer.println(line)
          line = reader.readLine()
        }

        writer.flush()

        statement.getUpdateCount
      } catch {
        case ex: SQLException =>
          throw new RuntimeException(s"Failed to execute COPY statement: $copySql", ex)
      }
    }

    def close(): Unit = {
      outputStream.close()
      inputStream.close()
      statement.close()
    }
  }


  object SparkVerticaIntegration {

    def writeDataFrameToVertica(df: DataFrame, connectionUrl: String, table: String, batchSize: Int = 5000,fieldColumns:util.ArrayList[String]): Unit = {
      val connection = DriverManager.getConnection(connectionUrl)
      val outputStream = new ByteArrayOutputStream()
      val writer = new VerticaCopyStreamImpl(connection, s"copy $table from stdin delimiter ','")
      val buffer = new ListBuffer[String]
      var count = 0

      val numPartitions = df.rdd.getNumPartitions


      if (buffer.nonEmpty) {
        outputStream.reset()

        val inputStream: InputStream = new ByteArrayInputStream(buffer.mkString("\n").getBytes("UTF-8"))

        writer.startCopy(inputStream, outputStream)
      }

      writer.close()
      connection.close()
    }


    def rows2CsvStr(row: Row, delimiter: String, fieldNames: util.ArrayList[String]) = {
      val sb = new StringBuilder

      //var fieldNames = new util.ArrayList[String]()
      import scala.collection.JavaConversions._

      /*if (fieldNames == null || fieldNames.size() == 0) {
       val schema = row.schema
        for (elem <- schema) {
          fieldNames.add(elem.name)
        }
      }*/
      var j = 0
      import scala.collection.JavaConversions._
      var index = 0;
      for (fieldName <- fieldNames) {
        if (j > 0) sb.append(delimiter)
        val valueSome = row.getValuesMap(fieldNames).get(fieldName)
        var value = "";
        if (valueSome == null || valueSome.get == null) {
          value = null
        } else  {
          value = valueSome.get.toString
        }
        sb.append(this.val2CopyStr(value, delimiter))
        j += 1
        index += 1
      }
      sb.toString()
    }

    private val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    private def val2CopyStr(`val`: String, delimiter: String): String = {
      if (`val` == null) return ""
      if (`val`.isInstanceOf[Date]) return sdf.format(`val`.asInstanceOf[Date])
      if (`val`.isInstanceOf[Date]) {
        val dt = new Date(`val`.asInstanceOf[Date].getTime)
        return sdf.format(dt)
      }
      escapeCsvSpecialCharacters(`val`, delimiter)
    }

    private def escapeCsvSpecialCharacters(data: String, delimiter: String) = { //        String escapedData = data.replaceAll("\\R", " ");
      var escapedData = data
      if (data.contains(delimiter) || data.contains("\"") || data.contains("'")) {
        val str = data.replace("\"","\"\"")
        escapedData = "\"" + str + "\""
      }
      escapedData
    }

  }

}
