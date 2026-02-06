/*
 * The MIT License
 * Copyright © 2026 Jordan Epstein
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package io.github.jordepic.spark.rules

import io.github.jordepic.proto._
import io.github.jordepic.spark.DataHarnessTable
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.jdk.CollectionConverters._

case class UnionTableResolutionRule(spark: SparkSession)
    extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformDown {
    case relation: DataSourceV2Relation
        if relation.table.isInstanceOf[DataHarnessTable] =>
      val dataHarnessTable = relation.table.asInstanceOf[DataHarnessTable]
      val schema = dataHarnessTable.schema()
      val sourceDataFrames = loadSourceDataFrames(dataHarnessTable.getResponse)

      if (sourceDataFrames.isEmpty) {
        relation
      } else {
        val projectedDataFrames =
          sourceDataFrames.map(df => projectToSchema(df, schema))
        val unionDf = projectedDataFrames.reduce((df1, df2) => df1.union(df2))
        val unionLogicalPlan = unionDf.queryExecution.analyzed
        val originalOutput = relation.output

        val projectList = originalOutput.zip(unionLogicalPlan.output).map {
          case (target, source) =>
            if (source.exprId == target.exprId) {
              target
            } else {
              Alias(source, target.name)(exprId = target.exprId)
            }
        }
        Project(projectList, unionLogicalPlan)
      }
  }

  private def loadSourceDataFrames(
      response: LoadTableResponse
  ): Seq[DataFrame] = {
    val dataFrames =
      scala.collection.mutable.ListBuffer[DataFrame]()

    for (source <- response.getSourcesList.asScala) {
      if (source.hasKafkaSource) {
        dataFrames += loadKafkaDataFrame(source.getKafkaSource)
      } else if (source.hasYugabytedbSource) {
        dataFrames += loadYugabyteDataFrame(source.getYugabytedbSource)
      } else if (source.hasIcebergSource) {
        dataFrames += loadIcebergDataFrame(source.getIcebergSource)
      } else if (source.hasPostgresdbSource) {
        dataFrames += loadPostgresDataFrame(source.getPostgresdbSource)
      }
    }

    dataFrames.toSeq
  }

  private def loadKafkaDataFrame(
      kafkaSource: KafkaSourceMessage
  ): DataFrame = {
    val topicName = kafkaSource.getTopicName
    val partitionNum = kafkaSource.getPartitionNumber
    val startOffset = kafkaSource.getStartOffset
    val endOffset = kafkaSource.getEndOffset
    val brokerUrls = kafkaSource.getBrokerUrls

    val assignJson = s"""{"$topicName": [$partitionNum]}"""
    val startingOffsetsJson =
      s"""{"$topicName": {"$partitionNum": $startOffset}}"""
    val endingOffsetsJson = s"""{"$topicName": {"$partitionNum": $endOffset}}"""

    val kafkaOptions = Map(
      "kafka.bootstrap.servers" -> brokerUrls,
      "assign" -> assignJson,
      "startingOffsets" -> startingOffsetsJson,
      "endingOffsets" -> endingOffsetsJson
    )

    val kafkaDf = spark.read
      .format("kafka")
      .options(kafkaOptions)
      .load()

    val df = kafkaSource.getSchemaType match {
      case SchemaType.SCHEMA_TYPE_UNSPECIFIED | SchemaType.UNRECOGNIZED =>
        throw new IllegalArgumentException(
          "Kafka source does not have associated schema"
        )
      case SchemaType.PROTOBUF =>
        throw new UnsupportedOperationException(
          "Kafka table has Protobuf encoding. from_protobuf requires proto definition " +
            "on classpath or file descriptor."
        )
      case SchemaType.AVRO =>
        val avroSchema = kafkaSource.getSchema.replace("'", "\\'")
        kafkaDf
          .selectExpr(
            s"""from_avro(substring(value, 6), '$avroSchema') as val"""
          )
          .selectExpr("val.*")
    }

    val partitionFilter = kafkaSource.getPartitionFilter
    if (partitionFilter.nonEmpty) {
      df.where(partitionFilter)
    } else {
      df
    }
  }

  private def loadYugabyteDataFrame(
      yugabyteSource: YugabyteDBSourceMessage
  ): DataFrame = {
    val readTimestamp = yugabyteSource.getReadTimestamp
    val jdbcUrl = yugabyteSource.getJdbcUrl
    val dbTable = yugabyteSource.getTableName
    val username = yugabyteSource.getUsername
    val password = yugabyteSource.getPassword

    val jdbcOptions = Map(
      "url" -> jdbcUrl,
      "driver" -> "org.postgresql.Driver",
      "dbtable" -> dbTable,
      "user" -> username,
      "password" -> password,
      "sessionInitStatement" -> s"SET yb_read_time TO $readTimestamp"
    )

    val df = spark.read
      .format("jdbc")
      .options(jdbcOptions)
      .load()

    val partitionFilter = yugabyteSource.getPartitionFilter
    if (partitionFilter.nonEmpty) {
      df.where(partitionFilter)
    } else {
      df
    }
  }

  private def loadPostgresDataFrame(
      postgresSource: PostgresDBSourceMessage
  ): DataFrame = {
    val readTimestamp = postgresSource.getReadTimestamp
    val jdbcUrl = postgresSource.getJdbcUrl
    val tableName = postgresSource.getTableName
    val historyTableName = postgresSource.getHistoryTableName
    val username = postgresSource.getUsername
    val password = postgresSource.getPassword

    val timestamp = new java.sql.Timestamp(readTimestamp)
    val query =
      s"""(
      SELECT * FROM (
        SELECT * FROM $tableName
        UNION ALL
        SELECT * FROM $historyTableName
      ) AS combined
      WHERE sys_period @> '$timestamp'::timestamptz
    )"""

    val jdbcOptions = Map(
      "url" -> jdbcUrl,
      "driver" -> "org.postgresql.Driver",
      "query" -> query,
      "user" -> username,
      "password" -> password
    )

    val df = spark.read
      .format("jdbc")
      .options(jdbcOptions)
      .load()

    val partitionFilter = postgresSource.getPartitionFilter
    if (partitionFilter.nonEmpty) {
      df.where(partitionFilter)
    } else {
      df
    }
  }

  private def loadIcebergDataFrame(
      icebergSource: IcebergSourceMessage
  ): DataFrame = {
    val catalog = icebergSource.getSparkCatalogName
    val schema = icebergSource.getSparkSchemaName
    val table = icebergSource.getTableName
    val tablePath = s"$catalog.$schema.$table"
    val readTimestamp = icebergSource.getReadTimestamp

    val df = spark.read
      .format("iceberg")
      .option("as-of-timestamp", readTimestamp)
      .table(tablePath)

    val partitionFilter = icebergSource.getPartitionFilter
    if (partitionFilter.nonEmpty) {
      df.where(partitionFilter)
    } else {
      df
    }
  }

  private def projectToSchema(
      df: DataFrame,
      schema: StructType
  ): DataFrame = {
    val schemaFieldNames = schema.fieldNames
    val dfColumns = df.columns

    val selectedExpressions = schemaFieldNames.map { name =>
      if (dfColumns.contains(name)) {
        s"`$name`"
      } else {
        val field = schema(name)
        val typeStr = field.dataType.sql
        s"CAST(NULL AS $typeStr) as `$name`"
      }
    }.toIndexedSeq

    df.selectExpr(selectedExpressions: _*)
  }
}
