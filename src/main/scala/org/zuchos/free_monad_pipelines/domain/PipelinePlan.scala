package org.zuchos.free_monad_pipelines.domain

import cats.implicits._
import TableMetadata.{ ColumnName, TableName }

object PipelinePlan {

  //region details

  final case class TableProfile(nullRatios: Map[ColumnName, Double]) {
    override def toString: ColumnName = {
      nullRatios
        .map {
          case (columnName, nullRatio) => s"$columnName - nullRatio: $nullRatio"
        }
        .mkString(",")
    }
  }
  object DataProfile {
    val empty = DataProfile(Map.empty)
  }
  final case class DataProfile(tableProfiles: Map[TableName, TableProfile])

  //endregion

  private def pipelineForSingleTable(tableName: TableName, metadata: TableMetadata): PipelineAction[TableProfile] = {
    for {
      dateColumns <- profile(DateColumnsDetector(tableName, metadata.columns))
      _ <- if (dateColumns.nonEmpty) {
        transform(DateColumnTransformer(tableName, dateColumns))
      } else {
        noOpAction
      }
      nullColumnRatios <- profile(NullRatioCalculator(tableName, metadata.columns.keySet))
    } yield TableProfile(nullColumnRatios)
  }

  val planForAllTables: PipelineAction[DataProfile] = for {
    tablesMetadata <- getMetadata
    tableProfiles <- tablesMetadata.toList.traverse {
      case (tableName, metadata) => pipelineForSingleTable(tableName, metadata).map(tableName -> _)
    }
  } yield DataProfile(tableProfiles.toMap)
}
