package org.zuchos.free_monad_pipelines.plan

import cats.implicits._
import org.zuchos.free_monad_pipelines.model.TableMetadata

object PipelinePlan {

  final case class TableProfile(nullRatios: Map[String, Double])
  object DataProfile {
    val empty = DataProfile(Map.empty)
  }
  final case class DataProfile(tableProfiles: Map[String, TableProfile])

  private def pipelineForSingleTable(tableName: String, metadata: TableMetadata): PipelineAction[TableProfile] = {
    for {
      dateColumns <- profileTable(DateColumnsDetector(tableName, metadata.columns))
      _ <- if (dateColumns.nonEmpty) {
        transformTable(DateColumnTransformer(tableName, dateColumns))
      } else {
        noOpAction
      }
      nullColumnRatios <- profileTable(NullRatioCalculator(tableName, metadata.columns))
    } yield TableProfile(nullColumnRatios)
  }

  val planForAllTables: PipelineAction[DataProfile] = for {
    tableMetadata <- getMetadata
    tableProfiles <- tableMetadata.toList.traverse {
      case (tableName, metadata) => pipelineForSingleTable(tableName, metadata).map(tableName -> _)
    }
  } yield DataProfile(tableProfiles.toMap)
}
