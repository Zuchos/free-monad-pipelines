package org.zuchos.free_monad_pipelines.plan

import cats.implicits._
import org.zuchos.free_monad_pipelines.model.TableMetadata

object PipelinePlan {

  private def detectAndTransformDateColumns(tableName: String, metadata: TableMetadata): PlanAction[Unit] = for {
    dateColumns <- profileTable(DateColumnsDetector(tableName, metadata.columns))
    _ <- transformTable(DateColumnTransformer(tableName, dateColumns))
  } yield ()

  val planForAllTables: PlanAction[Unit] = for {
    tableMetadata <- getMetadata
    _ <- tableMetadata.toList.traverse {
      case (tableName, metadata) => detectAndTransformDateColumns(tableName, metadata)
    }
  } yield ()
}
