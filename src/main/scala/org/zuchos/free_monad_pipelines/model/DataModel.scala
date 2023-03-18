package org.zuchos.free_monad_pipelines.model

import org.zuchos.free_monad_pipelines.model.TableMetadata.{ ColumnName, ColumnType, TableName }

object TableMetadata {
  type TableName = String
  type ColumnName = String
  type ColumnType = String
}

case class TableMetadata(columns: Map[ColumnName, ColumnType])
case class DataModel[ActualTableType](metadata: Map[TableName, TableMetadata], data: Map[TableName, ActualTableType])
