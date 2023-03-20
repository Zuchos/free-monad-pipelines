package org.zuchos.free_monad_pipelines.model

import org.zuchos.free_monad_pipelines.model.TableMetadata.{ ColumnName, ColumnType, TableName }

object TableMetadata {
  type TableName = String
  type ColumnName = String
  type ColumnType = String
}

case class TableMetadata(columns: Map[ColumnName, ColumnType])
case class DataModel[ActualTableType](metadata: Map[TableName, TableMetadata], data: Map[TableName, ActualTableType]) {
  override def toString: ColumnName = {
    metadata.keySet
      .map { tableName =>
        s"Table - $tableName\n" +
          s"\tMetadata: ${metadata(tableName)}\n" ++
          s"\tData: ${data(tableName)}"
      }
      .mkString("\n\n")
  }
}
