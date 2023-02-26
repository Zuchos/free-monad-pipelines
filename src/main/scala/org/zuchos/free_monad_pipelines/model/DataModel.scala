package org.zuchos.free_monad_pipelines.model

case class TableMetadata(columns: Map[String, String])
case class DataModel[ActualTableType](metadata: Map[String, TableMetadata], data: Map[String, ActualTableType])
