package org.zuchos.free_monad_pipelines.infra.spark

import cats.data.{ StateT, WriterT }
import cats.effect.IO
import org.zuchos.free_monad_pipelines.domain.TableMetadata.{ ColumnName, ColumnType }
import org.zuchos.free_monad_pipelines.domain
import org.zuchos.free_monad_pipelines.domain.{DataModel, DateColumnTransformer, ExecutionJournal, Profiler, TableMetadata, Transformer}

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.zuchos.free_monad_pipelines.application.{ProfilingOps, TransformerOps}

object DataFrameOps {

  class TableDateColumnsDetector(tableName: String, allColumns: Map[ColumnName, ColumnType]) {
    def detect(dataModel: DataModel[DataFrame]): Set[String] = {
      dataModel.metadata(tableName).columns.filter(_._2 == "string").keySet
    }
  }

  class NullRatioCalculator(tableName: String, nullableColumns: Set[ColumnName]) {
    def calculate(dataModel: DataModel[DataFrame]): Map[String, Double] = {
      val dataFrame = dataModel.data(tableName)
      //count null with spark
      val countNullColumns = nullableColumns.map(colName => count(when(isnull(col(colName)), 1)).as(colName)).toList
      val totalRows = dataFrame.count()
      val frame = dataFrame.select(countNullColumns: _*)
      frame
        .collect()
        .headOption
        .map { row =>
          nullableColumns.map { colName =>
            val result = row.getAs[Long](colName).toDouble / totalRows.toDouble
            colName -> result
          }.toMap
        }
        .getOrElse(Map.empty)
    }
  }

  type PlanState[A] = StateT[IO, DataModel[DataFrame], A]
  type PlanMonad[A] = WriterT[PlanState, ExecutionJournal, A]

  val ioTransformerOps = new TransformerOps[IO, DataFrame] {
    override def applyTransformation(dataModel: DataModel[DataFrame], tableTransformer: Transformer): IO[DataModel[DataFrame]] = {
      tableTransformer match {
        case DateColumnTransformer(tableName, dateColumns: Set[ColumnName]) =>
          IO {
            val dataFrame = dataModel.data(tableName)
            val convertedDateColumns: List[Column] = dateColumns.toList.map(dc => to_date(col(dc)).as(dc))
            val otherColumns: List[Column] = (dataFrame.columns.toSet -- dateColumns).map(col).toList
            val updatedTable = dataFrame.select(otherColumns ++ convertedDateColumns: _*)
            val updatedColumns = dataModel.metadata(tableName).columns.map {
              case (colName, _) if dateColumns.contains(colName) => colName -> "date"
              case p                                             => p
            }
            dataModel.copy(
              metadata = dataModel.metadata + (tableName -> TableMetadata(updatedColumns)),
              data = dataModel.data + (tableName -> updatedTable)
            )
          }
      }
    }
  }

  implicit class ExecutionPlanResultSyntax[A](io: IO[A]) {
    def liftToPlanMonad: PlanMonad[A] = WriterT.liftF[PlanState, ExecutionJournal, A](StateT.liftF(io))
  }

  implicit val transformerOps = new TransformerOps[PlanMonad, DataFrame] {
    override def applyTransformation(dataModel: DataModel[DataFrame], tableTransformer: Transformer): PlanMonad[DataModel[DataFrame]] = {
      ioTransformerOps.applyTransformation(dataModel, tableTransformer).liftToPlanMonad
    }
  }

  val ioProfilingOps: ProfilingOps[IO, DataFrame] = new ProfilingOps[IO, DataFrame] {
    override def applyProfiling[A](dataModel: DataModel[DataFrame], tableProfiler: Profiler[A]): IO[A] = {
      tableProfiler match {
        case dd: domain.DateColumnsDetector => IO(new TableDateColumnsDetector(dd.tableName, dd.allColumns).detect(dataModel))
        case dd: domain.NullRatioCalculator => IO(new NullRatioCalculator(dd.tableName, dd.nullableColumns).calculate(dataModel))
      }
    }
  }

  implicit val profilingOps: ProfilingOps[PlanMonad, DataFrame] = new ProfilingOps[PlanMonad, DataFrame] {
    override def applyProfiling[A](dataModel: DataModel[DataFrame], tableProfiler: Profiler[A]): PlanMonad[A] = {
      ioProfilingOps.applyProfiling(dataModel, tableProfiler).liftToPlanMonad
    }
  }
}
