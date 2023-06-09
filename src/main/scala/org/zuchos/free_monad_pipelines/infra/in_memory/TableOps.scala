package org.zuchos.free_monad_pipelines.infra.in_memory

import cats.data.{ StateT, WriterT }
import cats.effect.IO
import org.zuchos.free_monad_pipelines.application.{ProfilingOps, TransformerOps}
import org.zuchos.free_monad_pipelines.domain.TableMetadata.{ ColumnName, ColumnType }
import org.zuchos.free_monad_pipelines.domain
import org.zuchos.free_monad_pipelines.domain.{DataModel, DateColumnTransformer, ExecutionJournal, Profiler, TableMetadata, Transformer}

object TableOps {

  case class Table(columns: Map[ColumnName, List[Any]])

  class TableDateColumnsDetector(tableName: String, allColumns: Map[ColumnName, ColumnType]) {
    def detect(dataModel: DataModel[Table]): Set[String] = {
      dataModel.metadata(tableName).columns.filter(_._2 == "string").keySet
    }
  }

  class NullRatioCalculator(tableName: String, nullableColumns: Set[ColumnName]) {
    def calculate(dataModel: DataModel[Table]): Map[String, Double] = {
      dataModel.data(tableName).columns.filter(p => nullableColumns.contains(p._1)).map {
        case (columnName, columnValues) if columnValues.nonEmpty => columnName -> (columnValues.count(_ == null) / columnValues.size.toDouble)
        case (columnName, _)                                     => columnName -> Double.NaN
      }
    }
  }

  type PlanState[A] = StateT[IO, DataModel[Table], A]
  type PlanMonad[A] = WriterT[PlanState, ExecutionJournal, A]

  val ioTransformerOps = new TransformerOps[IO, Table] {
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
    override def applyTransformation(dataModel: DataModel[Table], tableTransformer: Transformer): IO[DataModel[Table]] = {
      tableTransformer match {
        case DateColumnTransformer(tableName, dateColumns) =>
          val updatedColumns = dataModel.metadata(tableName).columns.map {
            case (colName, _) if dateColumns.contains(colName) => colName -> "date"
            case p                                             => p
          }
          val updatedTable = Table(dataModel.data(tableName).columns.map {
            case (colName, columnData) if dateColumns.contains(colName) => colName -> columnData.map {
              case s: String => format.parse(s)
              case null => null
            }
            case p                                                      => p
          })
          IO.pure(
            dataModel.copy(
              metadata = dataModel.metadata + (tableName -> TableMetadata(updatedColumns)),
              data = dataModel.data + (tableName -> updatedTable)
            )
          )
      }
    }
  }

  implicit class ExecutionPlanResultSyntax[A](io: IO[A]) {
    def liftToPlanMonad: PlanMonad[A] = WriterT.liftF[PlanState, ExecutionJournal, A](StateT.liftF(io))
  }

  implicit val transformerOps = new TransformerOps[PlanMonad, Table] {
    override def applyTransformation(dataModel: DataModel[Table], tableTransformer: Transformer): PlanMonad[DataModel[Table]] = {
      ioTransformerOps.applyTransformation(dataModel, tableTransformer).liftToPlanMonad
    }
  }

  val ioProfilingOps: ProfilingOps[IO, Table] = new ProfilingOps[IO, Table] {
    override def applyProfiling[A](dataModel: DataModel[Table], tableProfiler: Profiler[A]): IO[A] = {
      tableProfiler match {
        case dd: domain.DateColumnsDetector => IO.pure(new TableDateColumnsDetector(dd.tableName, dd.allColumns).detect(dataModel))
        case dd: domain.NullRatioCalculator => IO.pure(new NullRatioCalculator(dd.tableName, dd.nullableColumns).calculate(dataModel))
      }
    }
  }

  implicit val profilingOps: ProfilingOps[PlanMonad, Table] = new ProfilingOps[PlanMonad, Table] {
    override def applyProfiling[A](dataModel: DataModel[Table], tableProfiler: Profiler[A]): PlanMonad[A] = {
      ioProfilingOps.applyProfiling(dataModel, tableProfiler).liftToPlanMonad
    }
  }
}
