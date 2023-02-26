package org.zuchos.free_monad_pipelines.infra.custom

import cats.data.StateT
import cats.effect.IO
import org.zuchos.free_monad_pipelines.{ ProfilingOps, TransformerOps }
import org.zuchos.free_monad_pipelines.model.{ DataModel, TableMetadata }
import org.zuchos.free_monad_pipelines.plan.{ DateColumnTransformer, DateColumnsDetector, TableProfiler, TableTransformer }

import java.text.SimpleDateFormat

object TableOps {

  case class Table(columns: Map[String, List[Any]])

  class TableDateColumnsDetector(tableName: String, allColumns: Map[String, String]) {
    def detect(dataModel: DataModel[Table]): Set[String] = {
      dataModel.metadata(tableName).columns.filter(_._2 == "string").keySet
    }
  }

  type PlanMonad[A] = StateT[IO, DataModel[Table], A]

  val ioTransformerOps = new TransformerOps[IO, Table] {
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
    override def applyTransformation(dataModel: DataModel[Table], tableTransformer: TableTransformer): IO[DataModel[Table]] = {
      tableTransformer match {
        case DateColumnTransformer(tableName, dateColumns) =>
          val updatedColumns = dataModel.metadata(tableName).columns.map {
            case (colName, _) if dateColumns.contains(colName) => colName -> "date"
            case p                                             => p
          }
          val updatedTable = Table(dataModel.data(tableName).columns.map {
            case (colName, columnData) if dateColumns.contains(colName) => colName -> columnData.map { case s: String => format.parse(s) }
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

  implicit val transformerOps = new TransformerOps[PlanMonad, Table] {
    override def applyTransformation(dataModel: DataModel[Table], tableTransformer: TableTransformer): PlanMonad[DataModel[Table]] = {
      StateT.liftF(ioTransformerOps.applyTransformation(dataModel, tableTransformer))
    }
  }

  val ioProfilingOps: ProfilingOps[IO, Table] = new ProfilingOps[IO, Table] {
    override def applyProfiling[A](dataModel: DataModel[Table], tableProfiler: TableProfiler[A]): IO[A] = {
      tableProfiler match {
        case dd: DateColumnsDetector => IO.pure(new TableDateColumnsDetector(dd.tableName, dd.allColumns).detect(dataModel))
      }
    }
  }

  implicit val profilingOps: ProfilingOps[PlanMonad, Table] = new ProfilingOps[PlanMonad, Table] {
    override def applyProfiling[A](dataModel: DataModel[Table], tableProfiler: TableProfiler[A]): PlanMonad[A] = {
      StateT.liftF(ioProfilingOps.applyProfiling(dataModel, tableProfiler))
    }
  }
}
