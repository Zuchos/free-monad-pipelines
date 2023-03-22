package org.zuchos.free_monad_pipelines

import cats.{ Applicative, Monoid }
import cats.free.Free
import org.zuchos.free_monad_pipelines.model.TableMetadata.{ ColumnName, ColumnType, TableName }
import org.zuchos.free_monad_pipelines.model._

package object plan {

  //region Pipeline stages

  sealed trait PipelineStage[StageResult]

  sealed trait Profiler[DataProfile] extends PipelineStage[DataProfile]
  sealed trait Transformer extends PipelineStage[Unit]

  case object GetMetadata extends PipelineStage[Map[TableName, TableMetadata]]

  //endregion

  //region Actual transformers and profiles

  case class NullRatioCalculator(tableName: String, nullableColumns: Set[ColumnName]) extends Profiler[Map[ColumnName, Double]]
  case class DateColumnsDetector(tableName: String, allColumns: Map[ColumnName, ColumnType]) extends Profiler[Set[ColumnName]]

  case class DateColumnTransformer(tableName: String, dateColumns: Set[ColumnName]) extends Transformer

  //endregion

  //region Utils

  type PipelineAction[StageResult] = Free[PipelineStage, StageResult]

  def getMetadata: PipelineAction[Map[String, TableMetadata]] = {
    Free.liftF[PipelineStage, Map[String, TableMetadata]](GetMetadata)
  }

  def transform(tableTransformer: Transformer): PipelineAction[Unit] = {
    Free.liftF[PipelineStage, Unit](tableTransformer)
  }

  def profile[ProfilerResult](tableProfiler: Profiler[ProfilerResult]): PipelineAction[ProfilerResult] = {
    Free.liftF(tableProfiler)
  }

  private def pure[A](a: A): PipelineAction[A] = {
    Applicative[PipelineAction].pure(a)
  }

  def noOpAction: PipelineAction[Unit] = {
    Applicative[PipelineAction].pure(())
  }

  def liftToTransformationPlan(stages: List[PipelineStage[_]]): PipelineAction[Unit] = {
    stages.foldLeft(pure(())) {
      case (previousStage, transformer: Transformer) =>
        previousStage.flatMap(_ => transform(transformer))
      case (previousStage, _) => previousStage
    }
  }

  //endregion

  //region Execution Journal

  final case class ExecutionJournal(
      stages: List[PipelineStage[_]] = List.empty
  ) {
    override def toString: ColumnName = {
      "Executed Stages:\n" + stages.map(s => s"\t-$s\n").mkString
    }
  }

  object ExecutionJournal {
    def apply(stage: PipelineStage[_]): ExecutionJournal = {
      new ExecutionJournal(List(stage))
    }

    implicit val executionJournalMonoid: Monoid[ExecutionJournal] = new Monoid[ExecutionJournal] {
      override def empty: ExecutionJournal = ExecutionJournal(List.empty)

      override def combine(x: ExecutionJournal, y: ExecutionJournal): ExecutionJournal =
        ExecutionJournal(x.stages ++ y.stages)
    }
  }

  //endregion
}
