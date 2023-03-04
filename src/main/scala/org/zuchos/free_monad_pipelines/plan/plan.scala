package org.zuchos.free_monad_pipelines

import cats.{ Applicative, Monoid }
import cats.free.Free
import org.zuchos.free_monad_pipelines.model._

package object plan {

  final case class ExecutionJournal(
      stages: List[PipelineStage[_]] = List.empty
  )

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

  //region A
  sealed trait PipelineStage[StageResult]
  //endregion
  type PipelineAction[StageResult] = Free[PipelineStage, StageResult]

  case object GetMetadata extends PipelineStage[Map[String, TableMetadata]]
  sealed trait TableProfiler[ProfilingResult] extends PipelineStage[ProfilingResult]
  sealed trait TableTransformer extends PipelineStage[Unit]

  //region ActualTransformersAndProfiles
  case class DateColumnTransformer(tableName: String, dateColumns: Set[String]) extends TableTransformer

  case class DateColumnsDetector(tableName: String, allColumns: Map[String, String]) extends TableProfiler[Set[String]]
  case class NullRatioCalculator(tableName: String, nullableColumns: Map[String, String]) extends TableProfiler[Map[String, Double]]
  //endregion

  def getMetadata: PipelineAction[Map[String, TableMetadata]] = {
    Free.liftF[PipelineStage, Map[String, TableMetadata]](GetMetadata)
  }

  def pure[A](a: A): PipelineAction[A] = {
    Applicative[PipelineAction].pure(a)
  }

  def noOpAction[A]: PipelineAction[Unit] = {
    Applicative[PipelineAction].pure(())
  }

  def transformTable(tableTransformer: TableTransformer): PipelineAction[Unit] = {
    Free.liftF[PipelineStage, Unit](tableTransformer)
  }

  def profileTable[ProfilerResult](tableProfiler: TableProfiler[ProfilerResult]): PipelineAction[ProfilerResult] = {
    Free.liftF(tableProfiler)
  }
}
