package org.zuchos.free_monad_pipelines

import cats.Applicative
import cats.free.Free
import org.zuchos.free_monad_pipelines.model._

package object plan {

  sealed trait PlanStage[StageResult]
  type PlanAction[ActionResult] = Free[PlanStage, ActionResult]

  case object GetMetadata extends PlanStage[Map[String, TableMetadata]]

  sealed trait TableTransformer extends PlanStage[Unit]
  case class DateColumnTransformer(tableName: String, dateColumns: Set[String]) extends TableTransformer

  sealed trait TableProfiler[ProfilingResult] extends PlanStage[ProfilingResult]
  case class DateColumnsDetector(tableName: String, allColumns: Map[String, String]) extends TableProfiler[Set[String]]

  def getMetadata: PlanAction[Map[String, TableMetadata]] = {
    Free.liftF[PlanStage, Map[String, TableMetadata]](GetMetadata)
  }

  def pure[A](a: A): PlanAction[A] = {
    Applicative[PlanAction].pure(a)
  }

  def transformTable(tableTransformer: TableTransformer): PlanAction[Unit] = {
    Free.liftF(tableTransformer)
  }

  def profileTable[ProfilerResult](tableProfiler: TableProfiler[ProfilerResult]): PlanAction[ProfilerResult] = {
    Free.liftF(tableProfiler)
  }
}
