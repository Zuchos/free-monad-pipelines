package org.zuchos.free_monad_pipelines

import cats.implicits._
import cats.mtl.{ Stateful, Tell }
import cats.{ Monad, ~> }
import org.zuchos.free_monad_pipelines.model.DataModel
import org.zuchos.free_monad_pipelines.plan.{ ExecutionJournal, GetMetadata, PipelineStage, Profiler, Transformer }

//region Monad capabilities

trait TransformerOps[F[_], ActualDataType] {
  def applyTransformation(dataModel: DataModel[ActualDataType], tableTransformer: Transformer): F[DataModel[ActualDataType]]
}

trait ProfilingOps[F[_], ActualDataType] {
  def applyProfiling[A](dataModel: DataModel[ActualDataType], tableProfiler: Profiler[A]): F[A]
}

//endregion

class PipelinePlanCompiler[F[_]: Monad, ActualDataType](
    implicit State: Stateful[F, DataModel[ActualDataType]],
    AuditLog: Tell[F, ExecutionJournal],
    TransformerOps: TransformerOps[F, ActualDataType],
    ProfilingOps: ProfilingOps[F, ActualDataType]
) extends (PipelineStage ~> F) {
  override def apply[StageResult](fa: PipelineStage[StageResult]): F[StageResult] = {
    (fa match {
      case transformer: Transformer =>
        for {
          dataModel <- State.get
          updatedModel <- TransformerOps.applyTransformation(dataModel, transformer)
          _ <- AuditLog.tell(ExecutionJournal(transformer))
          _ <- State.set(updatedModel)
        } yield ()
      case profiler: Profiler[StageResult] =>
        for {
          dataModel <- State.get
          dataProfile <- ProfilingOps.applyProfiling(dataModel, profiler)
          _ <- AuditLog.tell(ExecutionJournal(profiler))
        } yield dataProfile
      case GetMetadata => State.get.map(_.metadata)
    }): F[StageResult]
  }

}
