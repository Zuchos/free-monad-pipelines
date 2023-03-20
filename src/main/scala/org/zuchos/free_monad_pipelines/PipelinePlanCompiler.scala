package org.zuchos.free_monad_pipelines

import cats.implicits._
import cats.mtl.{ Stateful, Tell }
import cats.{ Monad, ~> }
import org.zuchos.free_monad_pipelines.model.DataModel
import org.zuchos.free_monad_pipelines.plan.{ ExecutionJournal, GetMetadata, PipelineStage, TableProfiler, TableTransformer }

//region Monad capabilities

trait TransformerOps[F[_], ActualDataType] {
  def applyTransformation(dataModel: DataModel[ActualDataType], tableTransformer: TableTransformer): F[DataModel[ActualDataType]]
}

trait ProfilingOps[F[_], ActualDataType] {
  def applyProfiling[A](dataModel: DataModel[ActualDataType], tableProfiler: TableProfiler[A]): F[A]
}

//endregion

class PipelinePlanCompiler[F[_]: Monad, ActualDataType](
    implicit State: Stateful[F, DataModel[ActualDataType]],
    Journal: Tell[F, ExecutionJournal],
    transformerOps: TransformerOps[F, ActualDataType],
    profilingOps: ProfilingOps[F, ActualDataType]
) extends (PipelineStage ~> F) {
  override def apply[StageResult](fa: PipelineStage[StageResult]): F[StageResult] = {
    (fa match {
      case transformer: TableTransformer =>
        for {
          dataModel <- State.get
          updatedModel <- transformerOps.applyTransformation(dataModel, transformer)
          _ <- Journal.tell(ExecutionJournal(transformer))
          _ <- State.set(updatedModel)
        } yield ()
      case profiler: TableProfiler[StageResult] =>
        for {
          dataModel <- State.get
          profilingResult <- profilingOps.applyProfiling(dataModel, profiler)
          _ <- Journal.tell(ExecutionJournal(profiler))
        } yield profilingResult
      case GetMetadata => State.get.map(_.metadata)
    }): F[StageResult]
  }

}
