package org.zuchos.free_monad_pipelines

import cats.implicits._
import cats.mtl.Stateful
import cats.{ Monad, ~> }
import org.zuchos.free_monad_pipelines.model.DataModel
import org.zuchos.free_monad_pipelines.plan.{ GetMetadata, PlanStage, TableProfiler, TableTransformer }

trait TransformerOps[F[_], ActualDataType] {
  def applyTransformation(dataModel: DataModel[ActualDataType], tableTransformer: TableTransformer): F[DataModel[ActualDataType]]
}

trait ProfilingOps[F[_], ActualDataType] {
  def applyProfiling[A](dataModel: DataModel[ActualDataType], tableProfiler: TableProfiler[A]): F[A]
}

class PlanCompiler[F[_]: Monad, ActualDataType](
    implicit State: Stateful[F, DataModel[ActualDataType]],
    transformerOpt: TransformerOps[F, ActualDataType],
    profilingOps: ProfilingOps[F, ActualDataType]
) extends (PlanStage ~> F) {
  override def apply[StageResult](fa: PlanStage[StageResult]): F[StageResult] = {
    (fa match {
      case transformer: TableTransformer =>
        for {
          model <- State.get
          updatedModel <- transformerOpt.applyTransformation(model, transformer)
          _ <- State.set(updatedModel)
        } yield ()
      case profiler: TableProfiler[StageResult] =>
        for {
          model <- State.get
          profilingResult <- profilingOps.applyProfiling(model, profiler)
        } yield profilingResult
      case GetMetadata => State.inspect(_.metadata)
    }): F[StageResult]
  }

}
