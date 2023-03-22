package org.zuchos.free_monad_pipelines.plan

import cats.{ Applicative, Monad, ~> }
import cats.implicits._
import cats.mtl.Stateful
import org.zuchos.free_monad_pipelines.TransformerOps
import org.zuchos.free_monad_pipelines.model.DataModel

class PipelineStageExecutor[F[_]: Monad, ActualDataType](transformerOps: TransformerOps[F, ActualDataType])(implicit Applicative: Applicative[F]) {
  def execute(dataModel: DataModel[ActualDataType], pipelineStages: List[PipelineStage[_]]): F[DataModel[ActualDataType]] = {
    val value: F[DataModel[ActualDataType]] = Applicative.pure(dataModel)
    pipelineStages.foldLeft(value) {
      case (currentDataWrapped, stage: Transformer) =>
        currentDataWrapped.flatMap { currentData => transformerOps.applyTransformation(currentData, stage) }
      case (currentDataWrapped, _) => currentDataWrapped
    }
  }
}

class PipelineStageExecutor2[F[_]: Monad, ActualDataType](
    transformerOps: TransformerOps[F, ActualDataType]
)(implicit Applicative: Applicative[F], state: Stateful[F, DataModel[ActualDataType]])
  extends (PipelineStage ~> F) {
  override def apply[A](fa: PipelineStage[A]): F[A] = {
    for {
      currentData <- state.get
      updatedData <- fa match {
        case stage: Transformer =>
          transformerOps.applyTransformation(currentData, stage)
        case _ => Applicative.pure(currentData)
      }
      _ <- state.set(updatedData)
    } yield ().asInstanceOf[A]
  }
}
