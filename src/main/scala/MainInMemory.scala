import cats.effect.unsafe.implicits.global
import org.zuchos.free_monad_pipelines.infra.in_memory.TableOps._
import org.zuchos.free_monad_pipelines.{ PipelinePlanCompiler, plan }
import org.zuchos.free_monad_pipelines.model.{ DataModel, TableMetadata }
import org.zuchos.free_monad_pipelines.plan.{ PipelineAction, PipelinePlan, liftToTransformationPlan }

object MainInMemory extends App {
  {
    val table1 = "customers_table"
    val table2 = "products_table"
    println("-----------Training Pipeline----------------------")
    //region "Training" pipeline

    val planForAllTables: PipelineAction[PipelinePlan.DataProfile] = PipelinePlan.planForAllTables
    val planCompiler: PipelinePlanCompiler[PlanMonad, Table] = new PipelinePlanCompiler[PlanMonad, Table]()
    val compiledPipelinePlan: PlanMonad[PipelinePlan.DataProfile] = planForAllTables.foldMap(planCompiler)
    // PlanMonad[PipelinePlan.DataProfile] ==
    //  WriterT[StateT[IO, DataModel[Table], PipelinePlan.DataProfile], ExecutionJournal, PipelinePlan.DataProfile]
    val dataModelInTraining: DataModel[Table] = new DataModel[Table](
      metadata = Map(
        table1 -> TableMetadata(Map("date_column" -> "string")),
        table2 -> TableMetadata(Map("product_id" -> "int"))
      ),
      data = Map(
        table1 -> Table(Map("date_column" -> List("2023-03-28"))),
        table2 -> Table(Map("product_id" -> List(123, null)))
      )
    )

    //result of "Training" pipeline
    val (
      dataModelAfterTraining: DataModel[Table] /* from State */,
      (
        executionJournal: plan.ExecutionJournal /* from Writer aka Journal */,
        profile: PipelinePlan.DataProfile /* "A" */
      )
    ) = compiledPipelinePlan.run.run(dataModelInTraining).unsafeRunSync()

    println(executionJournal)
    println(dataModelAfterTraining)
    println(profile)
    //endregion
    println("-----------Prediction Pipeline--------------------")
    //region "Prediction" pipeline

    val dataModelInPrediction: DataModel[Table] = new DataModel[Table](
      metadata = Map(
        table1 -> TableMetadata(Map("date_column" -> "string")),
        table2 -> TableMetadata(Map("product_id" -> "int"))
      ),
      data = Map(
        table1 -> Table(Map("date_column" -> List("2023-03-28", null, null, "2023-03-29"))),
        table2 -> Table(Map("product_id" -> List(123, null, null, 42)))
      )
    )

    val transformationPlan: PipelineAction[Unit] = liftToTransformationPlan(executionJournal.stages)

    val (transformedDataModelInPrediction, _) = transformationPlan.foldMap(planCompiler).run.run(dataModelInPrediction).unsafeRunSync()
    println(transformedDataModelInPrediction)
    //endregion
    println("--------------------------------------------------")
  }
}
