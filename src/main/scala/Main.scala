import cats.effect.unsafe.implicits.global
import org.zuchos.free_monad_pipelines.infra.custom.TableOps.{ PlanMonad, Table }
import org.zuchos.free_monad_pipelines.{ PlanCompiler, plan }
import org.zuchos.free_monad_pipelines.model.{ DataModel, TableMetadata }
import org.zuchos.free_monad_pipelines.plan.{ PipelineAction, PipelinePlan, liftToTransformationPlan }

object Main extends App {
  {

    val table1 = "customers_table"
    val table2 = "products_table"

    val planForAllTables: PipelineAction[PipelinePlan.DataProfile] = PipelinePlan.planForAllTables
    val planCompiler: PlanCompiler[PlanMonad, Table] = new PlanCompiler[PlanMonad, Table]()
    val compiledPipelinePlan: PlanMonad[PipelinePlan.DataProfile] = planForAllTables.foldMap(planCompiler)

    val dataModel: DataModel[Table] = new DataModel[Table](
      metadata = Map(
        table1 -> TableMetadata(Map("date_column" -> "string")),
        table2 -> TableMetadata(Map("product_id" -> "int"))
      ),
      data = Map(
        table1 -> Table(Map("date_column" -> List("2023-03-28"))),
        table2 -> Table(Map("product_id" -> List(123, null)))
      )
    )

    val (
      updatedDataModel: DataModel[Table] /* from State */,
      (
        executionPlan: plan.ExecutionJournal /* from Writer aka Journal */,
        profile: PipelinePlan.DataProfile /* "A" */
      )
    ) = compiledPipelinePlan.run
      .run(
        dataModel
      )
      .unsafeRunSync()

    println(executionPlan)
    println(updatedDataModel)
    println(profile)

    val transformationPlan = liftToTransformationPlan(executionPlan.stages)

    val (transformedDataModel, _) = transformationPlan.foldMap(planCompiler).run.run(dataModel).unsafeRunSync()
    println(transformedDataModel)
  }
}
