import Main.updatedDataModel
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.free.Free
import org.zuchos.free_monad_pipelines.infra.custom.TableOps.{ PlanMonad, PlanState, Table }
import org.zuchos.free_monad_pipelines.PlanCompiler
import org.zuchos.free_monad_pipelines.infra.custom.TableOps
import org.zuchos.free_monad_pipelines.model.{ DataModel, TableMetadata }
import org.zuchos.free_monad_pipelines.plan.{ PipelineAction, PipelinePlan, PipelineStage, PipelineStageExecutor, PipelineStageExecutor2, pure }

object Main extends App {

  private val table1 = "customers_table"
  private val table2 = "products_table"

  private val planForAllTables: PipelineAction[PipelinePlan.DataProfile] = PipelinePlan.planForAllTables
  private val compiledPipelinePlan = planForAllTables.foldMap(new PlanCompiler[PlanMonad, Table]())

  private val dataModel: DataModel[Table] = new DataModel[Table](
    metadata = Map(
      table1 -> TableMetadata(Map("date_column" -> "string")),
      table2 -> TableMetadata(Map("product_id" -> "int"))
    ),
    data = Map(
      table1 -> Table(Map("date_column" -> List("2023-03-28"))),
      table2 -> Table(Map("product_id" -> List(123, null)))
    )
  )

  val (updatedDataModel, (executionPlan, profile)) = compiledPipelinePlan.run
    .run(
      dataModel
    )
    .unsafeRunSync()

  println(executionPlan)
  println(updatedDataModel)
  println(profile)

  val updatedDataModel2 = new PipelineStageExecutor[IO, Table](TableOps.ioTransformerOps).execute(dataModel, executionPlan.stages).unsafeRunSync()
  println(updatedDataModel2)

  val value: List[Free[PipelineStage, Unit]] = executionPlan.stages.map(s => Free.liftF[PipelineStage, Unit](s.asInstanceOf[PipelineStage[Unit]]))
  val r: PipelineAction[Unit] = value.reduce { (a: Free[PipelineStage, Unit], b: Free[PipelineStage, Unit]) => a.flatMap(_ => b) }
  val updatedModel3 = r.foldMap(new PipelineStageExecutor2[PlanMonad, Table](TableOps.transformerOps)).run.run(dataModel).unsafeRunSync()._1
  println(updatedModel3)

}
