import cats.effect.unsafe.implicits.global
import org.zuchos.free_monad_pipelines.infra.custom.TableOps.Table
import org.zuchos.free_monad_pipelines.infra.custom.TableOps.PlanMonad
import org.zuchos.free_monad_pipelines.PlanCompiler
import org.zuchos.free_monad_pipelines.model.{ DataModel, TableMetadata }
import org.zuchos.free_monad_pipelines.plan.PipelinePlan

object Main extends App {
  println("Hello, World!")

  private val tableName = "customers_table"

  private val value: PlanMonad[Unit] = PipelinePlan.planForAllTables.foldMap(new PlanCompiler[PlanMonad, Table]())

  val result: (DataModel[Table], Unit) = value
    .run(
      new DataModel[Table](Map(tableName -> TableMetadata(Map("date_column" -> "string"))), Map(tableName -> Table(Map("date_column" -> List("2023-03-28")))))
    )
    .unsafeRunSync()

  println(result)
}
