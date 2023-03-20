import cats.effect.unsafe.implicits.global
import org.apache.spark.sql.DataFrame
import org.zuchos.free_monad_pipelines.infra.spark.DataFrameOps._
import org.zuchos.free_monad_pipelines.model.{ DataModel, TableMetadata }
import org.zuchos.free_monad_pipelines.plan.{ PipelineAction, PipelinePlan, liftToTransformationPlan }
import org.zuchos.free_monad_pipelines.{ PipelinePlanCompiler, plan }

object MainSpark extends App {

  def printDataModel(dataModel: DataModel[DataFrame]) = {
    dataModel.data.foreach {
      case (tableName, df) =>
        println(s"Table $tableName")
        df.show()
        df.printSchema()
    }
  }
  {
    val table1 = "customers_table"
    val table2 = "products_table"

    val sparkSession = org.apache.spark.sql.SparkSession
      .builder()
      .master("local[*]")
      .appName("Datapipeline with Spark example")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    println("-----------Training Pipeline----------------------")
    //region "Training" pipeline

    val planForAllTables: PipelineAction[PipelinePlan.DataProfile] = PipelinePlan.planForAllTables
    val planCompiler: PipelinePlanCompiler[PlanMonad, DataFrame] = new PipelinePlanCompiler[PlanMonad, DataFrame]()
    val compiledPipelinePlan: PlanMonad[PipelinePlan.DataProfile] = planForAllTables.foldMap(planCompiler)

    import sparkSession.implicits._

    // PlanMonad[PipelinePlan.DataProfile] ==
    //  WriterT[StateT[IO, DataModel[Table], PipelinePlan.DataProfile], ExecutionJournal, PipelinePlan.DataProfile]
    val dataModelInTraining: DataModel[DataFrame] = new DataModel[DataFrame](
      metadata = Map(
        table1 -> TableMetadata(Map("date_column" -> "string")),
        table2 -> TableMetadata(Map("product_id" -> "int"))
      ),
      data = Map(
        table1 -> List("2023-03-28", "2023-03-29", null).toDF("date_column"),
        table2 -> List(123, 42).toDF("product_id")
      )
    )

    printDataModel(dataModelInTraining)

    //result of "Training" pipeline
    val (
      dataModelAfterTraining: DataModel[DataFrame] /* from State */,
      (
        executionJournal: plan.ExecutionJournal /* from Writer aka Journal */,
        profile: PipelinePlan.DataProfile /* "A" */
      )
    ) = compiledPipelinePlan.run.run(dataModelInTraining).unsafeRunSync()

    println(executionJournal)
    printDataModel(dataModelAfterTraining)
    println(profile)
    //endregion
    println("-----------Prediction Pipeline--------------------")
    //region "Prediction" pipeline

    val dataModelInPrediction: DataModel[DataFrame] = new DataModel[DataFrame](
      metadata = Map(
        table1 -> TableMetadata(Map("date_column" -> "string")),
        table2 -> TableMetadata(Map("product_id" -> "int"))
      ),
      data = Map(
        table1 -> List("2023-03-28", "2023-03-29", null, null, null).toDF("date_column"),
        table2 -> List(123, 42, 77).toDF("product_id")
      )
    )

    val transformationPlan: PipelineAction[Unit] = liftToTransformationPlan(executionJournal.stages)

    val (transformedDataModelInPrediction, _) = transformationPlan.foldMap(planCompiler).run.run(dataModelInPrediction).unsafeRunSync()
    printDataModel(transformedDataModelInPrediction)
    //endregion
    println("--------------------------------------------------")

    sparkSession.close()
  }
}
