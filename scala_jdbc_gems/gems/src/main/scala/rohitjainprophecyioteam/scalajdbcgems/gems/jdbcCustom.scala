package rohitjainprophecyioteam.scalajdbcgems.gems

import ai.x.play.json.Encoders.encoder
import ai.x.play.json.Jsonx
import io.prophecy.gems._
import io.prophecy.gems.dataTypes._
import io.prophecy.gems.uiSpec._
import io.prophecy.gems.diagnostics._
import io.prophecy.gems.datasetSpec._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import io.prophecy.gems.copilot._
import play.api.libs.json.{Format, OFormat, JsResult, JsValue, Json}


class JdbcCustom extends DatasetSpec {

  val name: String = "JdbcCustom"
  val datasetType: String = "File"

  type PropertiesType = JdbcCustomProperties
  case class JdbcCustomProperties(
    @Property("Schema")
    schema: Option[StructType] = None,
    @Property("Path")
    path: String = ""
    ) extends DatasetProperties

  implicit val JdbcCustomPropertiesFormat: Format[JdbcCustomProperties] = Json.format


  def sourceDialog: DatasetDialog = DatasetDialog("JdbcCustom")
    .addSection("LOCATION", TargetLocation("path"))
    .addSection(
      "PROPERTIES",
      ColumnsLayout(gap = Some("1rem"), height = Some("100%"))
        .addColumn(
          ScrollBox()
            .addElement(
              StackLayout()
                .addElement(
                  StackItem(grow = Some(1)).addElement(
                    FieldPicker(height = Some("100%"))
                  )
                )
            ),
          "auto"
        )
        .addColumn(SchemaTable("").bindProperty("schema"), "5fr")
    )
    .addSection(
      "PREVIEW",
      PreviewTable("").bindProperty("schema")
    )

  def targetDialog: DatasetDialog = DatasetDialog("JdbcCustom")
    .addSection("LOCATION", TargetLocation("path"))
    .addSection(
      "PROPERTIES",
      ColumnsLayout(gap = Some("1rem"), height = Some("100%"))
        .addColumn(
          ScrollBox().addElement(
            StackLayout(height = Some("100%")).addElement(
              StackItem(grow = Some(1)).addElement(
                FieldPicker(height = Some("100%"))
              )
            )
          ),
          "auto"
        )
        .addColumn(SchemaTable("").isReadOnly().withoutInferSchema().bindProperty("schema"), "5fr")
    )

  override def validate(component: Component)(implicit context: WorkflowContext): List[Diagnostic] = Nil

  def onChange(oldState: Component, newState: Component)(implicit context: WorkflowContext): Component = newState

  class JdbcCustomFormatCode(props: JdbcCustomProperties) extends ComponentCode {
    def sourceApply(spark: SparkSession): DataFrame = {
      var reader = spark.read.format("JdbcCustom")
      reader.load(props.path)
    }

    def targetApply(spark: SparkSession, in: DataFrame): Unit = {
      var writer = in.write.format("JdbcCustom")
      writer.save(props.path)
    }
  }
  def deserializeProperty(props: String): JdbcCustomProperties = Json.parse(props).as[JdbcCustomProperties]

  def serializeProperty(props: JdbcCustomProperties): String = Json.toJson(props).toString()

}
