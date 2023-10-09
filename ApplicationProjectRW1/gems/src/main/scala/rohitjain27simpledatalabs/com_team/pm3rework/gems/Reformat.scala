package rohitjain27simpledatalabs.com_team.pm3rework.gems

import io.prophecy.gems._
import io.prophecy.gems.dataTypes._
import io.prophecy.gems.uiSpec._
import io.prophecy.gems.diagnostics._
import io.prophecy.gems.componentSpec._
import org.apache.spark.sql.{DataFrame, SparkSession}
import io.prophecy.gems.copilot._
import play.api.libs.json.{Format, OFormat, JsResult, JsValue, Json}


class Reformat extends ComponentSpec {

  val name: String = "Reformat"
  val category: String = "Transform2"
  type PropertiesType = ReformatProperties
  override def optimizeCode: Boolean = true

  case class ReformatProperties(
    @Property("Property1")
    property1: String = ""
  ) extends ComponentProperties

  implicit val ReformatPropertiesFormat: Format[ReformatProperties] = Json.format

  def dialog: Dialog = Dialog("Reformat")

  def validate(component: Component)(implicit context: WorkflowContext): List[Diagnostic] = Nil

  def onChange(oldState: Component, newState: Component)(implicit context: WorkflowContext): Component = newState

  def deserializeProperty(props: String): ReformatProperties = Json.parse(props).as[ReformatProperties]

  def serializeProperty(props: ReformatProperties): String = Json.toJson(props).toString()

  class ReformatCode(props: PropertiesType) extends ComponentCode {
    def apply(spark: SparkSession, in: DataFrame): DataFrame = {
      val out = in
      out
    }
  }
}
