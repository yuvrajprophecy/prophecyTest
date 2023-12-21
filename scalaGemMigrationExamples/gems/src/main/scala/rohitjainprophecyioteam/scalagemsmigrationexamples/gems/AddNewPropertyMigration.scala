package rohitjainprophecyioteam.scalagemsmigrationexamples.gems


import io.prophecy.gems.migration.PropertyMigrationObj
import rohitjainprophecyioteam.scalagemsmigrationexamples.gems.Reformat.ReformatProperties

object AddNewProperty extends PropertyMigrationObj[ReformatProperties]{
  def migrationNumber: Int = 1

  override def up(oldProperties: ReformatProperties): ReformatProperties =
    oldProperties.copy(newProperty = Some("newProperty"))

  override def down(newProperties: ReformatProperties): ReformatProperties =
    newProperties.copy(newProperty = None)
}
