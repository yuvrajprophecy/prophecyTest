package rohitjainprophecyioteam.scalaformatmigrationexamples.gems


import io.prophecy.gems.migration.PropertyMigrationObj
import rohitjainprophecyioteam.scalaformatmigrationexamples.gems.CsvCustom.CsvProperties

object AddNewProperty extends PropertyMigrationObj[CsvProperties]{
  def migrationNumber: Int = 1

  override def up(oldProperties: CsvProperties): CsvProperties =
    oldProperties.copy(newProperty = Some("newProperty"))

  override def down(newProperties: CsvProperties): CsvProperties =
    newProperties.copy(newProperty = None)
}