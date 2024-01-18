package rohitjainprophecyioteam.scalajdbcgems.gems

import io.prophecy.gems.dataTypes.{ConfigSecret, SecretValue, TextSecret, VaultSecret}
import io.prophecy.gems.migration.PropertyMigrationObj
import rohitjainprophecyioteam.scalajdbcgems.gems.JdbcCustom.JDBCCustomProperties

object SecretPropertyMigration extends PropertyMigrationObj[JDBCCustomProperties] {
  def migrationNumber: Int = 1

  override def up(oldProperties: JDBCCustomProperties): JDBCCustomProperties = {
    oldProperties.credType match {
      case "databricksSecrets" =>
        oldProperties.copy(
          secretUsername =
            Some(SecretValue(List(VaultSecret("Databricks", Some(""), Some("0"), Some(oldProperties.credentialScope.getOrElse("")), "username")))),
          secretPassword = Some(SecretValue(List(VaultSecret("Databricks", Some(""), Some("0"), Some(oldProperties.credentialScope.getOrElse("")), "password")))),
          credentialScope = None,
          textUsername = None,
          textPassword = None,
          credType = ""
        )

      case "userPwd" =>
        oldProperties.copy(
          secretUsername =
            Some(SecretValue(List(TextSecret(oldProperties.textUsername.getOrElse(""))))),
          secretPassword = Some(SecretValue(List(TextSecret(oldProperties.textPassword.getOrElse(""))))),
          credentialScope = None,
          textUsername = None,
          textPassword = None,
          credType = ""
        )

      case "userPwdEnv" =>
        oldProperties.copy(
          secretUsername =
            Some(SecretValue(List(TextSecret(oldProperties.textUsername.getOrElse(""))))),
          secretPassword = Some(SecretValue(List(TextSecret(oldProperties.textPassword.getOrElse(""))))),
          credentialScope = None,
          textUsername = None,
          textPassword = None,
          credType = ""
        )
      case default =>
        oldProperties
    }
  }


  override def down(newProperties: JDBCCustomProperties): JDBCCustomProperties = {
    val newPropertiesWithMigratedUsername = newProperties.secretUsername match {
      case Some(secretValue) =>
        secretValue.parts.headOption match {
          case Some(secretPart: VaultSecret) =>
            newProperties.copy(credentialScope = secretPart.secretScope, secretUsername = None, credType = "databricksSecrets")
          case Some(secretPart: TextSecret) =>
            newProperties.copy(textUsername = Some(secretPart.value), secretUsername = None, credType = "userPwd")
          case Some(secretPart: ConfigSecret) =>
            newProperties.copy(textUsername = Some(secretPart.value.mkString("")), secretUsername = None, credType = "userPwd")
          case None =>
            newProperties.copy(textUsername = Some(""))
        }
      case None => newProperties.copy(textUsername = Some(""))

    }
    newPropertiesWithMigratedUsername.secretPassword match {
      case Some(secretValue) =>
        secretValue.parts.headOption match {
          case Some(secretPart: VaultSecret) =>
            newProperties.copy(credentialScope = secretPart.secretScope, secretPassword = None, credType = "databricksSecrets")
          case Some(secretPart: TextSecret) =>
            newProperties.copy(textPassword = Some(secretPart.value), secretPassword = None, credType = "userPwd")
          case Some(secretPart: ConfigSecret) =>
            newProperties.copy(textPassword = Some(secretPart.value.mkString("")), secretPassword = None, credType = "userPwd")
          case None =>
            newProperties.copy(textPassword = Some(""))
        }
      case None =>
        newProperties.copy(textPassword = Some(""))
    }
  }
}