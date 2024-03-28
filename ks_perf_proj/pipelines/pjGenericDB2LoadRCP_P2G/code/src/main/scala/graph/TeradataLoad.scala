package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object TeradataLoad {

  def apply(context: Context, in: DataFrame): Unit = {
    val Config = context.config
    val jdbc_username: String = s"${Config.pUKDWUSERNAME}"
    val jdbc_password: String = s"${Config.pUKDWPASSWORD}"
    val jdbc_url:      String = s"${Config.pUKDWSCHEMA}"
    var writer = in.write.format("jdbc")
    writer = writer
      .option("url",      jdbc_url)
      .option("dbtable",  s"W01_${Config.pIDENT}_${Config.pTABLE_NAME}")
      .option("user",     jdbc_username)
      .option("password", jdbc_password)
      .option("driver",   "com.tera.teraDriver")
    writer = writer.mode("overwrite")
    writer.save()
    locally {
      val sql =
        s"INSERT INTO ${Config.pTABLE_NAME} SELECT * FROM W01_${Config.pIDENT}_${Config.pTABLE_NAME};"
      locally {
        import java.sql.{Connection, DriverManager}
        var connection: Connection = null
        try {
          connection =
            DriverManager.getConnection(jdbc_url, jdbc_username, jdbc_password)
          val statement = connection.prepareStatement(sql)
          try statement.executeUpdate()
          finally statement.close()
        } finally if (connection != null) connection.close()
      }
    }
  }

}
