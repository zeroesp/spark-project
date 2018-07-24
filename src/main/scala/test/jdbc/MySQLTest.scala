package test.jdbc

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, SQLException}

object MySQLTest {
  def main(args: Array[String]): Unit ={
    var connection: Connection = null
    var statementSelect: PreparedStatement = null
    var resultSet: ResultSet = null;

    try {
      Class.forName("com.mysql.jdbc.Driver")
      connection = DriverManager.getConnection("jdbc:mysql://169.56.124.28:3306/test", "pipeline", "pipeline")
      statementSelect = connection.prepareStatement("select * from json_table limit 10")
      resultSet = statementSelect.executeQuery()

      while(resultSet.next()){
        println(resultSet.getString(1) + ", " + resultSet.getString(2) + ", " + resultSet.getString(3))
      }
    }
    catch {
      case e: Exception => {
        println("[JDBC Writer ERROR] query fail!")
        e.printStackTrace
      }
    } finally {
      if (resultSet != null) {
        try {
          resultSet.close
        } catch {
          case e: SQLException => {}
        }
      }
      if (statementSelect != null) {
        try {
          statementSelect.close
        } catch {
          case e: SQLException => {}
        }
      }
      if (connection != null) {
        try {
          connection.close
        } catch {
          case e: SQLException => {}
        }
      }
    }
  }
}
