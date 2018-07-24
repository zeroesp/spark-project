package test.jdbc

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, SQLException}

object PostresqlTest {
  def main(args: Array[String]): Unit ={
    var connection: Connection = null
    var statementSelect: PreparedStatement = null
    var resultSet: ResultSet = null;

    try {
      Class.forName("org.postgresql.Driver")
      connection = DriverManager.getConnection("jdbc:postgresql://169.56.124.28:5432/phjdb", "phj", "phj")

      //select
      /*
      statementSelect = connection.prepareStatement("SELECT * FROM testtable")
      resultSet = statementSelect.executeQuery()

      while(resultSet.next()){
        println(resultSet.getString(1) + ", " + resultSet.getString(2))
      }
      */
      statementSelect = connection.prepareStatement("create table test_write3" +
        "(col_1 VARCHAR(1000)," +
        "col_2 INT," +
        "col_3 BIGINT," +
        "col_4 FLOAT4," +
        "col_5 FLOAT8," +
        "col_6 character varying(100)," +
        "col_7 character varying(100)" +
        ")")
      statementSelect.execute()
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
