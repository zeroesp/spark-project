package test

import org.apache.commons.lang3.time.FastDateFormat

object ScalaTest {
  def main(args: Array[String]) : Unit = {
    //타임스템프 테스트
    var currentTime = System.currentTimeMillis
    println("currentTime : " + currentTime)

    var dateFormat = FastDateFormat.getInstance("YYYY-MM-dd")
    println("dateFormat : " + dateFormat)

    var formattedTime = dateFormat.format(currentTime)
    println("formattedTime : " + formattedTime)
    println(FastDateFormat.getInstance("yyyy/MM/dd HH:mm:ss.SSS").format(System.currentTimeMillis))

    //substring
    var str = "test"
    try {
      println("str.length() : " + str.length())
      println(str.substring(0,2))
      println(str.substring(2,str.length()))
    } catch {
      case e: Exception => {
        println("substring err" + e)
      }
    }

    val tableFullName = "jdbc:mysql://10.178.50.88:3306/test"
    val tableNameInfo = tableFullName.split('/')
    val tableSchema = if (tableNameInfo.length > 1) {
      tableNameInfo(tableNameInfo.length-1)
    } else {
      null
    }

    println(tableFullName)
    println(tableNameInfo(0))
    println(tableSchema)


  }
}
