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
      print("str.length() : " + str.length() + "\n")
      print(str.substring(0,2) + "\n")
      print(str.substring(2,str.length()) + "\n")
    } catch {
      case e: Exception => {
        println("substring err" + e)
      }
    }
  }
}
