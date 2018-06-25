package test

import scala.collection.mutable.HashMap

object ScalaTest2 {
  def main(args: Array[String]) : Unit = {
    var test = " te st ";
    println(test)
    println(test.trim)

    var keyValueMap: HashMap[String, String] = HashMap()
    keyValueMap.put("Csv ","test")
    val v = keyValueMap("csv")
    println(v)
  }
}
