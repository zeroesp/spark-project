package test

import scala.collection.mutable.HashMap
import scala.collection.mutable.{HashMap => MutableHashMap}

object ScalaTest2 {
  def main(args: Array[String]) : Unit = {
    var test = " te st ";
    println(test)
    println(test.trim)

    var keyValueMap: HashMap[String, String] = HashMap()
    keyValueMap.put("Csv ","test")
    val v = keyValueMap("Csv ")
    println(v)

    val map: MutableHashMap[Int, String] = MutableHashMap()
    map += (1 -> "test")
    map.put(2,"aa")

    println(map)
    println(map.get(1))

  }
}
