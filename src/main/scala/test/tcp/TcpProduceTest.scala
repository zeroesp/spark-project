package test.tcp

import java.net.Socket

import org.apache.commons.lang3.time.FastDateFormat

object TcpProduceTest {
  def main(args: Array[String]) : Unit = {
    val socket = new Socket("localhost", 17888)
    val out = socket.getOutputStream
    var i = 0
    while (true) {
      val date = FastDateFormat.getInstance("yyyy/MM/dd HH:mm:ss.SSS").format(System.currentTimeMillis)
      val msg = date + ",aa,bb,cc,test" + i + "\n"
      out.write(msg.getBytes)
      Thread.sleep(1000)
      i += 1
    }
  }
}
