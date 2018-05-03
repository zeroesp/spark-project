import org.apache.spark.{SparkContext, SparkConf}

object SimpleReadFile {
  def main(args: Array[String]) : Unit = {
    //windows hadoop conf
    System.setProperty("hadoop.home.dir", "C:\\winutils")

    //Conf, Context
    val conf = new SparkConf().setAppName("SimpleReadFile").setMaster(args(0))
    val sc = new SparkContext(conf)

    //logic -- start
    val filePath = "./pom.xml"
    val inputRDD = sc.textFile(filePath)
    val matchTerm = "spark"
    val numMatches = inputRDD.filter(_.contains(matchTerm)).count()
    println("%s lines in %s contains %s".format(numMatches, filePath, matchTerm))
    //logic -- end

    System.exit(0)
  }
}
