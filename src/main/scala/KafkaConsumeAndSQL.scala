import org.apache.spark.{SparkConf, SparkContext}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, LogManager}

//table schema
case class Apt(year_month : String, reg_cd : String, reg_nm : String, dong_nm : String, apart : String, space : Float, price : Float, built_year : String, year : String, month : String, day : String, land_num : String, reg_cd2 : String, floor : String)

object KafkaConsumeAndSQL {
  def main(args: Array[String]): Unit ={
    //Conf, Context (bind exception > bindAddress set)
    val sparkConf = new SparkConf().setAppName("KafkacConsumer").setMaster(args(0)).set("spark.driver.bindAddress", "127.0.0.1")
    val streamingContext = new StreamingContext(sparkConf, Seconds(10))

    //sql context
    val sqlContext = SparkSession.builder().appName("sqltest").getOrCreate()
    //DF transform
    import sqlContext.implicits._

    //log level modify
    LogManager.getRootLogger.setLevel(Level.WARN)

    //logic start
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092", //"localhost:9092,anotherhost:9092"
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "consumer_group", //use_a_separate_group_id_for_each_stream
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("apart") //Array("topicA", "topicB")
    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    kafkaStream.foreachRDD(rdd => {
      println("* RDD size = " + rdd.count())

      rdd.map(s => s.value().split("\\|"))
        .map(s => Apt(s(0), s(1), s(2), s(3), s(4), s(5).toFloat, s(6).replace(",","").toFloat, s(7), s(8), s(9), s(10), s(11), s(12), s(13)))
        .toDF().createOrReplaceTempView("apt_table")

      println("== space 10/20/30/40 ")
      sqlContext.sql(
        "select year_month, reg_cd, reg_nm, " +
        " case when space<66.12 then 10" +
        " when space>=66.12 and space<99.17 then 20" +
        " when space>=99.17 and space<132.23 then 30" +
        " when space>=132.23 then 40 end as area" +
        " ,count(*) as count, round(avg(price),2) as avg_price, min(price) as min_price, max(price) as max_price " +
        "from apt_table " +
        "group by year_month, reg_cd, reg_nm, " +
        "case when space<66.12 then 10" +
        " when space>=66.12 and space<99.17 then 20" +
        " when space>=99.17 and space<132.23 then 30" +
        " when space>=132.23 then 40 end " +
        "order by 1,2,4"
      ).collect().foreach(println)

      //println("== apt_table")
      //sqlContext.sql("select * from apt_table").collect().foreach(println)
      //println("== space 80~100")
      //sqlContext.sql("select year_month, reg_nm, space, price from apt_table where space >= 80 and space < 100").collect().foreach(println)
    })
    //logic end

    streamingContext.start()
    streamingContext.awaitTermination()

    //streamingContext.stop()
  }
}
