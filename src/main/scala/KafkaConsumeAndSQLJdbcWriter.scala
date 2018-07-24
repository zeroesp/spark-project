import java.sql.{Connection, DriverManager, PreparedStatement, SQLException}
import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.types._

object KafkaConsumeAndSQLJdbcWriter {
  def main(args: Array[String]): Unit ={
    //Conf, Context (bind exception > bindAddress set)
    val sparkConf = new SparkConf().setAppName("KafkacConsumer").setMaster(args(0)).set("spark.driver.bindAddress", "127.0.0.1")
    val streamingContext = new StreamingContext(sparkConf, Seconds(5))

    //sql context
    val sqlContext = SparkSession.builder().appName("sqltest").getOrCreate()
    //DF transform
    import sqlContext.implicits._

    //log level modify
    LogManager.getRootLogger.setLevel(Level.WARN)

    //logic start
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "127.0.0.1:9092", //"localhost:9092,anotherhost:9092"
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "consumer_group", //use_a_separate_group_id_for_each_stream
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("json_test") //Array("topicA", "topicB")
    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    //jdbc 작업
    val jdbcProps = new Properties
    jdbcProps.setProperty("driver", "com.mysql.jdbc.Driver")
    jdbcProps.setProperty("user", "pipeline")
    jdbcProps.setProperty("password", "pipeline")

    val tableFullName = "test_table234"
    val tableNameInfo = tableFullName.split('.')
    val tableSchema = if (tableNameInfo.length > 1) {
      tableNameInfo(0)
    } else {
      null
    }
    val tableName = if (tableNameInfo.length > 1) {
      tableNameInfo(1)
    } else {
      tableFullName
    }

    var targetSchema = new StructType()

    //테이블 생성 여부
    var CreateTableYn = false

    try {
      val originDf = sqlContext.read.jdbc("jdbc:mysql://169.56.124.28:3306/test", tableFullName, jdbcProps)
      println("[JDBC Writer] table <" + tableFullName + "> is exists.")
      originDf.printSchema
      targetSchema = originDf.schema
    } catch {
      case e: Exception => {
        if (e.getMessage.contains(tableFullName) && e.getMessage.contains("exist")) {
          println("[JDBC Writer] table <" + tableFullName + "> will be created.(if supported)")
          //테이블 생성 여부
          CreateTableYn = true
        } else {
          println("[JDBC Writer ERROR] caused by: " + e.getMessage)
        }
      }
    }

    if(targetSchema.isEmpty){
      println("targetSchema is null")
    }else{
      println("targetSchema is not null")
    }

    var column: Array[StructField] = Array()
    column = column :+ StructField("col_0" , StringType, true)
    column = column :+ StructField("col_1" , IntegerType, true)
    column = column :+ StructField("col_2" , LongType, true)
    column = column :+ StructField("col_3" , FloatType, true)
    column = column :+ StructField("col_4" , DoubleType, true)
    var schema = StructType(column)
    schema.printTreeString()
    for(name <- schema.fieldNames){
      println(name)
    }

    // create 문 만들기 - 시작
    var result = "CREATE TABLE " + tableName + " ("
    for((field, idx) <- schema.fields.zipWithIndex){
      println(field.dataType.typeName)
      if("string".equals(field.dataType.typeName) ) {
        result += field.name + " VARCHAR(1000)"
      } else if("integer".equals(field.dataType.typeName)) {
        result += field.name + " INT"
      } else if("long".equals(field.dataType.typeName)) {
        result += field.name + " BIGINT"
      } else if("float".equals(field.dataType.typeName)) {
        result += field.name + " FLOAT"
      } else if("double".equals(field.dataType.typeName)) {
        result += field.name + " DOUBLE"
      }
      if (idx < schema.fields.length - 1) {
        result += ", "
      }
    }
    result += ")"
    println(result)
    // create 문 만들기 - 끝

    //테이블 생성 여부 확인 후 작업 - 시작
    if(CreateTableYn){
      println("Create table process")
      var connection: Connection = null
      var statementCreate: PreparedStatement = null

      try {
        Class.forName("com.mysql.jdbc.Driver")
        connection = DriverManager.getConnection("jdbc:mysql://169.56.124.28:3306/test", "pipeline", "pipeline")
        statementCreate = connection.prepareStatement(result)
        statementCreate.execute()
      }
      catch {
        case e: Exception => {
          println("[JDBC Writer ERROR] create table fail!")
          e.printStackTrace
        }
      } finally {
        if (statementCreate != null) {
          try {
            statementCreate.close
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
    //테이블 생성 여부 확인 후 작업 - 끝


    kafkaStream.foreachRDD(rdd => {
      println("* RDD size = " + rdd.count())



    })
    //logic end

    streamingContext.start()
    streamingContext.awaitTermination()

    //streamingContext.stop()
  }
}
