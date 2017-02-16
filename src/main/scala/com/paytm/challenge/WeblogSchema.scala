package com.paytm.challenge


import org.apache.spark.sql.{DataFrame, Row, SparkSession, types}
import org.apache.spark.sql.types._


/**
  * Created by koosha on 15/02/17.
  * This object helps with creating the dataframe over our weblogs, with the right schema!
  */
object WeblogSchema
{
  val logSchema = StructType(Seq(
    StructField("timestamp" , TimestampType , true),
    StructField("elb" , StringType , true),
    StructField("clientIP" , StringType , true),
    StructField("clientPort" , StringType , true),
    StructField("backendPort" , StringType, true),
    StructField("request_processing_time" , DoubleType, true),
    StructField("backend_processing_time" , DoubleType, true),
    StructField("response_processing_time", DoubleType, true),
    StructField("elb_status_code", StringType, true),
    StructField("backend_status_code", StringType, true),
    StructField("received_bytes", LongType, true),
    StructField("sent_bytes", LongType, true),
    StructField("requestType", StringType, true),
    StructField("URL", StringType, true),
    StructField("protocol", StringType, true)
    //StructField("user_agent", StringType, true),
    //StructField("ssl_cipher", StringType, true),
    //StructField("ssl_protocol" , StringType, true)

  ))

  /** Parsing the rdd of log string to row of data chunks we need **/
  import StringImplicits._
  def logStringToRow(row:String):Row ={
    val r = row.split(" ")
    Row(r(0).replace('T',' ').init.toTimestampSafe.getOrElse(null),
      r(1),
      r(2).split(":")(0),
      r(2).split(":")(1),
      r(3),
      r(4).toDoubleSafe.getOrElse(null),
      r(5).toDoubleSafe.getOrElse(null),
      r(6).toDoubleSafe.getOrElse(null),
      r(7), r(8),
      r(9).toLongSafe.getOrElse(null),
      r(10).toLongSafe.getOrElse(null),
      r(11).tail,
      r(12),
      r(13).init
    )
  }
}
