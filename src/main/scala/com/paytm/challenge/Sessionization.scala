package com.paytm.challenge

/**
  * Created by koosha on 13/02/17.
  */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Sessionization {

  def main(args: Array[String])
  {

    val spark = SparkSession.builder()
      .appName("Weblog Sessionization")
      .master("local[*]")
      .getOrCreate()

    val sc =spark.sparkContext

    /* The file containing web logs, placed in the resource folder in the project */
    val pathtoLogFile = getClass.getResource("/2015_07_22_mktplace_shop_web_log_sample.log")

    /*loading the log file to a Spark RDD*/
    val weblogs = sc.textFile(pathtoLogFile.getPath())

    /* Parsing log strings to extract separate chunks of meaningful data.
       each chunk would be an element of a Row object.  i.e. loading
       structured data to a new RDD of Rows*/
    val rowRDD = weblogs.map(row => WeblogSchema.logStringToRow(row))

    /* Creating Spark DataFrame for web logs, with the desired schema - See WeblogSchema object */
    val weblogsDataFrame = spark.createDataFrame(rowRDD, WeblogSchema.logSchema)

    /* Test */
    weblogsDataFrame.printSchema()
    weblogsDataFrame.show(10,false)

    val timeOrderedWeblogs = weblogsDataFrame.orderBy(asc("timestamp"));
    timeOrderedWeblogs.persist()

    /****
         IMPORTANT:
            I've conducted this challenge based of the following assumptions:
              (1) Sessions ONLY end when the user inactivity exceeds the inactivity threshold. The
                  inactivity threshold has been set to 900 secs (15 mins) - but can bee changed in
                  settings object.
              (2) I have supposed that sessions are not ended by users and each session lasts at least as
                  long as the inactivity threshold. For instance, If a client has only one activity in the
                  log (their IP appears only once in the entire log), then the session is assumed to be 15
                  minutes (= inactivity threshold)
              (3) In summary, a session ends 15 mins after the user's last activity in the log, i.e. no sessions lives
                  less than 15 mins! ( I'm fair to sessions :) )
      ****/



    /** Adding PrevTime and NextTime columns to the log data frame.
      * "PrevTime" indicates the last timestamp the client had activity in the log, whether it's
      * in the current session or not.(null if current log is the first activity of the user)
      * "NextTime" is the timestamp for the next activity of the user in the log, whether it's in
      * the current session or not(null if current log is the last activity of the user)
      */
    val orderedLogsWithPrevNext = timeOrderedWeblogs.
      withColumn("PrevTime",lag("timestamp",1).over(Window.partitionBy("clientIP").orderBy("timestamp"))).
      withColumn("NextTime" , lead("timestamp" , 1).over(Window.partitionBy("clientIP").orderBy("timestamp"))).
      orderBy("timestamp")



    /**
      * Adding InactiveSince and InactiveUntill column to the above dataframe.
      * InactiveSince: Time (in secs) ever since the user has been inactive (-1 if this is the user's first activity in the log).
      * InactiveUntil: Time (in secs) the user will stay inactive until its next activity (-1 if this is the user's last activity in the log).
    * */
    val orderedLogsWithTimeGaps = orderedLogsWithPrevNext.select("timestamp" ,"clientIP", "PrevTime", "NextTime").
      withColumn("InactiveSince" , col("timestamp").cast("long") - col("PrevTime").cast("long")).
      withColumn("InactiveUntil" , col("NextTime").cast("long") - col("timestamp").cast("long") ).
      orderBy("timestamp").
      na.fill(-1,Seq("InactiveSince")).na.fill(-1,Seq("InactiveUntil"))


    /**
      *  Let's keep only the rows with InactiveSince of either -1 or  a value larger InactivityThreshold,
      *  and also the rows with InactiveUntil of either -1 or a value larger than InactivityThreshold.
      *  Why? Because these are the moments in which either a new session starts or a session ends (or maybe both, i.e. sessions containing
      *  only one activity in the log, with InactiveSince and InactiveUntil value of -1)!
      */
    val sessionFirstAndLastActivity = orderedLogsWithTimeGaps.
      filter(col("InactiveSince").equalTo(-1) || col("InactiveSince") > Settings.SessionInactivityThreshold ||
        col("InactiveUntil").equalTo(-1) || col("InactiveUntil") > Settings.SessionInactivityThreshold)
      .orderBy(asc("timestamp"))


    /**
      *  So far, each row belongs to a timestamp in which a session starts, or a session ends. Let's combine these two types
      *  of rows to one, to create a complete sessions list. So, our new dataframe would have client IP, Session StartTime,
      *  Session LastActivity, and SessionLength. The LastActivity column shows the user's last activity session,
      *  meaning the session EndTime happens 15 mins after (InactivityThreshold).
      *  So: SessionLength = LastActivity - StartTime + InactivityThreshold
      */
    val allSessionsList = sessionFirstAndLastActivity.
      filter(col("InactiveSince")>Settings.SessionInactivityThreshold || col("InactiveSince").equalTo(-1)).
      select( "clientIP" ,"timestamp" ).
      withColumn( "LastActivity" ,
        lead("timestamp" , 1).over(Window.partitionBy("clientIP").orderBy("timestamp"))).
      withColumn("ActivityLength" , col("LastActivity").cast("long") - col("timestamp").cast("long")).
      withColumn("SessionLength" , col("ActivityLength").
        cast("long") + Settings.SessionInactivityThreshold).na.fill(Settings.SessionInactivityThreshold, Seq("SessionLength")).
      withColumnRenamed("timestamp","StartTime")

    /**  Giving a unique sessionID to each session **/
    val sessions = allSessionsList.orderBy("StartTime").
      withColumn("SessionID", monotonically_increasing_id()).withColumnRenamed("clientIP" , "client")

    sessions.persist()

    /*Test*/
    sessions.show(50,false)

    /**
      * Sessionizing the original web log => Assign sessionID to each row of the weblogs.
      * I did this by joining the sessions dataframe to the original weblogs data frame using
      * the right condition on timestamps.
      */
    val sessionizedWebLogs = timeOrderedWeblogs.join(sessions , timeOrderedWeblogs("clientIP") === sessions("client") &&
      (timeOrderedWeblogs("timestamp") === sessions("StartTime") ||
        (timeOrderedWeblogs("timestamp") > sessions("StartTime") &&
          timeOrderedWeblogs("timestamp").cast("long") - sessions("StartTime").cast("long") <=
            sessions("ActivityLength").cast("long")))).
      select("SessionID", "timestamp" , "clientIP" ,"URL" ,"SessionLength")

    /** Test **/
    sessionizedWebLogs.filter(col("SessionID").equalTo(2)).show(30,false)


   /*** Average Session Time ***/
    println("Average session time for ALL sessions:")
    sessions.select(avg("SessionLength").alias("Avg_Session_Time")).show()


    /*** Most engaged users ***/
    val mostEngagedUsers = sessions.groupBy("client").
      agg( sum("SessionLength").alias("Total_SessionLength_Secs") ,
        avg("SessionLength").alias("Average_SessionLength_Secs")).
      orderBy(desc("Total_SessionLength_Secs"))

    println("Top Engaged Users:")
    mostEngagedUsers.show(100, false)


    /**
      * Unique url visit per session.
      * I'm not sure if I've got the question exactly right. My own understanding is
      * to return urls which are visited exactly once in a given session.
      * */
    sessionizedWebLogs.groupBy("SessionID" , "URL").count().alias("count").filter(col("count").equalTo(1)).show(false)

  }


}
