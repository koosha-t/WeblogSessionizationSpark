package com.paytm.challenge

/**
  * Created by koosha on 15/02/17.
  */

import  java.sql.Timestamp

/** From Spark In Action excellent book:
  * The implicit StringImprovements class in the StringImplicits object defines four methods that
  * can be implicitly added to Scala's String class and used to safely convert strings to integers, longs, timestamps and doubles.
  */
object StringImplicits {
  implicit class StringImprovements(val s: String){
    import scala.util.control.Exception.catching
    def toIntSafe = catching(classOf[NumberFormatException]) opt s.toInt
    def toLongSafe = catching(classOf[NumberFormatException]) opt s.toLong
    def toDoubleSafe = catching(classOf[NumberFormatException]) opt s.toDouble
    def toTimestampSafe = catching(classOf[IllegalArgumentException]) opt Timestamp.valueOf(s)
  }
}
