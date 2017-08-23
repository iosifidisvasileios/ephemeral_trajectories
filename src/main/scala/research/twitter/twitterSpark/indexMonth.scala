package research.twitter.twitterSpark

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.{DateTime, Period}

/*Method to split the dataset per month*/
object indexMonth {
  

  def dateRange(from: DateTime, to: DateTime, step: Period): Iterator[DateTime]      =Iterator.iterate(from)(_.plus(step)).takeWhile(!_.isAfter(to))
  val from = new DateTime().withYear(2009).withMonthOfYear(4).withDayOfMonth(1).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0)
  val to = new DateTime().withYear(2009).withMonthOfYear(7).withDayOfMonth(1).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0)



  def main(args: Array[String]) {
    val sdf: SimpleDateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy") //Cluster                                
    val sdf_2: SimpleDateFormat = new SimpleDateFormat("yyyy-MM")
    def conf = new SparkConf().setAppName(indexMonth.getClass.getName)    
    val sc = new SparkContext(conf)
    println("Hello, world!")
    sc.setLogLevel("ERROR")
    val unlabeledInput = sc.textFile(args(0))
    dateRange(from, to, new Period().withMonths(1)).toList.foreach { step =>

        val unlabeledMapped = unlabeledInput.filter{ line =>
        
          val parts = line.split("\",\"")
          val stringDate = parts(2).replaceAll("[\"]", "")
          val temp: Date = sdf.parse(stringDate)
          try {
  
            val date: DateTime = new DateTime(temp)
            step.getMonthOfYear.equals(date.getMonthOfYear) && step.getYear.equals(date.getYear)
  
          } catch {
            case e: java.lang.NumberFormatException => println(line, e)
              false
          }
      }

      if (!unlabeledMapped.isEmpty()){
        unlabeledMapped.saveAsTextFile(args(1) + sdf_2.format(step.toDate))
      }
    }


  }
  
}