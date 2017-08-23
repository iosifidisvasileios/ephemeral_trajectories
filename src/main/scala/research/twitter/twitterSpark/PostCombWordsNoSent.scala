package research.twitter.twitterSpark

import edu.stanford.nlp.ling._
import scala.io.Source
import java.io.BufferedWriter
import java.io.FileWriter
import com.opencsv.CSVWriter
//import au.com.bytecode.opencsv.CSVWriter

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.util._

import org.apache.log4j.Logger
import org.apache.log4j.Level

//object that combines the results that come from the new words 
object PostCombWordsNoSent{
  //Combining the results of the new words (word_POS, Sentiment) "whatever_WDT::4"
  def groupWordsNoSentiment(sourceNewWords:RDD[String], sc:SparkContext) : RDD[(String, (String, String, Long))] = {  
     
   //(whatever, (WDT, 4, 1) )
   val newWordsRDD = sourceNewWords.map(line => ( line.replace("\"", "").split("::")(0), line.replace("\"", "").split("::")(1) ) )
                                   .map(line => (line._1.split("_")(0),((line._1.split("_")(1)),line._2,1L)))
                                   .reduceByKey((x,y) => (x._1+"/"+y._1,x._2+y._2,x._3+y._3 ))
                                   .map(line => (line._1, (posCombiner(line._2._1), getPosNegString(line._2._2), line._2._3 ))).sortBy(_._2._3,false) 
   return newWordsRDD
  }
  
  //method that combines the results of the new words (word, Sentiment) "whatever_<POS/POS/POS>::4"
  def groupWordsNoSentimentNoPOS(sourceNewWords:RDD[String], sc:SparkContext) : RDD[(String, (String, String, Long))] = {
   
   val newWordsRDD = sourceNewWords.map(line => ( line.replace("\"", "").split("::")(0), line.replace("\"", "").split("::")(1) ) )
                                   .map(line => (line._1.split("_")(0),(line._1.split("_")(1).replaceAll("[<>]", ""), line._2, 1L) ) )  
                                   .reduceByKey((x,y) => (x._1+"/"+y._1,x._2+y._2,x._3+y._3))
                                   .map(line => (line._1, ( posCombiner(line._2._1), getPosNegString(line._2._2), line._2._3 ))).sortBy(_._2._3,false)
   
   return newWordsRDD
  }
  
  
  def posCombiner(pos:String): String = 
  {
    var posOut =""
    var posArray = pos.split("/")
    val groupPos =  posArray.toList groupBy (word => word) mapValues(_.size)
    val ordPos = groupPos.toSeq.sortWith(_._2 > _._2)
    val (posMax, valueMax) = ordPos.head
    return "<"+posMax+">"
  }
   
  def getPosNegString (numberStr:String) : String =
  {      
    var listChars = numberStr.toList
    var countPos = 0
    var countNeg = 0
    var countNeut = 0
    var countNoS = 0
    for(a <- listChars)
    {      
      if(a == '0')
      {
        countNeg += 1
      }else if(a == '4')
      {
        countPos += 1
      }else if(a == '2')
      {
        countNeut += 1
      }else if (a == '5')
      {
        countNoS += 1
      }
      
    }
    return "<Pos:"+countPos+" Neg:"+ countNeg+" Neut:"+ countNeut+" NoSe:"+ countNoS+">"
  }
}
