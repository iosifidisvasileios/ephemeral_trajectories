package research.twitter.twitterSpark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark
import org.apache.spark.util._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.Map
import scala.collection.mutable.HashMap

import java.io.BufferedInputStream
import java.io.File
import java.io.FileInputStream
import java.io.InputStream
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

import org.joda.time._
import org.joda.time.format._
import org.joda.time.DateTimeZone

//common function used over the whole implementation
object Utils {
  //getting the general part of a speech 
  def getGeneralPOS(specificPOS:String) :String =
  {
    var generalPOS:String = ""        
    specificPOS match
    {
      case "JJ" | "JJR" | "JJS" => generalPOS = "a";
      case "NN" | "NNS" | "NNP" | "NNPS" => generalPOS = "n"
      case "RB" | "RBR" | "RBS"  => generalPOS = "r"
      case "VB" | "VBD" | "VBG" | "VBN" | "VBP" | "VBZ" => generalPOS = "v"
      case _ => generalPOS = "other" //other speech part not in the scope of "a", "n", "r", "v"          
    }      
    return generalPOS      
  }
  
  //checking whether it is a superlative or comparative 
  def checkSuperlativeComparative(posSpecific:String) :Boolean = 
  {    
    var flag = false
    posSpecific match {
      case "JJR"|"JJS"|"RBR"|"RBS" => flag = true      
      case _ => flag = false
    }    
    return flag
  }
  
  //checking whether it is a negative word 
  def isNegativeWord(word:String, separator:String):Boolean = 
  {
    var isNegative:Boolean = false
    //not_RB
    if(word.startsWith("not_") && ((word.split(separator).length) == 3) )
    {
      isNegative = true
    }
    return isNegative
  }
  //split a word given a char separator 
  def splitWord(word:String, separator:String)=
  {
    
    val word_ = word.split(separator)(0)
    val pos_ = word.split(separator)(1)
    (word_,pos_)
  }
  //split a negated word given a char separator
  def splitWordNegative(word:String, separator:String)=
  {
    //input not_play_VB
    val word_ = word.split(separator)(1)
    val pos_ = word.split(separator)(2)
    (word_,pos_)
  }
  //check if it is a verb 
  def isVerb(pos:String) :Boolean =
  {
    var isVerb_ = false
    val verbTags = Array("VB","VBD", "VBG","VBN","VBP","VBZ")
    if(verbTags.contains(pos)) isVerb_ = true
    return isVerb_
      
  }
  //check if it is an adjective 
  def isAdjective(pos:String) :Boolean= 
  {
    var isAdjective_ = false
    val adjTags = Array("JJ","JJR","JJS")
    if(adjTags.contains(pos)) isAdjective_ = true
    return isAdjective_    
  } 
  
  //check if it is a verb 
  def checkPOSVerb(wordWithPOS:String) : Boolean =
  {    
    var flag:Boolean = false
    var regExpr = """(\x5F{1}V[A-Z]{1,2})""".r
    val result = regExpr.findFirstIn(wordWithPOS)
    
    if(result != None && !wordWithPOS.startsWith("not_"))
    {        
      flag = true
    }
    return flag
  }
    
  //check if it is a noun 
  def checkPOSNoun(word:String) : Boolean = 
  {
    var flag:Boolean = false
    val regExpr = """(\x5F{1}N[A-Z]{1,3})""".r
    val result = regExpr.findFirstIn(word)
    if(result != None && !word.startsWith("not_"))
    {
      flag = true
    }
    return flag
  }
  
  //method that capitalizes the first letter of nouns  
  def capitalizeNounsDeletePos(tweet:String) :String =
  {     
    val posToCapitalize = Array( "NN", "NNS", "NNP", "NNPS" )
    var capitalizedTweet = ""    
    for (word <- tweet.split(" "))
    {
      var (word_,pos) = Utils.splitWord(word, "_")        
      if(posToCapitalize.contains(pos))
      {
        capitalizedTweet += word_.capitalize+" "
      }else
      {
        capitalizedTweet += word_ +" "
      }        
    }   
   return capitalizedTweet.trim() 
  }
  
  //method that capitalizes the first letter of nouns    
  def capitalizeWord(word:String, posWord:String) :String =
  {     
    val posToCapitalize = Array( "NN", "NNS", "NNP", "NNPS" )
    val posNewWords = posWord.split(" ")
    val intersec = posToCapitalize.intersect(posNewWords)
    var capitalizedTweet = ""          
    if(intersec.length > 0)
    {
      capitalizedTweet = word.capitalize
    }else
    {
      capitalizedTweet = word
    }  
    return capitalizedTweet.trim() 
  }
  //delete part of speech from a tweet   
  def deleteTweetPos(tweet:String) :String = 
  {
    var newTweet = ""
    for(word <- tweet.split(" "))
    {
      var (word_,pos) = Utils.splitWord(word, "_")
      newTweet += word_ +" "
    }      
    return newTweet      
  }
  //tweet to an array
  def tweetToArray(text:String, separator:String) :Array[String] = 
  {
    var arrayString:Array[String] = text.split(separator)
    arrayString = arrayString.map(x => x.replaceAll("\"", ""))
    return arrayString
  }
  
  //generating a hashmap from a RDD  
  def generateDictFromRDD(textRDD:RDD[String], sc:SparkContext) :scala.collection.Map[String, Long] =  
  { 
    
    var mapDictionary = textRDD.map(line => deleteTweetPos(tweetToArray(line,"\",\"")(1)))
                               .flatMap(line=>line.split(" "))
                               .map(word => (word,1L))
                               .reduceByKey(_+_)
                               .filter(_._2 >= 10)
    
    var returnRddDict = mapDictionary.collectAsMap()          
    return returnRddDict
  }
    
  def filterPerMonth (tweet:String, monthFilter:Int) :Boolean =
  {
    var result = false
    val dft : DateTimeFormatter =  DateTimeFormat.forPattern("E MMM d HH:mm:ss Z yyyy")
    var tweetTime : DateTime = new DateTime
    
    val cols = tweet.split(",").map(_.trim)      
    tweetTime = dft.parseDateTime(cols(1))
    val monthDataSet = tweetTime.getMonthOfYear()
    if(monthDataSet == monthFilter )
    {
      result = true
    }
    return result
  }
    
}