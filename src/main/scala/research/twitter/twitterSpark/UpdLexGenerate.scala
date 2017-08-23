package research.twitter.twitterSpark

import scala.io.Source
import java.io.BufferedWriter
import java.io.FileWriter
import com.opencsv.CSVWriter
import scala.collection.mutable.Map
import scala.collection.mutable.HashMap
import scala.math.log
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.util._

import org.apache.log4j.Logger
import org.apache.log4j.Level



object UpdLexGenerate {
  
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)  
  var dictionaryHMap =   scala.collection.Map[String,Int]()
  
  def MethodAvgUpdateLexicon(word:String, pos:String, sentiment:String, number:Long):String = 
  {   
    var stringTweet = ""    
    var countTot:Long = 0 
    var others:Long = 0        
    //ugh	0.09623102425405688	0.9037689757459432
    val wordNoSent  =  word
    val partSpeech  =  pos.replaceAll("[<>]", "")
    
    val (countPos, countNeg, countNeut) = separateValues(sentiment)
    
    if (countPos > 5 && countNeg >5)
    {
      //Including only the positive and negative we do not consider Neutral therefore we subtract it from the countTot        
      var countTot:Long = (countPos + countNeg)        //Not considering the neutral and no sentiment cases only positive and negative clases
      var avgPos:Double  = countPos.toDouble/countTot.toDouble
      var avgNeg:Double  = countNeg.toDouble/countTot.toDouble 
      //POS:  "a" -> adjectives, "n" -> nouns, "r" -> adverbs, "v" -> verbs, "o" -> others    
      //WriteCSV.writeArrayToCSV(Array(partSpeech, wordNoSent,  avgPos.toString(), avgNeg.toString(), countTot.toString() ),csvSlangFoundWriter)
      stringTweet = pos+"\t"+wordNoSent+"\t"+avgPos.toString()+"\t"+avgNeg.toString()+"\t"+countTot.toString()
    }
    return stringTweet
  }  
  //MethodPmiUpdateLexicon
  def MethodPmiUpdateLexicon(word:String, pos:String, sentiment:String, number:Long, labelDistantSup:Boolean, totalTweets:Long, totalTweetsPositive:Long, totalTweetsNegative:Long, maxVal:Double, minVal:Double, tweetoToken:String ) : String=
  {
      var stringTweet = ""
      
      val ( countPos, countNeg, countNeut) = separateValues(sentiment)
      if (countPos > 5 && countNeg > 5)
      { 
        //Only consider the positive + negative from the new words combined
        val freqWordCorpus = countPos + countNeg
        var pmiPositive = aggSentiValsPmiPositive(totalTweets, countPos, freqWordCorpus, totalTweetsPositive)
        var pmiNegative = aggSentiValsPmiNegative(totalTweets, countNeg, freqWordCorpus, totalTweetsNegative)
        var newSentiment = pmiPositive - pmiNegative
        //Normalization
        var normVal = normalizeValue(maxVal, minVal,newSentiment )
        
//        if(word == "obama" || word == "iphone" )
//        {
//            println("type: "+tweetoToken ) 
//            println("word: "+ word)
//            println("totalTweets: " + totalTweets)
//            println("countPos: " + countPos)
//            println("countNeg: "+ countNeg)
//            println("freqWordCorpus: " + freqWordCorpus)
//            println("totalTweetsPositive: " + totalTweetsPositive)
//            println("totalTweetsNegative:  " + totalTweetsNegative)
//            println("pmiPositive: "+pmiPositive)
//            println("pmiNegative: "+pmiNegative)
//            println("pmiComWithoutNormalization: "+newSentiment)
//            println("pmiComNormalization: "+normVal)
//        }
        stringTweet = pos+"\t"+word+"\t"+normVal.toString()+"\t"+freqWordCorpus.toString()
      }
    return stringTweet
  }
  
  def normalizeValue (maxValue:Double, minValue:Double, value:Double) :Double =
  {
    var normValue = ( ( 2*( (value - minValue) / (maxValue - minValue) )) - 1 )
    return normValue    
  }
  
  //calculate max and min values 
  def CalculateMaxMin( word:String, pos:String, sentiment:String, wordFreq:Long, totalTokens:Long, totalTokensPositive:Long, totalTokensNegative:Long ) :Double =
  {  
    var finalValue:Double = 0.0
    val (countPos, countNeg, countNeut) = separateValues(sentiment)
    if (countPos > 5 && countNeg > 5 )  
    {
      val freqWordCorpus =  countPos + countNeg
      var pmiPositive = aggSentiValsPmiPositive(totalTokens, countPos, freqWordCorpus, totalTokensPositive) //TODO
      var pmiNegative = aggSentiValsPmiNegative(totalTokens, countNeg, freqWordCorpus, totalTokensNegative)
      finalValue = pmiPositive - pmiNegative
    }
    return finalValue    
  } 
  
  //point Wise Mutual Information for positive Labels
  def aggSentiValsPmiPositive(totalToks:Long, freqWordPosiLabel:Long, freqWordCorpus:Long, totalToksPos:Long ) :Double =
  {     
    var aggregateValue:Double = 0.0  
    aggregateValue = log2( BigDecimal( BigInt(freqWordPosiLabel)*BigInt(totalToks) ) / BigDecimal( BigInt(freqWordCorpus)*BigInt(totalToksPos) ) )      
    return aggregateValue
  }    
    
  //point Wise Mutual Information for negative Labels
  def aggSentiValsPmiNegative(tokensCorpus:Long, freqWordNegLabel:Long, freqWordCorpus:Long, totalToksNeg:Long ) :Double =
  {    
    var aggregateValue:Double = 0.0
    aggregateValue =  log2( BigDecimal( BigInt(freqWordNegLabel)* BigInt(tokensCorpus)) / BigDecimal( BigInt(freqWordCorpus)*BigInt(totalToksNeg) ) )      
    return aggregateValue
  }    
    
  //method that calculates the log in base 2
  def log2(valueIn:BigDecimal) :Double =
  {
    var res = log(valueIn.toDouble)/log(2)
    return res
  }
  
  //separate values 
  def separateValues(values:String) = 
  {    
    val sentiValues = values.split(" ")    
    var countPos = sentiValues(0).split(":")(1).toLong
    var countNeg = sentiValues(1).split(":")(1).toLong
    var countNeut = sentiValues(2).split(":")(1).toLong
    (countPos, countNeg, countNeut)    
  }
  
   
  def tokenizeDictionaryResults(sparkCont:SparkContext, sourceRDD:RDD[String], labelDistSup:Boolean) =
  { 
    var initRDD = if(labelDistSup) sourceRDD.map(line => removePoSpeechNegation(line)) else  sourceRDD.filter (line => isPosorNeg(line)).map(line => removePoSpeechNegation(line))    
    dictionaryHMap  = initRDD.flatMap(line=>line.split(" ")).map(word => (word,1)).reduceByKey(_+_).collectAsMap()
  }
  
  def removePoSpeechNegation(tweet:String):String =
  {

    var arrayWords = tweet.replaceAll("[\"]", "").split(',')(2)
    var newTweet:String = ""
    for( word <- arrayWords.split(" "))
    {
       var partWords =  word.split("_")
       if(partWords.length == 3)
       {
         newTweet += " "+partWords(1)
       }else if (partWords.length == 2) {
         newTweet += " "+partWords(0)         
       }
    }
    return newTweet.trim()    
  }
  
  
  //taking the result csv we calculate the total number of Tokens 
  def calculateTokensTotal(sc:SparkContext, inRdd:RDD[String], isDistantSuper:Boolean) :Long =
  { 
    var resultRDD  =  if(isDistantSuper) inRdd.map(line => removePoSpeechNegation(line)) else inRdd.filter(line => isPosorNeg(line)).map(line => removePoSpeechNegation(line))    
    var totalNumberTokens = resultRDD.flatMap(word => word.split(" ")).count()
    return totalNumberTokens    
  }
  
  
  //considering the result csv we calculate number of tokens in the positive labels 
  def calculateTokensPosiClas(sparkCont:SparkContext, inRdd:RDD[String], labelDist:Boolean):Long = 
  { 
    var resultRDD = if(labelDist) inRdd.filter(line => isPosDist(line)).map(line => removePoSpeechNegation(line) )  else inRdd.filter(line => isPosSentiWord(line)).map(line => removePoSpeechNegation(line))   
    var resultCsvRDD = resultRDD.map(line => line.trim())        
    var totalNoTokensPositive = resultCsvRDD.flatMap(line => line.split(" ")).count()        
    return totalNoTokensPositive    
  }
  //considering the result csv calculate number of tokens in the negative labels 
  def calculateTokensNegClas(sc:SparkContext, inRdd:RDD[String], labelDist:Boolean):Long = 
  { 
    var resultRDD = if(labelDist) inRdd.filter(line => isNegDist(line)).map(line => removePoSpeechNegation(line)) else inRdd.filter(line => isNegSentiWord(line)).map(line => removePoSpeechNegation(line))    
    var resultCsvRDD = resultRDD.map(line => line.trim())    
    var totalNoTokensNegative = resultCsvRDD.flatMap(line => line.split(" ")).count()    
    return totalNoTokensNegative
  }
  
  /*Considering the number of tweets*/ 
   def calculateTweetsTotal(sparkCont:SparkContext, sourceRDD:RDD[String], labelDistantSuper:Boolean) :Long =
  { 
    var resultRDD  =  if(labelDistantSuper) sourceRDD.map(line => removePoSpeechNegation(line)) else sourceRDD.filter(line => isPosorNeg(line)).map(line => removePoSpeechNegation(line))    
    var totalNumberTokens = resultRDD.count()
    return totalNumberTokens    
  }
  
  //considering the number of tokens in positive labels
  def calculateTweetsPosiClas(sparkCont:SparkContext, sourceRDD:RDD[String], labelDistant:Boolean):Long = 
  {     
    var rddPositiveTweets = if(labelDistant) sourceRDD.map(line => line.replaceAll("[\"]", "").split(',')(0).toInt )  else   sourceRDD.map(line => line.replaceAll("[\"]", "").split(',')(1).toInt )    
    var rddResult = rddPositiveTweets.filter(line => (line == 4)).count()       
    return rddResult    
  }
  //considering the number of tokens in negative labels
  def calculateTweetsNegClas(sparkCont:SparkContext, sourceRDD:RDD[String], labelDistant:Boolean):Long = 
  { 
    var rddNegativeTweets = if(labelDistant) sourceRDD.map(line => line.replaceAll("[\"]", "").split(',')(0).toInt ) else sourceRDD.map(line => line.replaceAll("[\"]", "").split(',')(1).toInt  )
    var rddResNeg = rddNegativeTweets.filter(line => (line == 0)).count()
    return rddResNeg
  }
 //check if the sentiment is positive
  def isPosDist(line:String) :Boolean=
  {
    var isPos = false 
    var labelSentWNet = line.replaceAll("[\"]", "").split(",")(0).toInt
    if(labelSentWNet == 4)
    {
      isPos = true
    }
    return isPos
  }
  //check if the sentiment is negative
  def isNegDist(line:String) :Boolean=
  {
    var isNeg = false    
    var labelSentWNet = line.replaceAll("[\"]", "").split(",")(0).toInt
    if(labelSentWNet == 0)
    {
      isNeg = true
    }
    return isNeg
  }
  
  
  def isPosSentiWord(line:String) :Boolean=
  {
    var isPos = false
    var labelSentWNet = line.replaceAll("[\"]", "").split(",")(1).toInt
    if(labelSentWNet == 4)
    {
      isPos = true
    }
    return isPos
  }
  
  def isNegSentiWord(line:String) :Boolean=
  {
    var isNeg = false    
    var labelSentWNet = line.replaceAll("[\"]", "").split(",")(1).toInt
    if(labelSentWNet == 0 )
    {
      isNeg = true
    }
    return isNeg
  }
  
  def isPosorNeg(line:String) :Boolean=
  {
    var isPoN = false
    var labelSentWNet = line.replaceAll("[\"]", "").split(",")(1).toInt
    if(labelSentWNet == 0 || labelSentWNet == 4)
    {
      isPoN = true
    }
    return isPoN
  }  
}