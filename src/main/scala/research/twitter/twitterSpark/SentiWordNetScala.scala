package research.twitter.twitterSpark

import scala.collection.JavaConversions._
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import scala.io._
import scala.io.Source._
import scala.io.Source

import java.io.InputStreamReader

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

import com.opencsv.CSVWriter

import java.io.BufferedReader
import java.io.FileWriter
import java.io.BufferedWriter



/*class that interacts the lexicon SentiWordNet 3.0: An Enhanced Lexical Resource for Sentiment Analysis and Opinion Mining.},
  Baccianella, Stefano and Esuli, Andrea and Sebastiani, Fabrizio
  
  * */

class SentiWordNetScala(pathToSWN: String) {
  
  var dictionary: Map[String, Double] = new HashMap[String, Double]()
  var dictionaryNeg: Map[String,Double] = new HashMap[String, Double]()
  var dictionaryPos: Map[String,Double] = new HashMap[String, Double]()
  var dictionaryObj: Map[String,Double] = new HashMap[String, Double]()  
  val tempDictionary = new HashMap[String, HashMap[Integer, Double]]()  
  val tempDictionaryPosNeg  = new HashMap[String, HashMap[Integer, String]]()
  var lineNumber = 0
  var line: String = ""
  
  try {
    
    var conf = new Configuration()
    var fileSystem = FileSystem.get(conf)
    var path = new Path(pathToSWN)
    val inputStream = fileSystem.open(path)
    val csv = new BufferedReader(new InputStreamReader(fileSystem.open(path)))
    
    while ( {(line = csv.readLine()); line != null} )
    {
      if(line != None)
      {
        lineNumber += 1
        if (!line.trim().startsWith("#")) {
          val data = line.split("\t")
          val wordTypeMarker = data(0)  
          if (data.length != 6) {
            throw new IllegalArgumentException("error while reading file, line: " + lineNumber)
          }
          val synsetScore = java.lang.Double.parseDouble(data(2)) - java.lang.Double.parseDouble(data(3))          
          val posNegativeString:String = data(2)+"##"+data(3)          
          val synTermsSplit = data(4).split(" ")
          for (synTermSplit <- synTermsSplit) {            
            val synTermAndRank = synTermSplit.split("#")
            val synTerm = synTermAndRank(0) + "#" + wordTypeMarker
            val synTermRank = java.lang.Integer.parseInt(synTermAndRank(1))            
            if (!tempDictionary.containsKey(synTerm)) {
              tempDictionary.put(synTerm, new HashMap[Integer, Double]())              
            }
            tempDictionary.get(synTerm).put(synTermRank, synsetScore) 
            if(!tempDictionaryPosNeg.containsKey(synTerm)){
              tempDictionaryPosNeg.put(synTerm, new HashMap[Integer,String]())
            }
            tempDictionaryPosNeg.get(synTerm).put(synTermRank,posNegativeString)
          }
        }          
      }      
    }
    
  } catch {
    case t: Throwable => t.printStackTrace() 
  } 
  for ((key, value) <- tempDictionary) {
    val word = key
    val synSetScoreMap = value
    var score = 0.0
    var sum = 0.0
    var counter = 0    
    for ((key, value) <- synSetScoreMap) {     
      score += value / key.toDouble         
      //score += value //Key/ Rank   
      sum += 1.0 / key.toDouble
      counter += 1
    }
    score /= sum 
    dictionary.put(word, score)
  }    
  for ((key, value) <- tempDictionaryPosNeg) {
    val word = key
    val hashMapSentimentValues = value //a term can have different sentiment values + Rankings
    var scorePositive = 0.0
    var scoreNegative = 0.0
    var sumPositive = 0.0
    var sumNegative = 0.0      
    var sum = 0.0    
    var counter = 0
    for ((key,value) <- hashMapSentimentValues)
    {
      var positive = 0.0
      var negative = 0.0
      val sentimentVal = value.split("##")
                
      positive = java.lang.Double.parseDouble(sentimentVal(0).toString()) // positive
      negative = java.lang.Double.parseDouble(sentimentVal(1).toString()) // negative
      scorePositive += positive / key.toDouble  //Harmonic Mean 
      scoreNegative += negative / key.toDouble //Harmonic Mean
      counter += 1
      sum += 1.0 / key.toDouble
    }    
    scorePositive /= sum  //Harmonic Mean 
    scoreNegative /= sum  //Harmonic Mean
    scoreNegative = scoreNegative
    dictionaryPos.put(word, scorePositive)
    dictionaryNeg.put(word, scoreNegative)
    var objectivity = 1 - (scorePositive + scoreNegative)
    dictionaryObj.put(word, objectivity) 
  } 
    
  def contains(word: String, pos: String) :Boolean =
  {
    val containsWord = dictionary.containsKey(word + "#" + pos)
    return containsWord
  } 
      
  def extract(word: String, pos: String): Double = {    
    var result = 0.0
    val containsWord = dictionary.containsKey(word + "#" + pos)
    if(containsWord)
    {      
      result = dictionary.get(word + "#" + pos)      
    }    
    return result
  }
  
  def extractPos(word: String, pos: String): Double = {
    var result = 0.0 
    val containsWord = dictionaryPos.containsKey(word + "#" + pos)
    if(containsWord)
    {
      result = dictionaryPos.get(word + "#" + pos)
    }    
    return result
  }
  
  def extractNeg(word: String, pos: String): Double = {    
    var result = 0.0 
    val containsWord = dictionaryNeg.containsKey(word + "#" + pos)
    if(containsWord)
    {
      result = dictionaryNeg.get(word + "#" + pos)
    }    
    return result
  }
  
  def extractObjectivity(word: String, pos: String): Double = {    
    var result = 0.0 
    val containsWord = dictionaryObj.containsKey(word + "#" + pos)
    if(containsWord)
    {
      result = dictionaryObj.get(word + "#" + pos)
    }    
    return result
  }
  
   
  def statiticsSentiWordNet() = 
  { 
    val sentiWordNetCsv = Source.fromFile(PrParameters.projectFolder+"SentiWordNetStatistics.csv")
    var countPositivesTerms:Int = 0
    var countNegativeTerms:Int = 0
    var countObjectiveTerms:Int = 0
    var countOnlyObjecTerms:Int = 0
    var countNeutralSameValDistinctZero:Int = 0
    for(line <- sentiWordNetCsv.getLines())
    {
      val lineArray = line.split("\t")
      val word = lineArray(0)
      val wordPosit = lineArray(1).toDouble
      val wordNegat = lineArray(2).toDouble
      val wordObj   = lineArray(3).toDouble
      if( wordPosit > wordNegat ) 
      {   
        countPositivesTerms += 1         
      }
      if( wordNegat > wordPosit ) 
      {  
        countNegativeTerms += 1 
      }
      if ((wordNegat == wordPosit)&& (wordNegat == 0.0) && (wordPosit == 0.0) )
      {
        countOnlyObjecTerms += 1        
      }
      if( (wordNegat == wordPosit)&& (wordNegat > 0 ) && (wordNegat > 0) )
      {
        countNeutralSameValDistinctZero += 1
        println(word)
      }      
    }
    println("Positive Terms SentiWordNet: "+ countPositivesTerms)
    println("Negative Terms SentiWordNet: "+ countNegativeTerms)    
    println("Only Objective Terms SentiWordNet: "+ countOnlyObjecTerms)
    println("Neutral Terms distinct Zero: "+ countNeutralSameValDistinctZero)
  }
}