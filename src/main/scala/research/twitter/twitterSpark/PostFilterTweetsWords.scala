package research.twitter.twitterSpark

import scala.io.Source
import java.io.BufferedWriter
import java.io.FileWriter
import com.opencsv.CSVWriter
import scala.collection.mutable.Map
import scala.collection.mutable.HashMap
import scala.math.log
import scala.collection.mutable.ArrayBuffer

object PostFilterTweetsWords {
  //deletes innecesary tweets
  def deleteInnecesaryTweets() :Unit =
  {       
    val csvReaderNewWordsNoSentiment =  Source.fromFile(PrParameters.projectFolder+"sentimentDataset.csv")    
    val csvForDictionary = new BufferedWriter(new FileWriter(PrParameters.projectFolder+"totalTest_ohneInnecesary.csv"));
    val csvForDictionaryWrt:CSVWriter = new CSVWriter(csvForDictionary,'\t')  
    var counter:Int = 0    
    for (line <- csvReaderNewWordsNoSentiment.getLines())
    {
      var lineModified = line.replaceAll("[\"]", "")
      if(lineModified.contains("blktweetpreprocessing")|| lineModified.contains("typostweet") || lineModified.contains("blkless3chars") || lineModified.contains("blklessfreqtweet") || lineModified.contains("blknegationtweet") || lineModified.contains("blkstopwordtweet") || lineModified.contains("meaninglesstweet") )
      {
        counter += 1         
      }else
      {
        WriteCSV.writeTextToCSV(lineModified, csvForDictionaryWrt)  
      }
    }
    println("Successfully Finished ")
    println(counter)
    WriteCSV.closeWriter(csvForDictionaryWrt)
  }
  //method that returns true if a tweet was marked as invalid to avoid performing the further steps
  def hasInnecTweets(tweet:String) :Boolean =
  {
    var hasInnecTweet:Boolean = true
    var lineModified = tweet.replaceAll("[\"]", "")
    if(lineModified.contains("blktweetpreprocessing")|| lineModified.contains("typostweet") || lineModified.contains("blkless3chars") || lineModified.contains("blklessfreqtweet") || lineModified.contains("blknegationtweet") || lineModified.contains("blkstopwordtweet") || lineModified.contains("meaninglesstweet") )
    {
      hasInnecTweet = false         
    }
    return hasInnecTweet
  }
  //"meaninglesstweet_NN::4"
  def deleteInnecesaryTweetsFromNewWords(sourceFile:String) :Unit =
  {
    val csvReaderNewWordsNoSentiment =  Source.fromFile(PrParameters.projectFolder+sourceFile+".csv")   
    val csvForDictionary = new BufferedWriter(new FileWriter(PrParameters.projectFolder+sourceFile+"_ohneInnecesary.csv"));
    val csvForDictionaryWrt:CSVWriter = new CSVWriter(csvForDictionary,'\t')  
    var counter:Int = 0    
    for (line <- csvReaderNewWordsNoSentiment.getLines())
    {
      var lineModified = line.replaceAll("[\"]", "")
      if(lineModified.contains("blktweetpreprocessing")|| lineModified.contains("typostweet") || lineModified.contains("blkless3chars") || lineModified.contains("blklessfreqtweet") || lineModified.contains("blknegationtweet") || lineModified.contains("blkstopwordtweet") || lineModified.contains("meaninglesstweet"))
      {
        counter += 1         
      }else
      {
        WriteCSV.writeTextToCSV(lineModified, csvForDictionaryWrt)  
      }
      
    }
    println("Successfully Finished ")
    println(counter)
    WriteCSV.closeWriter(csvForDictionaryWrt)
  }
  //deletes innecesary word from the updates to the lexicon 
  def hasInneWords(word:String) : Boolean = 
  {
    var hasInneWord = true
    var lineModified = word.replaceAll("[\"]", "")
    if(lineModified.contains("blktweetpreprocessing")|| lineModified.contains("typostweet") || lineModified.contains("blkless3chars") || lineModified.contains("blklessfreqtweet") || lineModified.contains("blknegationtweet") || lineModified.contains("blkstopwordtweet") || lineModified.contains("meaninglesstweet") || lineModified.contains("null") )
    {
      hasInneWord = false         
    }
    return hasInneWord
  }
 
  def main (arg:Array[String]) =
  {
     deleteInnecesaryTweets()
     deleteInnecesaryTweetsFromNewWords("NewWords")
     deleteInnecesaryTweetsFromNewWords("NewWordsOriginalSentiment")
  }
}