package research.twitter.twitterSpark

import scala.io.BufferedSource
import scala.io.Source
import org.apache.spark.SparkContext

import java.io.InputStreamReader
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/*Method that loads the verbs along with the opposites and has methods to interact*/

object ReplaceNegations {
  
  var listOppositesHMap = scala.collection.Map[String, String]()
  var listVerbsHMap = scala.collection.Map[String, Int]()
  
  loadVerbsList()
  loadOppositeList()
  
  //search negation word and analyzes up to the third next word whether it is followed by a verb or adjective
  //I am not in a really good mood -->I am bad mood 
  def searchNegationsPOS(tweet:String):String = {  
    val charNegation = """(not_[A-Z]{2,4}+\x20{1})""".r
    val result = charNegation.findFirstIn(tweet)
    var newTweet = ""
    
    if(result != None)
    { 
      var arrayStringTweet   =  tweet.split(" ")
      var numberWordsOnTweet:Int  =  arrayStringTweet.length  //To avoid e.g. 7 < 8   
      var counter:Int = 0            
      while(counter < numberWordsOnTweet)
      { 
        if(arrayStringTweet(counter) == "not_RB" && (counter < numberWordsOnTweet - 1)) 
        {
          var foundNegationPhrase:Boolean = false
          var nextWord:Int = 1
          var enoughWords:Boolean = true
          
          while(!foundNegationPhrase && (nextWord < 5) && enoughWords)
          {           
            var negationPhrase:String = ""
            var followedVerb:Boolean   = isFollowingVerb(arrayStringTweet, counter, nextWord)
            var followedAdj:Boolean    = isFollowingAdj(arrayStringTweet, counter, nextWord)
            if(followedVerb || followedAdj)
            {
              
              if(followedVerb)
              {                
                negationPhrase =    buildNegationPhraseVerb(arrayStringTweet, counter, nextWord) 
                newTweet += negationPhrase+" "
              }
              if(followedAdj)
              {
                negationPhrase =   buildNegationPhraseAdj(arrayStringTweet, counter, nextWord)
                newTweet += negationPhrase+" "
              }
              
              foundNegationPhrase = true
              counter += nextWord+1             
                
            }else
            { 
              if((counter + nextWord ) < (numberWordsOnTweet - 1))
              {                
                nextWord += 1
              }else
              {
                enoughWords = false
              }
            }
          }           
          if(!foundNegationPhrase)
          {
            newTweet += arrayStringTweet(counter)+" "
            counter += 1
          }
        }else 
        {
          newTweet += arrayStringTweet(counter)+" "
          counter+=1
        }
      }      
    }else
    {
      newTweet = tweet
    }
    if(newTweet.trim().length == 0 )
    {
      newTweet = "blknegationtweet_BT"     
    }
    return newTweet.trim()
  }
  
  //Up to 4 next positions
  def isFollowingVerb(tweet:Array[String], positionNegation:Int, numberFollowingWords:Int) :Boolean =
  {
    var foundVerb:Boolean = false
    var indexFollowingWord:Int = positionNegation + numberFollowingWords    
    var (followingWord, posFollowingWord) = Utils.splitWord(tweet(indexFollowingWord), "_")
    foundVerb =  Utils.isVerb(posFollowingWord)        
    return foundVerb
  }
  //build negation phrase verb
  def buildNegationPhraseVerb(tweet:Array[String], positionNeg:Int, noFollwWords:Int) :String =
  {
    var tweetNegation:String = ""
    val numberWords = tweet.length    
    val isNegVerb  = searchListOpposite(tweet(positionNeg+noFollwWords))    
    val beforeNegation   = tweet.slice(0,positionNeg)
    val negation = if(isNegVerb)  Array("not_"+tweet(positionNeg+noFollwWords))  else Array("not_"+tweet(positionNeg+noFollwWords))    
    val afterNegation    = tweet.slice(positionNeg+noFollwWords+1, numberWords)
    val tweetArrayNegation = beforeNegation++negation++afterNegation    
    tweetNegation = negation.mkString(" ")    
    return tweetNegation
  }
  
  //Up to 4 next positions
  def isFollowingAdj(tweet:Array[String], positionNegation:Int, numberFollowingWords:Int) :Boolean =
  {
    var foundAdj:Boolean = false
    var indexFollowingWord:Int = positionNegation + numberFollowingWords
    var (followingWord, posFollowingWord) = Utils.splitWord(tweet(indexFollowingWord), "_")
    foundAdj = Utils.isAdjective(posFollowingWord)
    return foundAdj
  }
  //build negation phrase adj 
  def buildNegationPhraseAdj(tweet:Array[String], positionNeg:Int, noFollwWords:Int) :String =
  {
    var tweetNegation:String = "" 
    val numberWords = tweet.length    
    val isOpposite = searchListOpposite(tweet(positionNeg+noFollwWords))    
    val beforeNegation = tweet.slice(0,positionNeg)    
    val negation = if(isOpposite) Array(getOppositeHMap(tweet(positionNeg+noFollwWords)) ) else Array("not_"+tweet(positionNeg+noFollwWords))
    val afterNegation = tweet.slice(positionNeg+noFollwWords+1, numberWords)
    val tweetArrayNegation = beforeNegation++negation++afterNegation    
    tweetNegation = negation.mkString(" ")    
    return tweetNegation    
  }
  
  //search opposite
  def searchListOpposite(wordPOS:String) :Boolean =  
  {
    var flag = false
    var wordPOSArray = wordPOS.split("_") //word_POS
    if(listOppositesHMap.contains(wordPOSArray(0)))
    {
      flag = true
    }
    
    return flag
  }
  //get the opposite 
  def getOppositeHMap(wordPOS:String) :String = 
  {
    var wordPOSArray = wordPOS.split("_")
    var out:String = ""    
    out =  listOppositesHMap.get(wordPOSArray(0)).mkString(" ":String)+"_"+wordPOSArray(1)
    return out
  }
  
  //search in a list of verbs
  def searchListVerbs(wordPOS:String) :Boolean =  
  {
    var flag = false
    var wordPOSArray = wordPOS.split("_") //word_POS
    if(listVerbsHMap.contains(wordPOSArray(0)))
    {
      flag = true
    }    
    return flag
  }

  //check for other negation word like none or no and change to not   
  def checkOtherNegations(tweet:String) : String = 
  {
    var checkedTweet = ""
    val regExpr = """(none of\x20{1})|(no\x20{1})|(none\x20{1})""".r
    val regExprAtEnd = """(none of)$|(no)$|(none)$""".r      
    checkedTweet = regExpr.replaceAllIn(tweet, "not any ")     
    checkedTweet = regExprAtEnd.replaceAllIn(checkedTweet, "not any")
    return checkedTweet
  }    

  //check other negation word like none or no and change to not case part of speech 
  def checkOtherNegationsPOS(tweet:String) : String = 
  {
    var checkedTweet = ""    
    val regExpr = """(\bnone_[A-Z]{2,4} of_[A-Z]{2,4}\x20{1})|(\bno_[A-Z]{2,4}\x20{1})|(\bnone_[A-Z]{2,4}\x20{1})""".r      
    val regExprAtEnd = """(none_[A-Z]{2,4} of_[A-Z]{2,4})$|(no_[A-Z]{2,4})$|(none_[A-Z]{2,4})$""".r          
    checkedTweet = regExpr.replaceAllIn(tweet, "not_RB any_DT ")     
    checkedTweet = regExprAtEnd.replaceAllIn(checkedTweet, "not_RB any_DT")
    return checkedTweet
  }
  
  //load list of verbs into memory 
  def loadVerbsList()  =
  {
    var conf = new Configuration()
    var fileSystem = FileSystem.get(conf)
    var path = new Path(PrParameters.listVerbsNegation)
    val inputStream = fileSystem.open(path)
   
    var line: String = ""  
    val csv = new BufferedReader(new InputStreamReader(fileSystem.open(path)))
    while ( {(line = csv.readLine()); line != null} )
    {
      if(line != None)
      {
        listVerbsHMap += line -> 1
      }
    }   
  }
  
  //load opposite list words into memory
  def loadOppositeList()  =
  {
    var conf = new Configuration()
    var fileSystem = FileSystem.get(conf)
    var path = new Path(PrParameters.listAdjOpp)
    val inputStream = fileSystem.open(path)
    
    var line: String = ""  
    val csv = new BufferedReader(new InputStreamReader(fileSystem.open(path)))
    while ( {(line = csv.readLine()); line != null} )
    {
      if(line != None)
      {
        listOppositesHMap += line.split("::")(0) -> line.split("::")(1)          
      }
    } 
  }    
}