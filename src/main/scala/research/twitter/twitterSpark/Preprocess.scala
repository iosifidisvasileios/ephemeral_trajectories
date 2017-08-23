package research.twitter.twitterSpark


import scala.io.BufferedSource
import scala.io.Source
import scala.io.Codec
import java.nio.charset.CodingErrorAction

import org.apache.spark.SparkContext
import org.apache.spark
import org.apache.spark.broadcast._

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import java.io.InputStreamReader

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

/*object that contains the methods for pre processing */

object Preprocess {
  
  implicit val codec = Codec("UTF-8")
  codec.onMalformedInput(CodingErrorAction.REPLACE)
  codec.onUnmappableCharacter(CodingErrorAction.REPLACE)    
  var listContractionsHMap =  scala.collection.Map[String,String]()
  var listStopWordsHMap = scala.collection.Map[String,Int]()
  //loading the source files into memory
  loadStopWords()
  loadContractionList()
   
  
  //lower case 
  def lowerCaseTweet(tweet:String) :String = 
  {
    return  tweet.toLowerCase().trim()
  }
  
  //remove 2 or more consecutive blank spaces
  def deleteBlankSpaces(tweet:String) : String = {    
    val blankSpaceRegex = """[\x20]{2,}""".r     
    var tweetString : String =  blankSpaceRegex.replaceAllIn(tweet, " ")
    if(tweetString.trim().length == 0 )  tweetString = "blktweetpreprocessing"
    return tweetString.trim()
  }
  
  def deleteBlankSpacesFinal(listWordsTweet:String) : String = {    
    val blankSpaceregex = """[\x20]{2,}""".r
     var tweetString : String = listWordsTweet
     tweetString = blankSpaceregex.replaceAllIn(listWordsTweet, " ")
      if(tweetString.trim().length == 0 )
      {
        tweetString = "blktweetpreprocessing_BT"     
      }      
      return tweetString.trim()
  }
  
  //check for SpecialHtmlChars
  def checkForSpecialHtmlChars(tweet: String) : String =
  {         
    val regexSpecialHtlm = """&(?:[A-Za-z]+[;])""".r
    var tweetString : String = regexSpecialHtlm.replaceAllIn(tweet, " ")
    tweetString =  this.deleteBlankSpaces(tweetString)
    return tweetString.trim()
  }
  
  //method for removing special characters
  def deleteSpecialCharacters(listWordsTweet: String) : String =   {
    val numPattern = """((https?:\/\/(?:www\.|(?!www))[^\s\.]+\.[^\s]{2,}|www\.[^\s]+\.[^\s]{2,})|[^\x20\x27\x40\x23\x2D\x30-\x39\x41-\x5A\x61-\x7A]+)+""".r     
    var tweetString = numPattern.replaceAllIn(listWordsTweet, " ")  
    val blankSpaceRegExp = """[\x20]{2,}""".r
    tweetString = blankSpaceRegExp.replaceAllIn(tweetString, " ")
    if(tweetString.trim().length == 0 )
    {
      tweetString = "blktweet"     
    }
    return tweetString.trim()      
  }
  //delete urls  
  def deleteUrls(tweet: String) : String =   {
    val numPattern = """((https?:\/\/(?:www\.|(?!www))[^\s\.]+\.[^\s]{2,}|www\.[^\s]+\.[^\s]{2,}))+""".r 
    var tweetString = numPattern.replaceAllIn(tweet, " ") 
    tweetString = this.deleteBlankSpaces(tweetString)  
    return tweetString.trim()      
  }
  
  //characters when they start with re- pre-  
  def removeRePreCharacteresBegin(tweet:String):String = 
  {    
    var tweetString = tweet
    if(tweet.contains("-")){
      tweetString = ""      
      val patternRemove = """(\b(re|pre|Pre|Re)+\x2D{1})+""".r      
      val seqStr = Seq("re-","pre-","Pre-","Re-")
      var stringWord = ""
      
      for(word <- tweet.split(" "))
      {   
        
        if(word.startsWith("re-") || word.startsWith("pre-") || word.startsWith("Re-") || word.startsWith("Pre-") )
        {          
          stringWord = word.replaceFirst("[-]", "")
          if(Thesaurus.searchTerm(stringWord))
           {
             tweetString += stringWord+" "
           }else
           {
             stringWord = patternRemove.replaceAllIn(word, "")
             tweetString += stringWord+" "          
           }
        }
        else 
        {
          tweetString += word+" "
        }
      }   
      tweetString = this.deleteBlankSpaces(tweetString)      
    }   
    return tweetString.trim()
  }
  
  
  //delete numbers but keep the string that contain numbers like ipad2 or iphone3
  def deleteNumbers(tweet:String) :String = 
  { 
    val regexNumber = """(\b[0-9]+\b)""".r
    var tweetString = regexNumber.replaceAllIn(tweet, "")
    tweetString = this.deleteBlankSpaces(tweetString)    
    return tweetString.trim()
  }
  
  //remove the strings that contain @ and remove char #
  def deleteSharpAtChars(wordsTweet:String):String = 
  {
    var newTweet = wordsTweet
    if(wordsTweet.contains("@")|| wordsTweet.contains("#"))
    {
      val regexSharp = """(\S+@\S+)|(@\S+)|(\S+@\B)|(\B@\B)""".r //@
      val regexAt = """(\S+#\S+)|(#\S+)|(\S+#\B)|(\B#\B)""".r //#
      newTweet = regexSharp.replaceAllIn(wordsTweet, "")      
      newTweet = regexAt.replaceAllIn(newTweet, "")
      newTweet = this.deleteBlankSpaces(newTweet)      
    }      
     return newTweet.trim()
  }
  //remove the strings that contain @ and remove char #
  def deleteAuthorHashTag(tweet:String) = 
  {
    var newTweet = tweet
    var listHashTags:String= ""
    if(tweet.contains("@") || tweet.contains("#"))
    {
      val regexAt = """(\B@\S+)""".r //@            
      newTweet = regexAt.replaceAllIn(tweet, " ")      
      val regexSharp = """(\B#)""".r //#
      newTweet = regexSharp.replaceAllIn(newTweet, " ")
      newTweet = this.deleteBlankSpaces(newTweet) 
      for(word <- tweet.split(" "))
      {
        if(word.startsWith("#"))
        {
          listHashTags += word.replaceFirst("[#]", "")+" "
        }                
      }      
    }
    var retHashTag = if (listHashTags != "") listHashTags else "null"
    (newTweet.trim(), retHashTag.trim())
  }
  
  //method to replace the characters  waitttt   -> wait agaiiin  -> again   
  def replaceRepeatedChars(listWordsTweet: String) : String =  {      
    var newTweet = ""
    val regRepeatedCharacters = """([a-z])\1{2,}""".r
    for(word <- listWordsTweet.split(" "))
    {        
      val matchRegRepChar2 = regRepeatedCharacters.findFirstIn(listWordsTweet)
      if(matchRegRepChar2 != None)
      {
        var wordOneChar   = regRepeatedCharacters.replaceAllIn(word, "$1")
        var wordTworChar  = regRepeatedCharacters.replaceAllIn(word, "$1$1")
        if(Thesaurus.searchTerm(wordTworChar))
        {
          newTweet += wordTworChar+" "
        }else
        {
          newTweet += wordOneChar+" "
        }
       }else
       {
         newTweet += word+" "
       }
    }
    return newTweet
  }
    
  //look up for contractions and replace them for non contracted terms --->  doesn't::does not
  def checkForContractions(tweet: String) : String =
  {
    var tweetWithContractions:String = ""
   
    for (word<- tweet.split(" ")){
      if(listContractionsHMap.contains(word))
      { 
        tweetWithContractions += listContractionsHMap.get(word).mkString(" ":String)+" "        
      }else{        
        tweetWithContractions += word+" "        
      }      
    }
    return tweetWithContractions.trim()
  }
  
  //replace the punctuation marks
  def replacePunctuationMarks(tweet:String) :String = 
  {    
    val punctuationMark ="""[’'\[\]\(\)\{\}\<\>:;,‒–—―‐\-….!«»?¿‘’“”'"/⁄]+""".r
    var tweetAfterRegExpr =  punctuationMark.replaceAllIn(tweet, " ")
    tweetAfterRegExpr = this.deleteBlankSpaces(tweetAfterRegExpr)    
    return tweetAfterRegExpr.trim()    
  }  
  //delete special characters
  def deleteSpecialChars(tweet:String) : String = 
  {
    var tweetAfter = ""
    //(\w+[^A-Za-z0-9\x20]+\w+)|([^A-Za-z0-9\x20]+\w+)|(\w+[^A-Za-z0-9\x20]+)|(\B[^A-Za-z0-9\x20]\B)|(\b_\b)
    val regExprSpecial = """[^a-zA-Z0-9\s:]+""".r
    tweetAfter = regExprSpecial.replaceAllIn(tweet, " ")
    tweetAfter = this.deleteBlankSpaces(tweetAfter)    
    return tweetAfter.trim()
  }
  
  //x27 
  def deleteCharacterX27(listWordsTweet: String) : String =   {
    
     val charPattern = """([\x27])+""".r;
     var tweetString : String = listWordsTweet
     tweetString = charPattern.replaceAllIn(listWordsTweet, " ")
     val regExpBlankSpace = """[\x20]{2,}""".r //blank spaces
     tweetString = regExpBlankSpace.replaceAllIn(tweetString, " ")
     
      // When the tweet is blank    
      if(tweetString.trim().length == 0 )
      {
        tweetString = "blktweet"     
      }
      return tweetString.trim() //return tweetString.toLowerCase().trim()      
  }
  
  //delete words less three chars more eleven
  def deleteWordLessThreeCharsMoreEleven(tweet:String):String=
  {    
    var newTweet:String = ""
    for(word <- tweet.split(" "))
    {      
      if( word.trim().length() >= 3 && word.trim().length() <= 11)
      {
        newTweet += word.trim()+" "
      }
    }
    return newTweet.trim()
  }
  
  //Delete words that have less than 3 characters
  def deleteThreeCharsWords(tweet:String):String =
  {
    var newTweet:String=""    
    for (word <- tweet.split(" "))
    {
      if( word.trim().length() >= 3)
      {
        newTweet += word.trim()+" "
      }      
    }
    if(newTweet.trim().length == 0 )
    {
      newTweet = "blkless3chars_BT"     
    }    
    return newTweet.trim()
  }
  
  def deleteThreeCharsWordsPOS(tweet:String):String =
  {
    var newTweet:String=""
    for (word <- tweet.split(" "))
    {
      var (word_,pos_) =  Utils.splitWord(word, "_")         
      if( word_.trim().length() >= 3)
      {
        newTweet += word.trim()+" "
      }      
    }
    if(newTweet.trim().length == 0 )
    {
      newTweet = "blkless3chars_BT"     
    }
    return newTweet.trim()
  }  
  //deleting tweets that contain less than 3 words
  def deleteTweetsLessThreeWords(tweet:String) : String = 
  {
    var tweetOut ="" 
    var arrayTweet = tweet.split(" ")    
    if(tweet.startsWith("blktweetpreprocessing") || tweet.startsWith("typostweet") ||    tweet.startsWith("blkless3chars") || tweet.startsWith("blklessfreqtweet") || tweet.startsWith("blknegationtweet") || tweet.startsWith("blkstopwordtweet")  || tweet.startsWith("meaninglesstweet"))
    {
      tweetOut = tweet
    }else
    {
      if(arrayTweet.length >= 3)
      {
        tweetOut = tweet
      }else
      {
        tweetOut = "meaninglesstweet_NN"
      } 
    }
    return tweetOut
  }
  
  //removing the word not that is not followed neither by a verb  
  def removeNotAlone(listWordsTweet:String):String = {    
    var tweet:String =""
    for(word <- listWordsTweet.split(" "))
    {      
      var arrayWordParts =  word.split("_")
      if(word.contains("not_") && (arrayWordParts.length == 3))
      {
        tweet += word+" "
        
      }else if (!word.contains("not_"))
      {
        tweet += word+" "
      }      
    }    
    
    if(tweet.trim().length == 0 )
    {
      tweet = "blktweet"     
    }
    return tweet.trim()    
  }
  //removing stop words 
  def removeStopWords(tweet: String) : String =   {    
    val s = System.nanoTime
    var finalString = ""
    for(word <- tweet.split(" "))
    {      
      if(!listStopWordsHMap.contains(word))
      {
        finalString += word+" "        
      }
    }   
    if(finalString.trim().length == 0 )
    {
      finalString = "blktweet"     
    }     
    return finalString.trim()    
  }
  
  def removeStopWordsPOS(tweet: String) : String =   {
    var finalString = ""
    for(word <- tweet.split(" "))
    {
      var (word_,pos) = if(Utils.isNegativeWord(word, "_"))  Utils.splitWordNegative(word, "_")  else  Utils.splitWord(word, "_")
      if(! (listStopWordsHMap.contains(word_)) )
      {
        finalString += word+" "        
      }
    }        
    if(finalString.trim().length == 0 )
    {
      finalString = "blkstopwordtweet_BT"     
    }     
    return finalString.trim()    
  }
  
  //method to load contraction list
  def loadContractionList()  =
  {
    var conf = new Configuration()
    var fileSystem = FileSystem.get(conf)
    var path = new Path(PrParameters.contractionsFile)
    val inputStream = fileSystem.open(path)
       
    var line: String = ""  
    val csv = new BufferedReader(new InputStreamReader(fileSystem.open(path)))
    while ( {(line = csv.readLine()); line != null} )
    {
      if(line != None)
      {
        listContractionsHMap += line.split("::")(0) -> line.split("::")(1)
      }
    }
  }  
  //method to load stopwords in a HashMap     
  def loadStopWords( )  =
  {  
    var conf = new Configuration()
    var fileSystem = FileSystem.get(conf)
    var path = new Path(PrParameters.stopWordsFile)
    val inputStream = fileSystem.open(path)
    var line: String = ""
    val csv = new BufferedReader(new InputStreamReader(fileSystem.open(path)))
    while ( {(line = csv.readLine()); line != null} )
    {
      if(line != None)
      {
        listStopWordsHMap += line -> 1
      }
    }        
  }  
}