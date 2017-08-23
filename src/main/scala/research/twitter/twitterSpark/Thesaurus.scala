package research.twitter.twitterSpark

import scala.collection.mutable.HashMap
import scala.collection.mutable.Map
import scala.io.BufferedSource
import scala.io.Source

import java.io.InputStreamReader

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.spark.SparkContext
import org.apache.spark
import com.opencsv.CSVWriter

/*object to load the Roget's dictionary http://www.roget.org/ */

object Thesaurus {
  
  var thesaurusHMap = scala.collection.Map[String,Int]()
  loadThesaurus()
  //load the dictionary into memory 
  def loadThesaurus() = 
  {
    var conf = new Configuration()
    var fileSystem = FileSystem.get(conf)
    var path = new Path(PrParameters.thesaurusFile)
    val inputStream = fileSystem.open(path)
    var line: String = ""  
    val csv = new BufferedReader(new InputStreamReader(fileSystem.open(path)))
    while ( {(line = csv.readLine()); line != null} )
    {
      if(line != None)
      {
        thesaurusHMap += line.split(",")(0) -> line.split(",")(1).toInt
      }
    }  
  }
  //method that check given a tweet whether it contains less than 3 corrected english spelled words
  def deleteTweetsNonCorrectedWords(tweet:String): String =
  {
    var tweetResult = tweet
    if (tweet != "blktweetpreprocessing")
    {      
      var englishWords =  countWordsContainsThesaurus(tweet)
      if(englishWords >= 2)
      {
        tweetResult = tweet
      }else
      { 
        tweetResult = "typostweet"
      }
    }
    return tweetResult
  }
  
  //count words that are found on the dictionary  
  def countWordsContainsThesaurus(tweet:String):Int = 
  {
    var countThesaurus:Int = 0
    for(word <- tweet.split(" "))
    { 
      if(searchTerm(word))
      { 
        countThesaurus += 1
      }
    }
    return countThesaurus
  }
  //search a term on the dictionary
  def searchTerm(word:String) : Boolean =
  {
    var flag:Boolean = false
    if(thesaurusHMap.contains(word.toLowerCase()))
    {
      flag = true      
    }    
    return flag
  }
}