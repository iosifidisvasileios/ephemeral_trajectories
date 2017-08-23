package research.twitter.twitterSpark

import com.opencsv.CSVWriter
//import au.com.bytecode.opencsv.CSVWriter
import scala.io.Source
import scala.io.BufferedSource


import java.io.InputStreamReader

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark
import org.apache.spark.util._

/*Slang source  https://www.noslang.com/ 
 */


object SlangsConversion {
  
  var listSlangHMap =  scala.collection.Map[String,String]()
  loadSlangList()
  var SlangsFirstMethod: Int = 0
  var SlangsSecondMethod: Int = 0
  
  //method that search for a slang and converts into the normal form  
  def replaceSlangs(tweet: String, numberSlangs:Int)  =
  {
    var tweetWithSlangs:String = ""
    var listSlangs:String = ""
    for (word<- tweet.split(" ")){
      
      if(listSlangHMap.contains(word))
      {
        var slang:String = listSlangHMap.get(word).mkString(" ":String)        
        tweetWithSlangs += slang+" "
        
        listSlangs += word+" "
        //WriteCSV.writeTextToCSV(word+" -> "+slang, csvSlangFoundWriter)
        numberSlangs  match {
          case 1 => SlangsFirstMethod += 1
          case 2 => SlangsSecondMethod += 1   
        }       
      }else{
        tweetWithSlangs += word+" "        
      }
    }
    var retListSlangs = if(listSlangs != "") listSlangs.trim() else "null"  
    (tweetWithSlangs.trim(), retListSlangs)
  }
  
  
  //Method to load Slang list in a HashMap
  def loadSlangList()  = 
  {
    var conf = new Configuration()
    var fileSystem = FileSystem.get(conf)
    var path = new Path(PrParameters.slangList)
    val inputStream = fileSystem.open(path)
    
    var line: String = ""  
    val csv = new BufferedReader(new InputStreamReader(fileSystem.open(path)))
    while ( {(line = csv.readLine()); line != null} )
    {
      if(line != None)
      {
        listSlangHMap += line.split("::")(0) -> line.split("::")(1)
      }
    }
  }
  
  def getSlangsFirstMethod() :Int =
  {
    return SlangsFirstMethod
  } 
  
  def getSlangsSecondMethod() :Int =
  {
    return SlangsSecondMethod
  }
}