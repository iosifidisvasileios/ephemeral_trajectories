package research.twitter.twitterSpark


import scala.io.Source
import java.util.HashMap
import java.util.Map

/*class to load and interact with the terms that are not part from the lexicon */
class LoadUpdatesLexicon {
  
  var dictionaryUpdateLex: Map[String, Double] = new HashMap[String, Double]()
  var dictionaryUpdateLexPMI: Map[String, Double] = new HashMap[String, Double]()
  
  //AVG
  def LoadUpdateLexicon(sourcePath:String) :Unit = 
  {       
    val csvReaderSource =  Source.fromFile(sourcePath)
    for(line <- csvReaderSource.getLines() )
    { 
      var arrayLine = line.split("\t")
      var word = arrayLine(1).mkString("").toLowerCase()
      var positiveSentiment:Double = arrayLine(2).replaceAll("\"", "").toDouble
      var negativeSentiment:Double = arrayLine(3).replaceAll("\"", "").toDouble
      var averageSentiment:Double = positiveSentiment-negativeSentiment
      dictionaryUpdateLex.put(word,averageSentiment)
    }
    csvReaderSource.close()
  }
  //if a word is contained in the lexicon 
  def containsUpdateLexicon(word:String) :Boolean =
  {
   val containsWord = dictionaryUpdateLex.containsKey(word)
   return containsWord 
  }  
  
  //extract updates to lexicon
  def extractUpdateLexicon(word: String): Double = {    
    var result = 0.0    
    val containsWord = dictionaryUpdateLex.containsKey(word)
    if(containsWord)
    {      
      result = dictionaryUpdateLex.get(word)      
    }    
    return result
  }
  //PMI 
  def LoadUpdateLexiconPmi(sourcePath:String) :Unit = 
  {     
    val csvReaderSource =  Source.fromFile(sourcePath)
    for(line <- csvReaderSource.getLines() )
    {
      var arrayLine = line.split("\t")
      var word = arrayLine(1).mkString("").toLowerCase()
      var averageSentiment:Double = arrayLine(2).replaceAll("\"", "").toDouble
      dictionaryUpdateLexPMI.put(word,averageSentiment)
    }
    csvReaderSource.close()    
  }
  //method to retrieve from updates to lexicon pmi
  def extractUpdateLexiconPmi(word: String): Double = {    
    var result = 0.0    
    val containsWord = dictionaryUpdateLexPMI.containsKey(word)
    if(containsWord)
    {      
      result = dictionaryUpdateLexPMI.get(word)      
    }    
    return result
  }
  //method to check if a term is available in the dictionary update
  def containsUpdateLexiconPmi(word:String) :Boolean =
  {
   val containsWord = dictionaryUpdateLexPMI.containsKey(word)
   return containsWord 
  }
}