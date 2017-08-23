package research.twitter.twitterSpark


import edu.stanford.nlp.tagger.maxent.MaxentTagger
import edu.stanford.nlp.process.PTBTokenizer
import edu.stanford.nlp.ling.CoreLabel
import edu.stanford.nlp.process.CoreLabelTokenFactory._
import java.io.StringReader
import java.io.File
import edu.stanford.nlp.process.CoreLabelTokenFactory
import org.apache.spark.SparkFiles

//import org.apache.hadoop.conf._
//import org.apache.hadoop.fs._
import edu.stanford.nlp.tagger.maxent.TaggerConfig

/*Part of speech tagger object using library 
 * Feature-rich part-of-speech tagging with a cyclic dependency network 
  Toutanova, Kristina and Klein, Dan and Manning, Christopher D and Singer, Yoram*/
object PosTagger {
  
  val partOfSpeechSource = new MaxentTagger(SparkFiles.get(PrParameters.tagModelName)) //Cluster
  //val partOfSpeechSource = new MaxentTagger(ProjectParameters.taggerModelFile)   //Local  
  def partOfSpeech(tweet:String, level:String):String =
  {
    var tweetTagged:String =  level match {
      case "sentence" =>  partOfSpeechSentence(tweet)
      case "word"     =>  partOfSpeechWord(tweet)
    }       
    return tweetTagged.trim()     
  }  
  //Assumption that the method receives a tweet without negation like not_play
  def partOfSpeechSentence(tweet:String) :String =
  {          
    var tweetTagged_ =  partOfSpeechSource.tagTokenizedString(tweet.trim())
    return tweetTagged_
  }
  //method considering Par of speech, taking into account that tweets can have negations  
  def partOfSpeechWord(tweet:String) :String = 
  {
    var tweetTagged = "" 
    for(word <- tweet.split(" "))
    {  
      tweetTagged += partOfSpeechSource.tagString(word)
    }
    return tweetTagged
  }
   
  def checkWord(taggedWord:String, listNot:scala.collection.mutable.MutableList[String]):Boolean =
  {
     var check:Boolean = false
     if(listNot.length > 0)
     {
       for(w <- listNot) //not_play not_like
      { 
        var split = w.split("_")//(0)not(1)word
        if(taggedWord == split(1))
        {
          check = true
        }
       }
     }
    return check
  }
  //Part of speech  
  def partOfSpeechOtherMethods(tweet:String):String =
  {
    var tweetTagged:String = ""
    var tokens = new PTBTokenizer[CoreLabel]( new  StringReader (tweet),new CoreLabelTokenFactory(),"invertible").tokenize()
    var tweetTagged_ =  partOfSpeechSource.tagSentence(tokens)
    tweetTagged_.toString()
    return tweetTagged_.toString() 
  }
}
