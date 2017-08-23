package research.twitter.twitterSpark

import scala.io.Source
import java.io.BufferedWriter
import java.io.FileWriter

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.util._
import org.apache.spark.rdd.RDD
/*Method that organizes the preprocessing methods and the other calls  */
object ControllerMethods {   
  //Preprocessing methods calls
  def firstPreprMethods(tweetIn:String) :String =
  { 
    //Expected format
    //285888366476677121	Mon Dec 31 23:21:12 +0000 2012	When someone asks you for logic help but the logic appears messy, wires are everywhere and you have reorganize them. #RAAGE	neutral	null	asks logic logic appears messy wires reorganize rage 
    var tweetSplit:Array[String]  =  Utils.tweetToArray(tweetIn, "\t")    
    var sentimentBigData =  ""
    var tweet = ""
    try {
      sentimentBigData =  tweetSplit(3) //cluster
      tweet = tweetSplit(2) //cluster
    } catch {
      case t: Throwable => t.printStackTrace()
    }    
   
    var sentiment = sentimentBigData  match {
      case "neutral"   => 2
      case "positive"  => 4
      case "negative"  => 0
      case _  => 5
    }  
    //tweet to lower case
    var processedTweet = Preprocess.lowerCaseTweet(tweet)
    //removing blank spacess
    processedTweet = Preprocess.deleteBlankSpaces(processedTweet)
    //remove authors and remove # character
    var (procTweet, listHashTags) = Preprocess.deleteAuthorHashTag(processedTweet)
    processedTweet = procTweet
    //Slangs conversion
    var (procTweetSlang, listSlangs1) = SlangsConversion.replaceSlangs(processedTweet, 1)    
    processedTweet = procTweetSlang
    processedTweet = Preprocess.lowerCaseTweet(processedTweet)
    //Remove special characters like &lsaquo;&amp;&lt;&gt; 
    processedTweet = Preprocess.checkForSpecialHtmlChars(processedTweet)
    //deleting urls
    processedTweet = Preprocess.deleteUrls(processedTweet)
    //deleting re pre characters
    processedTweet = Preprocess.removeRePreCharacteresBegin(processedTweet)
    //deleting numbers
    processedTweet = Preprocess.deleteNumbers(processedTweet)
    //deleting repeated chars
    processedTweet = Preprocess.replaceRepeatedChars(processedTweet)
    //replacing contractions
    processedTweet = Preprocess.checkForContractions(processedTweet)
    processedTweet = Preprocess.lowerCaseTweet(processedTweet)
    //replace puntuations 
    processedTweet = Preprocess.replacePunctuationMarks(processedTweet)
    //removing special characters
    processedTweet = Preprocess.deleteSpecialChars(processedTweet)
    //Slangs second time    
    var (procTweetSlang2, listSlangs2) = SlangsConversion.replaceSlangs(processedTweet, 2)  
    processedTweet = procTweetSlang2     
    processedTweet = Preprocess.lowerCaseTweet(processedTweet)
    //replacing contractions
    //Performing again some of the above steps again
    processedTweet = Preprocess.checkForContractions(processedTweet)
    processedTweet = Preprocess.removeRePreCharacteresBegin(processedTweet)      
    processedTweet = Preprocess.deleteNumbers(processedTweet)
    processedTweet = Preprocess.replaceRepeatedChars(processedTweet)      
    processedTweet = Preprocess.replacePunctuationMarks(processedTweet)     
    processedTweet = Preprocess.deleteSpecialChars(processedTweet)
    
    return "\""+sentiment.toString()+"\",\""+processedTweet+"\",\""+tweet+"\",\""+ listHashTags +"\",\""+ listSlangs1+"\",\""+listSlangs2+"\""    
  }
  
  //method for checking tweets in other languages 
  def secondPreprMethods(tweetIn:String) :String = 
  {    
    var tweetSplit:Array[String]  =  Utils.tweetToArray(tweetIn,"\",\"")
    var sentiment =  tweetSplit(0)
    var tweet = tweetSplit(1)
    val tweetOriginal = tweetSplit(2)
    
    //Check against thesaurus if a tweet contains more than 2 corrected english words    
    var processedTweet = Thesaurus.deleteTweetsNonCorrectedWords(tweet)
    processedTweet = PosTagger.partOfSpeech(processedTweet,"sentence")
    processedTweet = Preprocess.deleteThreeCharsWordsPOS(processedTweet)
    return "\""+sentiment+"\",\""+processedTweet+"\",\""+tweetOriginal+"\",\""+tweetSplit(3)+"\",\""+tweetSplit(4)+"\",\""+tweetSplit(5)+"\""
  }  
  
  //method for checking the frequencies of the words in the dataset
  def checkForFrequecies( rddTweets: RDD[String], sc: SparkContext) :RDD[String] = 
  { 
    val broadcastRddTweets = sc.broadcast(rddTweets)
    val wordFrequencyRDD = Utils.generateDictFromRDD(broadcastRddTweets.value, sc)
    val broadcastHMap = sc.broadcast(wordFrequencyRDD)    
    
    //(0 -> sentiment, 1-> preprocessTweet, 2 -> Orig Tweet,  3->Hash, 4->Slangs1, 5->Slangs2, 6 ->ListNoFreq)
    val tweetThirdPreprocessRDD = broadcastRddTweets.value.map(line => {
      var tweetSplit:Array[String] = Utils.tweetToArray(line,"\",\"")
      var sentiment = tweetSplit(0)
      var tweet = tweetSplit(1)
      val tweetOrig = tweetSplit(2)
      var newTweet:String = ""
      var nonFrequentWords:String = ""
      if(tweet.trim() == "blkless3chars_BT" || tweet.trim().startsWith("blktweetpreprocessing") || tweet.trim().startsWith("meaninglesstweet") || tweet.trim().startsWith("typostweet"))
      {
        newTweet = tweet
           
      }else
      { 
        for(word <- tweet.split(" "))
        {
          var (wordTweet,posTweet) = Utils.splitWord(word, "_")                                                                                              
          var freqWord:Long = 0
          try {
            var wordExistHMap = broadcastHMap.value.get(wordTweet)
            if(wordExistHMap != None)
            {
              var freqWordDict =  broadcastHMap.value.get(wordTweet).get 
              if( freqWordDict >= 10)    
              {
                freqWord = freqWordDict
              }
            }
          } catch {
            case t: Throwable => t.printStackTrace() //handle error
          }
          if(freqWord > 10)
          {
            newTweet += word+" "
          }
          else
          {
            nonFrequentWords+= word+" "
          }
        }
        if(newTweet.trim().length() == 0)
        {
          newTweet = "blklessfreqtweet_BT"
        }
      }
      
      var retNonFrequentWords = if(nonFrequentWords != "") nonFrequentWords else "null"                                      
      "\""+sentiment+"\",\""+newTweet.trim()+"\",\""+tweetOrig+"\",\""+tweetSplit(3)+"\",\""+tweetSplit(4)+"\",\""+tweetSplit(5)+"\",\""+retNonFrequentWords.trim()+"\""  } )
  
    
    return tweetThirdPreprocessRDD
  }
  
  //Loading dictionary files for retrieving  the base form given a word 
  def thirdPreprMethodsRdd (rddTweets:RDD[String], sc:SparkContext) : RDD[String] = 
  { 
    //Loading files for the library
    sc.addFile(PrParameters.adjFile)
    sc.addFile(PrParameters.advFile)
    sc.addFile(PrParameters.dataAdj)
    sc.addFile(PrParameters.dataAdv)
    sc.addFile(PrParameters.dataNoun)
    sc.addFile(PrParameters.dataVerb)
    sc.addFile(PrParameters.indAdj)
    sc.addFile(PrParameters.indAdv)
    sc.addFile(PrParameters.indNoun)
    sc.addFile(PrParameters.indVerb)
    sc.addFile(PrParameters.nounFile)
    sc.addFile(PrParameters.verbFile)
    //(0 -> sentiment, 1-> preprocessTweet, 2 -> Orig Tweet,  3->Hash, 4->Slangs1, 5->Slangs2, 6 ->ListNoFreq)
    val tweetThirdPreprocessRDD = rddTweets.map(line =>{
      var tweetSplit:Array[String]  =  Utils.tweetToArray(line,"\",\"")
      var sentiment =  tweetSplit(0)
      var tweet = tweetSplit(1)
      val tweetOrig = tweetSplit(2)
      var tweetWithBaseForm:String = ""    
      var arrayTweet:Array[String] = tweet.split(" ")
      val posVerbs  = Array( "VBD","VBG","VBN","VBP","VBZ")
      val posNouns = Array("NNS","NNPS")      
      
      for(word <- tweet.split(" "))    
      {        
        val (word_, pos) =  if (Utils.isNegativeWord(word,"_")) Utils.splitWordNegative(word, "_") else Utils.splitWord(word, "_")        
        
        if(posVerbs.contains(pos))
        {
          if(Utils.isNegativeWord(word,"_"))  tweetWithBaseForm += "not_"          
          tweetWithBaseForm += FindBaseForm.getBaseFormVerb(word_) +"_"+ pos+" " 
                    
        }else if(posNouns.contains(pos))
        {
          if(Utils.isNegativeWord(word,"_"))  tweetWithBaseForm += "not_"
          tweetWithBaseForm += FindBaseForm.getSingularNoun(word_) +"_"+pos+" "
        }
        else
        {
          tweetWithBaseForm += word+" "
        }        
      }                          
      var processedTweetBaseForm= Preprocess.deleteThreeCharsWordsPOS(tweetWithBaseForm.trim())
      "\""+sentiment+"\",\""+processedTweetBaseForm.trim()+"\",\""+tweetOrig+"\",\""+tweetSplit(3)+"\",\""+tweetSplit(4)+"\",\""+tweetSplit(5)+"\",\""+tweetSplit(6)+"\""
     
     })     
     return tweetThirdPreprocessRDD
  }
  
  //NER 3 Class 
  def entity3Class(text:String) : String =
  {
    var arrayTweet = Utils.tweetToArray(text,"\",\"")    
    var temporal3class = SetEntityToWords3Class.classifyNounsClass(arrayTweet(1))
    return temporal3class        
  }
  //NER 7 Class
  def entity7Class(text:String) :String = 
  {
    var arrayTweet = Utils.tweetToArray(text,",")
    var temporal7class = SetEntityToWords7Class.classifyNameClass(arrayTweet(1))    
    return temporal7class
  }
  
  
  //Preprocessing method calls 
  def fourthPreprMethods(tweet:String) :String = 
  {
    //(0 -> sentiment, 1-> preprocessTweet, 2 -> Orig Tweet,  3->Hash; 4->Slangs1, 5->Slangs2, 6 ->ListNoFreq)
    
    var tweetSplit:Array[String]  =  Utils.tweetToArray(tweet, "\",\"")
    var sentiment =  tweetSplit(0)
    var tweetContent = tweetSplit(1)
    val tweetOrig = tweetSplit(2)
    //negations
    var processedTweet = ReplaceNegations.checkOtherNegationsPOS(tweetContent)    
    processedTweet = ReplaceNegations.searchNegationsPOS(processedTweet)
    //Stop words removal
    processedTweet = Preprocess.removeStopWordsPOS(processedTweet)
    //deleting blank spaces
    processedTweet = Preprocess.deleteBlankSpacesFinal(processedTweet)
    //removing tweets which have less than 3 words 
    processedTweet = Preprocess.deleteTweetsLessThreeWords(processedTweet)    
    
    return "\""+sentiment+"\",\""+processedTweet+"\",\""+tweetOrig+"\",\""+tweetSplit(3)+"\",\""+tweetSplit(4)+"\",\""+tweetSplit(5)+"\",\""+tweetSplit(6) +"\""   
  }  
  //variables for building the confusion matrix
  var actNoPreNo, actNoPredYes, actNoPredNeu, actNoPredNoSent : Int = 0    
  var actYesPredNo, actYesPredYes, actYesPredNeu, actYesPredNoSent, actYesOth  : Int = 0
  var actNeutPredNo, actNeuPredYes, actNeuPredNeu,actNeuPredNoSent, actNeutOth  : Int = 0
  var ActualNoOthers  : Int = 0
  
  //method calls for assignment sentiment to tweets
  def assignSentiment(tweet:String) :String = 
  {    
    var tweetSplit:Array[String]  =  Utils.tweetToArray(tweet,"\",\"")    
    var sentiment =  tweetSplit(0)
    var tweetContent = tweetSplit(1)
    var tweetOrig = tweetSplit(2)
    //calculating the sentiment
    var processedTweet =  CalculateSentiment.calculateSentiment(tweetContent, sentiment.toInt )
    //converting to data set notation Neutral = 2, No sentiment = 5, Positive = 4, Negative = 0 
    var obtainedSentimentInt = CalculateSentiment.convertSentimentToOrginalNotation(processedTweet)
    //method call for building confusion matrix
    confusionMatrixCustom(sentiment.toInt, obtainedSentimentInt)    
    //( 0-> dataset Sentiment, 1-> sentiwordnet sentiment, 2-> Tweet, 3 -> wordsNoSentImpl, 4-> wordsNoSentOrig, 5 -> WordsNoSentImplDistTokens, 6 -> WordsNoSentOrigDistTokens, 7-> hashTags, 8-> listSlangs1, 9-> listSlangs2, 10 -> ListNoFrequent
    return "\""+sentiment+"\",\""+obtainedSentimentInt.toString+"\",\""+processedTweet(1)+"\",\""+processedTweet(2)+"\",\""+processedTweet(3)+"\",\""+processedTweet(5)+"\",\""+processedTweet(6)+"\",\""+tweetSplit(3)+"\",\""+tweetSplit(4)+"\",\""+tweetSplit(5)+"\",\""+tweetSplit(6)+"\""
  }
    
  //Method for building the confusion matrix   
  def confusionMatrixCustom (sentiment140 : Int, obtainedSentiment : Int) : Unit =   {
    /*0 = negative, 2 = neutral, 4 = positive, 5 = No sentiment*/   
    val sentimentOriginal = sentiment140 match
    { 
      case 0 =>  obtainedSentiment match {
        case 0 => actNoPreNo += 1
        case 4 => actNoPredYes += 1
        case 2 => actNoPredNeu +=1
        case 5 =>  actNoPredNoSent += 1
        case _ => ActualNoOthers += 1
       }        
      case 2 => obtainedSentiment match {
        case 0 => actNeutPredNo += 1
        case 4 => actNeuPredYes += 1
        case 2 => actNeuPredNeu +=1 
        case 5 =>  actNeuPredNoSent += 1
        case _ => actNeutOth += 1
      }
      case 4 => obtainedSentiment match {
        case 0 => actYesPredNo += 1
        case 4 => actYesPredYes += 1
        case 2 => actYesPredNeu +=1 
        case 5 =>  actYesPredNoSent += 1
        case _ => actYesOth += 1
      }
      case _=>  obtainedSentiment match {
        case _ => ActualNoOthers += 1
      }
    }
  }
    
  def getActNoPredNo() : Int =
  {
   return actNoPreNo 
  }
  
  def getActYesPredNo() :Int = 
  {
    return actYesPredNo 
  }
  
  def getActNoPredYes() :Int = 
  {
    return actNoPredYes  
  }
  
  def getActualYesPredYes() :Int = 
  {
    return actYesPredYes 
  } 
  
  def getActualNoPredNeutral() :Int = 
  {
    return actNoPredNeu  
  }
  
  def getActualNoPredNoSent() : Int=
  {
    return actNoPredNoSent 
  } 
  
  def getActualYesPredNeutral() :Int =
  {
    return actYesPredNeu  
  }
  
  def getActualYesPredNoSent() :Int=
  {
    return actYesPredNoSent  
  }
  
  def getActualNoOthers() :Int = 
  {
    return ActualNoOthers 
  }
  
  def getActualYesOthers() :Int =
  {
    return actYesOth
  }   
  
}