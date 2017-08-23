package research.twitter.twitterSpark

import java.io.BufferedWriter
import java.io.FileWriter
import com.opencsv.CSVWriter
//import au.com.bytecode.opencsv.CSVWriter
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map

/*object that has methods to calculate the sentiment using SentiWordNet 3.0: An Enhanced Lexical Resource for Sentiment Analysis and Opinion Mining.},
  Baccianella, Stefano and Esuli, Andrea and Sebastiani, Fabrizio
  
  * */
object CalculateSentiment {
   
  //load the sentiwordNet lexicon in a HashMap
  val sentiWordNetDictionary   = new SentiWordNetScala(PrParameters.sentiWordNetFile)   
  //Method to calculate the sentiment
  def calculateSentiment(tweet : String, originalSentiment:Int) : Array[String] =   {   
    var totalSentiment : Double = 0.0; //Initial Value
    var counter : Int = 0
    var sentenceHasSentiment = false
    var listWordsNoSentiment = new ListBuffer[String]() 
    var listWordsSentiment = new ListBuffer[String]()
    for(word <- tweet.split(" "))
    {         
      //checking whether is it negative
      var isNegativeWd:Boolean = Utils.isNegativeWord(word,"_")              
      // Retrieving from each word the partOSpeech and the word
      var (onlyWord, partOfSpeechSpecific) = if (isNegativeWd) Utils.splitWordNegative(word, "_") else Utils.splitWord(word, "_")        
      //converting specific part of speech into generic "a", "n", "r", "v" and "other" for other 
      var partOfSpeechGeneral : String = Utils.getGeneralPOS(partOfSpeechSpecific)        
      val (wordHasSentiment, partialsSentVal) = partOfSpeechGeneral match {
        case "a"|"r"  => calculateSentimentWordAdjAdv(word, onlyWord, partOfSpeechSpecific, partOfSpeechGeneral,isNegativeWd )    
        case "n"|"v"  => calculateSentimentWordNounVerb(word, onlyWord, partOfSpeechSpecific, partOfSpeechGeneral, isNegativeWd)
        case "other"  => calculateSentimentWordOthers(word, onlyWord, partOfSpeechSpecific, partOfSpeechGeneral, isNegativeWd)
      }
      //calculating the sentiment
      if(wordHasSentiment)
      {
        if(!sentenceHasSentiment)
        {
          sentenceHasSentiment = wordHasSentiment
        }
        counter += 1
        totalSentiment += partialsSentVal          
        listWordsSentiment += word
      }else
      {
        listWordsNoSentiment += word 
      }
    }
    if(totalSentiment != 0.0 && sentenceHasSentiment) 
    {
       totalSentiment = (totalSentiment/counter)
    }else if(!sentenceHasSentiment)
    {
      totalSentiment = 10 //Modify this, It does not look nice
    }
    var wordsNoSentiment, wordsNoSentOrigSentiment = "null"
    var wordsNoSentimentDist, wordsNoSentOrigSentimentDist = "null"
    var wordsSentiment = "null"  
    if( listWordsNoSentiment.length >0)
    { 
      val (wordsNoSentiment_, wordsNoSentOrigSentiment_, wordsNoSentDist_, wordsNoSentOrigSent_) = writePdfWordsNoSentiment(listWordsNoSentiment, totalSentiment, originalSentiment)
      wordsNoSentiment = wordsNoSentiment_
      wordsNoSentOrigSentiment = wordsNoSentOrigSentiment_
      wordsNoSentimentDist = wordsNoSentDist_
      wordsNoSentOrigSentimentDist =   wordsNoSentOrigSent_
    }
    if( listWordsSentiment.length >0)
    {
      wordsSentiment = gatherWordsSent(listWordsSentiment, totalSentiment)
    }
    val arrayResult =  Array(totalSentiment.toString(),tweet, wordsNoSentiment, wordsNoSentOrigSentiment, wordsSentiment, wordsNoSentimentDist, wordsNoSentOrigSentimentDist )
    return arrayResult
  }    
  
  //calculate sentiment for adjectives and adverbs   
  def calculateSentimentWordAdjAdv(completeWord:String, word:String, posSpecific:String, posGeneral:String, isNegation:Boolean)  =
  {      
    var hasSent:Boolean  = false      
    var positSent:Double = 0.0
    var negatSent:Double = 0.0
    var objectSent:Double = 0.0
    var totalSent:Double = 0.0     
    hasSent  = hasSentiment(word, posGeneral)
    
    if(hasSent)
    {
      positSent = getSentiPos(word, posGeneral)
      negatSent = getSentiNeg(word, posGeneral)
      objectSent = getSentiObjectivity(word, posGeneral)        
      totalSent = (positSent - negatSent)
      
      if(Utils.checkSuperlativeComparative(posSpecific))//Just based on POS
      {
        totalSent = boostSuperComparative(totalSent,posSpecific)                    
      }
    }else
    {
      var wordBForm:String = FindBaseForm.getBaseFormAdverbAdjective(completeWord, posSpecific) //return 0 if there does not have BF control
      var hasSentBF:Boolean = hasSentiment(wordBForm, posGeneral)
      if(hasSentBF)
      {
        hasSent = hasSentBF
        positSent = getSentiPos(wordBForm, posGeneral)
        negatSent = getSentiNeg(wordBForm, posGeneral)
        objectSent = getSentiObjectivity(wordBForm, posGeneral)        
        totalSent = (positSent - negatSent)          
        if(Utils.checkSuperlativeComparative(posSpecific))//Just based on what the POS
        {
          totalSent = boostSuperComparative(totalSent,posSpecific)                    
        }
      }
    }
    (hasSent, totalSent)
  }
  //calculate sentiment for noun adn verbs    
  def calculateSentimentWordNounVerb(completeWord:String, word:String, posSpecific:String, posGeneral:String, isNegation:Boolean)  =
  {      
    var hasSent:Boolean  = false      
    var positSent:Double = 0.0
    var negatSent:Double = 0.0
    var objectSent:Double = 0.0
    var totalSent:Double = 0.0      
    hasSent  = hasSentiment(word, posGeneral)
    
    if(hasSent)
    {
      positSent = getSentiPos(word, posGeneral)
      negatSent = getSentiNeg(word, posGeneral)
      objectSent = getSentiObjectivity(word, posGeneral)        
      totalSent = (positSent - negatSent) 
      if(isNegation)
      {
        totalSent = -1 * (positSent - negatSent)
      }        
    }else
    {
      var wordBForm:String = if(posGeneral == "v")  FindBaseForm.getBaseFormVerb(word) else FindBaseForm.getSingularNoun(word,posSpecific)
      var hasSentBF:Boolean = hasSentiment(wordBForm, posGeneral)
      if(hasSentBF)
      {
        hasSent = hasSentBF
        positSent = getSentiPos(wordBForm, posGeneral)
        negatSent = getSentiNeg(wordBForm, posGeneral)
        objectSent = getSentiObjectivity(wordBForm, posGeneral)        
        totalSent = (positSent - negatSent)
        if(isNegation)
        {
          totalSent = -1 * (totalSent)
        }          
      }
    }
    (hasSent, totalSent)
  }
  //calculate sentiment for other words   
  def calculateSentimentWordOthers(completeWord:String, word:String, posSpecific:String, posGeneral:String, isNegation:Boolean)  =
  {      
    var hasSent:Boolean  = false
    var totalSent:Double = 0.0                      
    (hasSent, totalSent)
  }   
    
  //Point Wise Mutual Information Adj and adv 
  def calculateSentimenUpdatePmiAdjAdv(completeWord:String, word:String, posSpecific:String, posGeneral:String, isNegation:Boolean, instUpdLexicons:LoadUpdatesLexicon)  =
  {      
    var hasSentSentiWordNet:Boolean  = false 
    var hasSentLexiconPMI:Boolean    = false
    var positSent:Double = 0.0
    var negatSent:Double = 0.0
    var objectSent:Double = 0.0      
    var totalSentSentiWordNet:Double = 0.0
    var totalSentUpdateLexPmi:Double = 0.0      
    var totalSentAggregated:Double = 0.0
    
    hasSentLexiconPMI  = hasSentimentLexiconPmi(word, instUpdLexicons)
    if(hasSentLexiconPMI)
    {
      totalSentUpdateLexPmi = getSentimentLexiconPmi(word, posGeneral, instUpdLexicons)      
      if(Utils.checkSuperlativeComparative(posSpecific))
      {
        totalSentUpdateLexPmi = boostSuperComparative(totalSentUpdateLexPmi, posSpecific)         
      }       
    }else
    {
      var baseFormAdjAdv = FindBaseForm.getBaseFormAdverbAdjective(word, posSpecific)
      var hasSentLexiconPMIBF:Boolean = hasSentimentLexiconPmi(baseFormAdjAdv,instUpdLexicons)
      if(hasSentLexiconPMIBF)
      {
        hasSentLexiconPMI = hasSentLexiconPMIBF        
        totalSentUpdateLexPmi = getSentimentLexiconPmi(baseFormAdjAdv, posGeneral,instUpdLexicons)      
        if(Utils.checkSuperlativeComparative(posSpecific))
        {
          totalSentUpdateLexPmi = boostSuperComparative(totalSentUpdateLexPmi, posSpecific)         
        }
        
      }else
      {
        hasSentLexiconPMI = false
        totalSentUpdateLexPmi = 0.0        
      }
    }      
    
    hasSentSentiWordNet = hasSentiment(word, posGeneral)
    if(hasSentSentiWordNet)
    {
      positSent = getSentiPos(word, posGeneral)
      negatSent = getSentiNeg(word, posGeneral)
      objectSent = getSentiObjectivity(word, posGeneral)        
      totalSentSentiWordNet = (positSent - negatSent)
      if(Utils.checkSuperlativeComparative(posSpecific))
      {
        totalSentSentiWordNet = boostSuperComparative(totalSentSentiWordNet, posSpecific)         
      }
    }else
    {
      var baseFormAdvAdj = FindBaseForm.getBaseFormAdverbAdjective(word, posSpecific)
      var hasSentSentiWordNetBF:Boolean = hasSentiment(baseFormAdvAdj, posGeneral)
      if(hasSentSentiWordNetBF)
      {
        hasSentSentiWordNet = hasSentSentiWordNetBF
        positSent = getSentiPos(baseFormAdvAdj, posGeneral)
        negatSent = getSentiNeg(baseFormAdvAdj, posGeneral)
        objectSent = getSentiObjectivity(baseFormAdvAdj, posGeneral)          
        totalSentSentiWordNet = (positSent - negatSent) 
        if(Utils.checkSuperlativeComparative(posSpecific))
        {
          totalSentSentiWordNet = boostSuperComparative(totalSentSentiWordNet, posSpecific)
        }
      }else
      {
        hasSentSentiWordNet = false
        totalSentSentiWordNet = 0.0
      }
    }
    
    totalSentAggregated = aggrSentiValsAvg(totalSentSentiWordNet, totalSentUpdateLexPmi)      
    (hasSentSentiWordNet, hasSentLexiconPMI, totalSentAggregated)       
  }
    
  //Calculating sentiment values using Point Wise Mutual for Noun Verbs
  def calculateSentimenUpdatePmiNounVerb(completeWord:String, word:String, posSpecific:String, posGeneral:String, isNegation:Boolean, instUpdLexicons:LoadUpdatesLexicon)  =
  {      
    var hasSentSentiWordNet:Boolean  = false 
    var hasSentLexiconPMI:Boolean    = false
    var positSent:Double = 0.0
    var negatSent:Double = 0.0
    var objectSent:Double = 0.0      
    var totalSentSentiWordNet:Double = 0.0
    var totalSentUpdateLexPmi:Double = 0.0      
    var totalSentAggregated:Double = 0.0
    
    hasSentLexiconPMI  = hasSentimentLexiconPmi(word,instUpdLexicons)
    if(hasSentLexiconPMI)
    {    
      totalSentUpdateLexPmi = getSentimentLexiconPmi(word, posGeneral,instUpdLexicons)      
      if(isNegation)
      {
        totalSentUpdateLexPmi = (-1)*totalSentUpdateLexPmi
      }      
      
    }else
    {
      var baseFormVerbNoun = FindBaseForm.getBaseFormVerb(word)
      var hasSentLexiconPMIBF:Boolean = hasSentimentLexiconPmi(baseFormVerbNoun, instUpdLexicons)
      if(hasSentLexiconPMIBF)
      {
        hasSentLexiconPMI = hasSentLexiconPMIBF        
        totalSentUpdateLexPmi = getSentimentLexiconPmi(baseFormVerbNoun, posGeneral, instUpdLexicons)      
        if(isNegation)
        {
          totalSentUpdateLexPmi = (-1)*totalSentUpdateLexPmi
        } 
        
      }else
      {
        hasSentLexiconPMI = false
        totalSentUpdateLexPmi = 0.0        
      }
    }
    //Checking on SentiWordNet
    hasSentSentiWordNet = hasSentiment(word, posGeneral)
    if(hasSentSentiWordNet)
    {
      positSent = getSentiPos(word, posGeneral)
      negatSent = getSentiNeg(word, posGeneral)
      objectSent = getSentiObjectivity(word, posGeneral)        
      totalSentSentiWordNet = (positSent - negatSent)
      if(isNegation)
      {
        totalSentSentiWordNet = (-1)*totalSentSentiWordNet         
      }
    }else
    {        
      var baseFormNounVerb = FindBaseForm.getBaseFormVerb(word)
      var hasSentSentiWordNetBF:Boolean = hasSentiment(baseFormNounVerb, posGeneral)
      if(hasSentSentiWordNetBF)
      {
        hasSentSentiWordNet = hasSentSentiWordNetBF
        positSent = getSentiPos(baseFormNounVerb, posGeneral)
        negatSent = getSentiNeg(baseFormNounVerb, posGeneral)
        objectSent = getSentiObjectivity(baseFormNounVerb, posGeneral)          
        totalSentSentiWordNet = (positSent - negatSent) 
        if(isNegation)
        {
          totalSentSentiWordNet = (-1)*totalSentSentiWordNet
        }
      }else
      {
        hasSentSentiWordNet = false
        totalSentSentiWordNet = 0.0
      }
    }
    
    totalSentAggregated = aggrSentiValsAvg(totalSentSentiWordNet, totalSentUpdateLexPmi)      
    (hasSentSentiWordNet, hasSentLexiconPMI, totalSentAggregated)       
  }
  //Calculating sentiment values using Point Wise Mutual Information Others
  def calculateSentimenUpdatePmiOthers(completeWord:String, word:String, posSpecific:String, posGeneral:String, isNegation:Boolean, instUpdLexicons:LoadUpdatesLexicon)  =
  {      
    var hasSentSentiWordNet:Boolean  = false 
    var hasSentLexiconPMI:Boolean    = false
    var totalSentSentiWordNet:Double = 0.0
    var totalSentUpdateLexPmi:Double = 0.0      
    var totalSentAggregated:Double = 0.0
    hasSentLexiconPMI  = hasSentimentLexiconPmi(word, instUpdLexicons)
    if(hasSentLexiconPMI)
    {   
      totalSentUpdateLexPmi = getSentimentLexiconPmi(word, posGeneral, instUpdLexicons)      
      if(isNegation)
      {
        totalSentUpdateLexPmi = (-1)*totalSentUpdateLexPmi
      }
    }else
    {        
      hasSentLexiconPMI = false
      totalSentUpdateLexPmi = 0.0
    }
    hasSentSentiWordNet = false
    totalSentSentiWordNet = 0.0
    
    totalSentAggregated = aggrSentiValsAvg(totalSentSentiWordNet, totalSentUpdateLexPmi)      
    (hasSentSentiWordNet, hasSentLexiconPMI, totalSentAggregated)       
  }
    
  //Calculating sentiment values using Average    
  def calculateSentimenUpdateAVGAdjAdv(completeWord:String, word:String, posSpecific:String, posGeneral:String, isNegation:Boolean, instUpdLexicons:LoadUpdatesLexicon)  =
  {      
    var hasSentSentiWordNet:Boolean  = false 
    var hasSentLexiconAVG:Boolean    = false
    var positSent:Double = 0.0
    var negatSent:Double = 0.0
    var objectSent:Double = 0.0      
    var totalSentSentiWordNet:Double = 0.0
    var totalSentUpdateLexAVG:Double = 0.0      
    var totalSentAggregated:Double = 0.0
    
    hasSentLexiconAVG  = hasSentimentLexicon(word, instUpdLexicons)
    if(hasSentLexiconAVG)
    {
      totalSentUpdateLexAVG = getSentimentLexicon(word, posGeneral, instUpdLexicons)      
      if(Utils.checkSuperlativeComparative(posSpecific))
      {
        totalSentUpdateLexAVG = boostSuperComparative(totalSentUpdateLexAVG, posSpecific)         
      }       
    }else
    {
      var baseFormAdjAdv = FindBaseForm.getBaseFormAdverbAdjective(word, posSpecific)
      var hasSentLexiconAVGBF:Boolean = hasSentimentLexiconPmi(baseFormAdjAdv, instUpdLexicons)
      if(hasSentLexiconAVGBF)
      {
        hasSentLexiconAVG = hasSentLexiconAVGBF        
        totalSentUpdateLexAVG = getSentimentLexicon(baseFormAdjAdv, posGeneral, instUpdLexicons)      
        if(Utils.checkSuperlativeComparative(posSpecific))
        {
          totalSentUpdateLexAVG = boostSuperComparative(totalSentUpdateLexAVG, posSpecific)         
        }
        
      }else
      {
        hasSentLexiconAVG = false
        totalSentUpdateLexAVG = 0.0        
      }
    }      
    
    hasSentSentiWordNet = hasSentiment(word, posGeneral)
    if(hasSentSentiWordNet)
    {
      positSent = getSentiPos(word, posGeneral)
      negatSent = getSentiNeg(word, posGeneral)
      objectSent = getSentiObjectivity(word, posGeneral)        
      totalSentSentiWordNet = (positSent - negatSent)
      if(Utils.checkSuperlativeComparative(posSpecific))
      {
        totalSentSentiWordNet = boostSuperComparative(totalSentSentiWordNet, posSpecific)         
      }
    }else
    {
      var baseFormAdvAdj = FindBaseForm.getBaseFormAdverbAdjective(word, posSpecific)
      var hasSentSentiWordNetBF:Boolean = hasSentiment(baseFormAdvAdj, posGeneral)
      if(hasSentSentiWordNetBF)
      {
        hasSentSentiWordNet = hasSentSentiWordNetBF
        positSent = getSentiPos(baseFormAdvAdj, posGeneral)
        negatSent = getSentiNeg(baseFormAdvAdj, posGeneral)
        objectSent = getSentiObjectivity(baseFormAdvAdj, posGeneral)          
        totalSentSentiWordNet = (positSent - negatSent) 
        if(Utils.checkSuperlativeComparative(posSpecific))
        {
          totalSentSentiWordNet = boostSuperComparative(totalSentSentiWordNet, posSpecific)
        }
      }else
      {
        hasSentSentiWordNet = false
        totalSentSentiWordNet = 0.0
      }
    }      
    totalSentAggregated = aggrSentiValsAvg(totalSentSentiWordNet, totalSentUpdateLexAVG)    
    (hasSentSentiWordNet, hasSentLexiconAVG, totalSentAggregated) 
  }
    
  //Calculating sentiment values using AVG Noun and Verbs
  def calculateSentimenUpdateAVGNounVerb(completeWord:String, word:String, posSpecific:String, posGeneral:String, isNegation:Boolean, instUpdLexicons:LoadUpdatesLexicon)  =
  {      
    var hasSentSentiWordNet:Boolean  = false 
    var hasSentLexiconAVG:Boolean    = false
    var positSent:Double = 0.0
    var negatSent:Double = 0.0
    var objectSent:Double = 0.0      
    var totalSentSentiWordNet:Double = 0.0
    var totalSentUpdateLexAvg:Double = 0.0      
    var totalSentAggregated:Double = 0.0
    
    hasSentLexiconAVG  = hasSentimentLexicon(word,instUpdLexicons)
    if(hasSentLexiconAVG)
    {    
      totalSentUpdateLexAvg = getSentimentLexicon(word, posGeneral,instUpdLexicons)      
      if(isNegation)
      {
        totalSentUpdateLexAvg = (-1)*totalSentUpdateLexAvg
      }      
      
    }else
    {
      var baseFormVerbNoun = FindBaseForm.getBaseFormVerb(word)
      var hasSentLexiconAVGBF:Boolean = hasSentimentLexicon(baseFormVerbNoun, instUpdLexicons)
      if(hasSentLexiconAVGBF)
      {
        hasSentLexiconAVG = hasSentLexiconAVGBF        
        totalSentUpdateLexAvg = getSentimentLexicon(baseFormVerbNoun, posGeneral, instUpdLexicons)      
        if(isNegation)
        {
          totalSentUpdateLexAvg = (-1)*totalSentUpdateLexAvg
        } 
        
      }else
      {
        hasSentLexiconAVG = false
        totalSentUpdateLexAvg = 0.0        
      }
    }
    
    //Checking on SentiWordNet
    hasSentSentiWordNet = hasSentiment(word, posGeneral)
    if(hasSentSentiWordNet)
    {
      positSent = getSentiPos(word, posGeneral)
      negatSent = getSentiNeg(word, posGeneral)
      objectSent = getSentiObjectivity(word, posGeneral)        
      totalSentSentiWordNet = (positSent - negatSent)
      if(isNegation)
      {
        totalSentSentiWordNet = (-1)*totalSentSentiWordNet         
      }
    }else
    {        
      var baseFormNounVerb = FindBaseForm.getBaseFormVerb(word)
      var hasSentSentiWordNetBF:Boolean = hasSentiment(baseFormNounVerb, posGeneral)
      if(hasSentSentiWordNetBF)
      {
        hasSentSentiWordNet = hasSentSentiWordNetBF
        positSent = getSentiPos(baseFormNounVerb, posGeneral)
        negatSent = getSentiNeg(baseFormNounVerb, posGeneral)
        objectSent = getSentiObjectivity(baseFormNounVerb, posGeneral)          
        totalSentSentiWordNet = (positSent - negatSent) 
        if(isNegation)
        {
          totalSentSentiWordNet = (-1)*totalSentSentiWordNet
        }
      }else
      {
        hasSentSentiWordNet = false
        totalSentSentiWordNet = 0.0
      }
    }
    
    totalSentAggregated = aggrSentiValsAvg(totalSentSentiWordNet, totalSentUpdateLexAvg)      
    (hasSentSentiWordNet, hasSentLexiconAVG, totalSentAggregated)       
  }
  
  //Calculating sentiment values using AVG Others
  def calculateSentimenUpdateAVGOthers(completeWord:String, word:String, posSpecific:String, posGeneral:String, isNegation:Boolean, instUpdLexicons:LoadUpdatesLexicon)  =
  {      
    var hasSentSentiWordNet:Boolean  = false 
    var hasSentLexiconAVG:Boolean    = false
          
    var totalSentSentiWordNet:Double = 0.0
    var totalSentUpdateLexAVG:Double = 0.0      
    var totalSentAggregated:Double = 0.0
    
    hasSentLexiconAVG  = hasSentimentLexicon(word,instUpdLexicons)
    if(hasSentLexiconAVG)
    {   
      totalSentUpdateLexAVG = getSentimentLexicon(word, posGeneral, instUpdLexicons)      
      if(isNegation)
      {
        totalSentUpdateLexAVG = (-1)*totalSentUpdateLexAVG
      }
    }else
    {        
      hasSentLexiconAVG = false
      totalSentUpdateLexAVG = 0.0
    }
    hasSentSentiWordNet = false
    totalSentSentiWordNet = 0.0
    
    totalSentAggregated = aggrSentiValsAvg(totalSentSentiWordNet, totalSentUpdateLexAVG)      
    (hasSentSentiWordNet, hasSentLexiconAVG, totalSentAggregated)       
  }
    
    
              
  def writePdfWordsNoSentiment(listWordsNoSentiment:ListBuffer[String], totalSentiment:Double, originalSentiment:Double )  = 
  {
     var wordsNoSentiment:String = ""
     var wordsOrigSentiment:String = ""
     var wordsNoSentimentDist:String = ""
     var wordsOrigSentimentDist:String = ""
     for(wordList <- listWordsNoSentiment) //not_word_POS
     {
       var (word:String, pos:String) =  if(Utils.isNegativeWord(wordList,"_")) Utils.splitWordNegative(wordList, "_") else Utils.splitWord(wordList, "_")
       
       
       
       totalSentiment match {
         case 0.0 =>      wordsNoSentiment +=word+"_"+pos+"::"+2+" "    // WriteCSV.writeTextToCSV(word+"_"+pos+"::"+2,csvWrtNew) // equal to zero  neutral           
         case x if x > 0 & x < 10 =>  wordsNoSentiment += word+"_"+pos+"::"+4+" "//WriteCSV.writeTextToCSV(word+"_"+pos+"::"+4,csvWrtNew) // greater than zero possitive 
         case x if x < 0.0 => wordsNoSentiment += word+"_"+pos+"::"+0+" " //WriteCSV.writeTextToCSV(word+"_"+pos+"::"+0,csvWrtNew) // less than zero negative 
         case 10.0 => wordsNoSentiment += word+"_"+pos+"::"+5+" " //WriteCSV.writeTextToCSV(word+"_"+pos+"::"+5,csvWrtNew)  // No sentiment
         case _ => wordsNoSentiment += word+"_"+pos+"::"+10+" "//WriteCSV.writeTextToCSV(word+"_"+pos+"::"+10,csvWrtNew)  // 10 Error
       }
       
       originalSentiment  match {
         case 0 =>  wordsOrigSentiment += word+"_"+pos+"::"+0+" " //WriteCSV.writeTextToCSV(word+"_"+pos+"::"+0, csvWrtOr) //0 in the data set denotes Negative Sentiment
         case 4 => wordsOrigSentiment += word+"_"+pos+"::"+4+" " //WriteCSV.writeTextToCSV(word+"_"+pos+"::"+4, csvWrtOr) //4 int the data set denotes Possitive Sentiment
         case 2 => wordsOrigSentiment += word+"_"+pos+"::"+2+" "
         case _ => wordsOrigSentiment += word+"_"+pos+"::"+10+" "//WriteCSV.writeTextToCSV(word+"_"+pos+"::"+10, csvWrtOr) //10 Error the data set denotes Possitive Sentiment
       }         
     }
     val listWordsNoSentAllTuple = ListBuffer[(String,String)]()
     for(wordList <- listWordsNoSentiment)
     {
       var (word:String, pos:String) =  if(Utils.isNegativeWord(wordList,"_")) Utils.splitWordNegative(wordList, "_") else Utils.splitWord(wordList, "_")
       listWordsNoSentAllTuple += ((word,pos))
     }
     var combinedList = listWordsNoSentAllTuple.groupBy(word => word._1 ).map(word => (word._1, listUnion(word._2)) )
     for ((word,posComb) <- combinedList)
     {
        totalSentiment match {
         case 0.0 =>      wordsNoSentimentDist +=word+"_"+posComb+"::"+2+" "            
         case x if x > 0 & x < 10 =>  wordsNoSentimentDist += word+"_"+posComb+"::"+4+" " 
         case x if x < 0.0 => wordsNoSentimentDist += word+"_"+posComb+"::"+0+" "   
         case 10.0 => wordsNoSentimentDist += word+"_"+posComb+"::"+5+" " 
         case _ => wordsNoSentimentDist += word+"_"+posComb+"::"+10+" " 
       }
       
       originalSentiment  match {
         case 0 =>  wordsOrigSentimentDist += word+"_"+posComb+"::"+0+" " 
         case 4 => wordsOrigSentimentDist += word+"_"+posComb+"::"+4+" "  
         case 2 => wordsOrigSentimentDist += word+"_"+posComb+"::"+2+" "
         case _ => wordsOrigSentimentDist += word+"_"+posComb+"::"+10+" "
       }
     }     
     (wordsNoSentiment.trim(), wordsOrigSentiment.trim(), wordsNoSentimentDist.trim(), wordsOrigSentimentDist.trim())
  }
  
  
  def listUnion(listBuf:ListBuffer[(String, String)]) :String  =
  {
    var retString = ""
    var sizeList = listBuf.size
    var count:Int = 0
    for(word <- listBuf)
    {  
       if(count == (sizeList - 1))
       {
         retString += word._2
       }else
       {
         retString += word._2+"/"  
       }
       count += 1
    }   
    return  "<"+retString+">"    
  }
  
  def gatherWordsSent(listWordsSentiment:ListBuffer[String],totalSentiment:Double ) :String  =
  {
    var wordsSentiment = ""    
    for(wordList <- listWordsSentiment)
    {
      var(word:String, pos:String) = if(Utils.isNegativeWord(wordList,"_")) Utils.splitWordNegative(wordList, "_") else Utils.splitWord(wordList, "_")                
      totalSentiment match {
         case 0.0 =>  wordsSentiment += word+"_"+pos+"::"+2+" " 
         case x if x > 0 =>  wordsSentiment += word+"_"+pos+"::"+4+" "  
         case x if x < 0 => wordsSentiment += word+"_"+pos+"::"+0+" "  
         case 10.0 => wordsSentiment += word+"_"+pos+"::"+5+" " 
         case _ => wordsSentiment += word+"_"+pos+"::"+10+" "
      }         
     }    
    return wordsSentiment.trim()
  }        
  
  //Updates to the lexicon
  def calculateSentimentUpdateLexicon(instUpdLexicon:LoadUpdatesLexicon, listWords : String, csvResultWriter:CSVWriter, csvNewWordsWriter:CSVWriter, csvNegSentimentWrt:CSVWriter, csvNegWithSentWrt:CSVWriter, csvWordsSentimentWriter:CSVWriter, originalSentiment:Int, csvNewWordsWriterOrigSent:CSVWriter) : Array[String] =   {
    var sentimentSet:Boolean = false      
    var counter : Int = 0
    var totalSentiment:Double = 0.0
    var listWordsNoSentiment = new ListBuffer[String]() 
    var listWordsSentiment = new ListBuffer[String]()
    
    for(word <- listWords.split(" "))
    {
      // nice_JJ -> "a" for sending the Sentiment Lexicon
      var isNegation:Boolean = Utils.isNegativeWord(word,"_")
      val (onlyWord:String, posSpecific:String) = if (isNegation) Utils.splitWordNegative(word, "_") else Utils.splitWord(word, "_")
      //"a" adjectives, "n" nouns, "v" verbs and "r" adverbs and other
      var partOSGeneric : String = Utils.getGeneralPOS(posSpecific)
      var (hasSentSentiWordnet, hasSentUpdateLexicon, totalSentAggregated) = partOSGeneric  match {
        case "a"|"r" => calculateSentimenUpdateAVGAdjAdv(word, onlyWord, posSpecific, partOSGeneric, isNegation, instUpdLexicon)
        case "v"|"n" =>  calculateSentimenUpdateAVGNounVerb(word, onlyWord, posSpecific, partOSGeneric, isNegation, instUpdLexicon) 
        case "other" =>  calculateSentimenUpdateAVGOthers(word, onlyWord, posSpecific, partOSGeneric, isNegation, instUpdLexicon)
      }        
      //both true or only one       
      if(hasSentSentiWordnet || hasSentUpdateLexicon)
      {
        counter += 1
        if(!sentimentSet)
        {
          sentimentSet = true //(hasSentSentiWordnet || hasSentUpdateLexicon)
        }
        totalSentiment += totalSentAggregated          
      }else
      {
        listWordsNoSentiment += word
      }
    }
    
    if(sentimentSet && totalSentiment != 0.0)
    {
      totalSentiment = (totalSentiment/counter)
    }else
    {
      totalSentiment = 10 //Modify this, It does not look nice
    }
    writePdfWordsNoSentiment(listWordsNoSentiment, totalSentiment, originalSentiment)
    generateCsvResults(originalSentiment, totalSentiment, listWords,csvResultWriter, sentimentSet) //csvResultWriter:CSVWriter
    val arrayResult =  Array(totalSentiment.toString(),listWords)
    return arrayResult
  }
                
  // Load updates lexicon PMI    
  def loadUpdLexiconPmi() : LoadUpdatesLexicon=
  {
    var instUpdLexicon = new LoadUpdatesLexicon()
    instUpdLexicon.LoadUpdateLexiconPmi(PrParameters.pmiLexicon)
    return instUpdLexicon
  }
  
  // Load updates lexicon AVG
  def loadUpdLexicon() : LoadUpdatesLexicon=
  {
    var instUpdLexicon = new LoadUpdatesLexicon()
    instUpdLexicon.LoadUpdateLexicon(PrParameters.avgLexicon)
    return instUpdLexicon
  }
    
    
  //calculate sentiment considering the update using PMI    
  def calculateSentimentUpdateLexiconPmi(instUpdLexiconsPmi:LoadUpdatesLexicon, listWords : String, csvResultWriter:CSVWriter, csvNewWordsWriter:CSVWriter, csvNegSentimentWrt:CSVWriter, csvNegWithSentWrt:CSVWriter, csvWordsSentimentWriter:CSVWriter, originalSentiment:Int, csvNewWordsWriterOrigSent:CSVWriter) : Array[String] =   {
    var sentimentSet:Boolean = false      
    var counter : Int = 0
    var totalSentiment:Double = 0.0
    var listWordsNoSentiment = new ListBuffer[String]() 
    var listWordsSentiment = new ListBuffer[String]()      
    
    for(word <- listWords.split(" "))
    {
      var isNegation:Boolean = Utils.isNegativeWord(word,"_")
      var(wordOnly:String, partOSSpecific:String) = if(isNegation) Utils.splitWordNegative(word, "_") else Utils.splitWord(word, "_")               
      var partOfSGeneral : String = Utils.getGeneralPOS(partOSSpecific)
      
      var (hasSentSentiWordnet, hasSentUpdateLexicon, totalSentAggregated) = partOfSGeneral  match {
        case "a" | "r" => calculateSentimenUpdatePmiAdjAdv(word, wordOnly, partOSSpecific, partOfSGeneral, isNegation, instUpdLexiconsPmi)
        case "v" | "n" => calculateSentimenUpdatePmiNounVerb(word, wordOnly, partOSSpecific, partOfSGeneral, isNegation, instUpdLexiconsPmi)
        case "other"   => calculateSentimenUpdatePmiOthers(word, wordOnly, partOSSpecific, partOfSGeneral, isNegation, instUpdLexiconsPmi)
      }
      
      if(hasSentSentiWordnet || hasSentUpdateLexicon)
      {
        counter += 1
        if(!sentimentSet)
        {
          sentimentSet = true
        }
        totalSentiment += totalSentAggregated
        
      }else
      {
        listWordsNoSentiment += word
      }    
    }
    if(sentimentSet && totalSentiment != 0.0)
    {
      totalSentiment = (totalSentiment/counter)
    }else
    {
      totalSentiment = 10 
    }
    //Words that do not have sentiment    
    writePdfWordsNoSentiment(listWordsNoSentiment, totalSentiment, originalSentiment)    
    generateCsvResults(originalSentiment, totalSentiment, listWords,csvResultWriter,sentimentSet) //csvResultWriter:CSVWriter
    val arrayResult =  Array(totalSentiment.toString(),listWords)
    return arrayResult
  }
  
    
    
    
  def hasSentiment(word:String, partOfSpeechLex:String) :Boolean =
  {
    return sentiWordNetDictionary.contains(word, partOfSpeechLex)
  }
    
   
    
    
  def getSentiPos(word:String, partOfSpeechLex:String) :Double = {      
    var sentiment:Double = sentiWordNetDictionary.extractPos(word.toLowerCase(), partOfSpeechLex)
    return sentiment
  }
  
  def getSentiNeg(word:String, partOfSpeechLex:String) :Double = {      
    var sentiment:Double = sentiWordNetDictionary.extractNeg(word.toLowerCase(), partOfSpeechLex)
    return sentiment
  }
  
  def getSentiObjectivity(word:String, partOfSpeechLex:String) :Double = {      
    var sentiment:Double = sentiWordNetDictionary.extractObjectivity(word.toLowerCase(), partOfSpeechLex)
    return sentiment
  }
    
  //Aggregate sentiment values with the SentiWordNet and DomainLexicon
  def aggrSentiValsAvg(sentWordNetVal:Double, sentDomLex:Double) : Double=
  {
    val tuple = (sentWordNetVal, sentDomLex)
    var aggregateValue:Double = 0.0
    tuple match 
    {
      case (sentiWordVal,sentDomLexVal) if sentiWordVal == 0 && sentDomLexVal == 0  => aggregateValue = 0
      case (sentiWordVal,sentDomLexVal) if sentiWordVal == 0 && sentDomLexVal != 0 => aggregateValue = sentDomLexVal
      case (sentiWordVal,sentDomLexVal) if sentiWordVal != 0 && sentDomLexVal == 0 => aggregateValue = sentiWordVal
      case (sentiWordVal,sentDomLexVal) if sentiWordVal != 0 && sentDomLexVal != 0 => aggregateValue = aggregateOperations(sentiWordVal, sentDomLexVal)
      case _ => aggregateValue = 0.0
    }      
    return aggregateValue
  }
  
  def aggregateOperations(sentiWordVal:Double, sentDomLexVal:Double) :Double = 
  {
    val alpha = 0.5
    var aggregateValue = alpha*sentiWordVal + ((1-alpha)* sentDomLexVal)
    return aggregateValue      
  }
  
    
  def hasSentimentLexicon(word:String, instUpdLexicons:LoadUpdatesLexicon) :Boolean =
  {
    return instUpdLexicons.containsUpdateLexicon(word)
  }
    
    
  /*extractUpdateLexicon*/
  
  def getSentimentLexicon(word:String, partOfSpeechLex:String, instUpdLexicons:LoadUpdatesLexicon) :Double = {
    //not_play_VBG 0 1 2  or  play_VBG 0 1
    var sentiment:Double = 0.0
    var wordArray:Array[String] = word.split("_")  // not_like_JJ_PERSON   S=4  || like_JJ_PERSON S=3 
    
    if(word.startsWith("not_"))
    {
      sentiment = instUpdLexicons.extractUpdateLexicon(wordArray(1).toLowerCase())
      sentiment = -1.0*sentiment
    }else
    {
      
      sentiment = instUpdLexicons.extractUpdateLexicon(wordArray(0).toLowerCase())
    }      
    
    return sentiment
  }
  
    
  def hasSentimentLexiconPmi(word:String, instUpdLexicons:LoadUpdatesLexicon) :Boolean =
  {
    return instUpdLexicons.containsUpdateLexiconPmi(word)
  }
    
    
  def getSentimentLexiconPmi(word:String, partOfSpeechGeneral:String, instUpdLexicons:LoadUpdatesLexicon) :Double = {
    //not_play_VBG 0 1 2  or  play_VBG 0 1
    var sentiment:Double = 0.0
    var wordArray:Array[String] = word.split("_")  // not_like_JJ_PERSON   S=4  || like_JJ_PERSON S=3 
    
    if(word.startsWith("not_"))
    {
      sentiment = instUpdLexicons.extractUpdateLexiconPmi(wordArray(1).toLowerCase())
      sentiment = -1.0*sentiment
    }else
    {
      
      sentiment = instUpdLexicons.extractUpdateLexiconPmi(wordArray(0).toLowerCase())
    }      
    
    return sentiment
  }
      
  //generating the results  
  def generateCsvResults(originalSentiment:Int, sentimentValue: Double,  tweet: String, csvResultWriter:CSVWriter, sentHasSenti:Boolean) : Unit ={
    
    //the polarity of the tweet (0 = negative, 2 = neutral, 4 = positive, 10 = no sentiment )
    if(sentHasSenti)
    {
      sentimentValue match {
        case x if x == 0.0 => WriteCSV.writeArrayToCSV(Array(originalSentiment.toString(), "2",tweet),csvResultWriter)
        case x if x > 0.0  => WriteCSV.writeArrayToCSV(Array(originalSentiment.toString(), "4",tweet), csvResultWriter) 
        case x if x < 0.0  => WriteCSV.writeArrayToCSV(Array(originalSentiment.toString(), "0",tweet),csvResultWriter)
        case _             => WriteCSV.writeArrayToCSV(Array(originalSentiment.toString(), "error",tweet),csvResultWriter)
      }
      
    }else
    { 
      WriteCSV.writeArrayToCSV(Array(originalSentiment.toString(), "5",tweet),csvResultWriter)
    }   
  }
  
  //set sentiment according to the notation of the original data set (0 = negative, 2 = neutral, 4 = positive) for comparison
  def convertSentimentToOrginalNotation(arraySentiment : Array[String]) : Int =
  { 
    val obtainedSentiment = arraySentiment(0).replace("\"", "").trim().toDouble
    var obtainedSentimentInt : Int = 1000       
    obtainedSentiment match
    {
      case obtainedSentiment if obtainedSentiment == 0  => obtainedSentimentInt = 2
      case obtainedSentiment if obtainedSentiment == 10.0  => obtainedSentimentInt = 5
      case obtainedSentiment if obtainedSentiment > 0.0  => obtainedSentimentInt = 4 
      case obtainedSentiment if obtainedSentiment < 0  => obtainedSentimentInt = 0        
      case _ => obtainedSentimentInt = 100
    }
    return obtainedSentimentInt   
  }
  
  def boostSuperComparative(sentimentValue:Double, partOfSpeechSpecific:String) :Double = 
  {
    var boostSentiment:Double = sentimentValue
    val factorCompar:Double =  PrParameters.compBooster
    val factorSuper:Double =   PrParameters.supBooster
    
    if(partOfSpeechSpecific == "JJR" || partOfSpeechSpecific == "RBR" )
    {
      boostSentiment = sentimentValue+ ((sentimentValue*factorCompar)/100.0)
    }else if(partOfSpeechSpecific == "JJS" || partOfSpeechSpecific == "RBS")
    {
      boostSentiment = sentimentValue+ ((sentimentValue*factorSuper)/100.0)
    }
    return boostSentiment    
  } 
    
}
