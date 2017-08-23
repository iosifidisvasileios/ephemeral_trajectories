package research.twitter.twitterSpark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import org.apache.log4j.Level

/*Main object which calls all the methods to generate the dictionaries that are not part from the lexicon*/
object MainSentiment {  
  
  def main(args:Array[String]): Unit  = 
  {    
    val conf = new SparkConf().setAppName("SAnalysisTwitter")    
    val sc = new SparkContext(conf)
    
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    sc.setLogLevel("ERROR")   
    
    //Gathering the arguments 
    PrParameters.setProjectDataset(args(0))
    PrParameters.setProjectFolder(args(1))
    
    println("Generating labels using SentiWordNEt")
    //ResMonths/201302Feb/    
    var lblTimeStamp = args(1).split("/")(1).replaceAll("[/]", "")
    
    sc.addFile(PrParameters.tagModelFile)
    val initialRDD = sc.textFile(PrParameters.projectDataset)
    val firstPreprRDD = initialRDD.map(line => ControllerMethods.firstPreprMethods(line))
                                  .map(line => ControllerMethods.secondPreprMethods(line))
    //building dictionary frequencies                         
    val secPreprRDD = ControllerMethods.checkForFrequecies(firstPreprRDD,sc)
    val thirdPreprRDD = ControllerMethods.thirdPreprMethodsRdd(secPreprRDD, sc).cache()         
    val fourthPreprRDD = thirdPreprRDD.map(line => ControllerMethods.fourthPreprMethods(line))    
    val sentWordNetRDD = fourthPreprRDD.map( line => ControllerMethods.assignSentiment(line)).cache()             
    
    sentWordNetRDD.saveAsTextFile(PrParameters.projectFolder+lblTimeStamp+"sentimentDataset.csv")
    thirdPreprRDD.unpersist()
    //printing the values of the confusion matrix   
    println("Distant supervision No SentiwordNet No "+ ControllerMethods.getActNoPredNo()) // ActualNoPredictedNo
    println("Distant supervision No SentiwordNet Yes "+ ControllerMethods.getActNoPredYes()) // ActualNoPredictedYes
    println("Distant supervision Yes SentiwordNet No "+ ControllerMethods.getActYesPredNo()) // ActualYesPredictedNo
    println("Distant supervision Yes SentiwordNet Yes " + ControllerMethods.getActualYesPredYes()) // ActualYesPredictedYes)
    println("Distant supervision No SentiwordNet Neutral " + ControllerMethods.getActualNoPredNeutral()) //  ActualNoPrectictedNeutral
    println("Distant supervision No SentiwordNet No Sentiment "+ ControllerMethods.getActualNoPredNoSent())// ActualNoPredictedNoSentiment
    println("Distant supervision Yes SentiwordNet Neutral "+ ControllerMethods.getActualYesPredNeutral()) // ActualYesPredictedNeutral 
    println("Distant supervision Yes SentiwordNet No Sentiement "+ ControllerMethods.getActualYesPredNoSent())//  ActualYesPredictedNoSentiment    
    println("Distant supervision No Others  "+ ControllerMethods.getActualNoOthers())
    println("Distant supervision Yes Others  "+ ControllerMethods.getActualYesOthers())      

    println("....")
    println("Generating the ephemeral entities dictionaries")
    //Removing the tweets that were marked as invalid during the previous steps
    val rddTweetsNoInnec = sentWordNetRDD.filter(line =>  PostFilterTweetsWords.hasInnecTweets(line)).cache()    
    val rddTweetsNoInnecShort = rddTweetsNoInnec.map(line => {  var arrayLine = line.split("\",\"");    
                                                                var origSent = arrayLine(0).replaceAll("[\"]", "");
                                                                var implSent = arrayLine(1).replaceAll("[\"]", "");
                                                                var procTweet = arrayLine(2).replaceAll("[\"]", "");
                                                                ("\""+origSent+"\",\""+implSent+"\",\""+procTweet+"\"") } ).cache()          
                                                                   
    //gathering words not part on the lexicon along with the sentiment that comes from dataset
    val rddWordsNoSentImpl = rddTweetsNoInnec.map(line => (line.split("\",\"")(3))
                                             .replaceAll("[\"]", ""))
                                             .flatMap(line => line.split(" "))
                                             .cache()
    //gathering words not part on the lexicon along with the sentiment that comes from sentiwordnet
    val rddWordsNoSentOrig = rddTweetsNoInnec.map(line => (line.split("\",\"")(4))
                                             .replaceAll("[\"]", ""))
                                             .flatMap(line => line.split(" "))
                                             .cache()                  
    //gathering words that are part from the lexicon 
//    val rddWordsSentiment  = rddTweetsNoInnec.map(line => (line.split("\",\"")(6))
//                                             .replaceAll("[\"]", ""))
//                                             .flatMap(line => line.split(" "))
//                                             .saveAsTextFile(ProjectParameters.projectFolder+lblTimeStamp+"WordsSentiment.csv")
//                                             
    //gathering words not part on the lexicon along with the sentiment that comes from dataset case  distinct
    val rddWordsNoSentOrigDist = rddTweetsNoInnec.map(line => (line.split("\",\"")(6))
                                                 .replaceAll("[\"]", ""))
                                                 .flatMap(line => line.split(" "))
                                                 .cache()
    //gathering words not part on the lexicon along with the sentiment that comes from sentiwordnet case distinct
    val rddWordsNoSentImplDist = rddTweetsNoInnec.map(line => (line.split("\",\"")(5))
                                                 .replaceAll("[\"]", ""))
                                                 .flatMap(line => line.split(" "))
                                                 .cache()
    //gathering the hashtags                                                                                                
    val rddHashTags = rddTweetsNoInnec.map(line => (line.split("\",\"")(7))
                                      .replaceAll("[\"]", ""))
                                      .flatMap(line => line.split(" "))
                                      .cache()      
    rddHashTags.filter(line => PostFilterTweetsWords.hasInneWords(line))
               .saveAsTextFile(PrParameters.projectFolder+lblTimeStamp+"HashTags.csv")           
    
    sentWordNetRDD.unpersist()
    rddHashTags.unpersist()
    
    
    //Tokens
    val rddWordsImplNoInnec   =  rddWordsNoSentImpl.filter(line => PostFilterTweetsWords.hasInneWords(line))        
    val rddWordsOrigNoInnec  =   rddWordsNoSentOrig.filter(line => PostFilterTweetsWords.hasInneWords(line))    
    val rddGroupNewWordsImp   =  PostCombWordsNoSent.groupWordsNoSentiment(rddWordsImplNoInnec,sc).cache()
    rddGroupNewWordsImp.saveAsTextFile(PrParameters.projectFolder+lblTimeStamp+"WordsNoSentimentImplComb.csv")
    val rddGroupNewWordsOrig  =  PostCombWordsNoSent.groupWordsNoSentiment(rddWordsOrigNoInnec,sc).cache()
    rddGroupNewWordsOrig.saveAsTextFile(PrParameters.projectFolder+lblTimeStamp+"WordsNoSentimentOrigComb.csv")
    
    //Tweets
    val rddWordsImplNoInnecDist = rddWordsNoSentImplDist.filter(line => PostFilterTweetsWords.hasInneWords(line))        
    val rddWordsOrigNoInnecDist = rddWordsNoSentOrigDist.filter(line => PostFilterTweetsWords.hasInneWords(line))
    val rddGroupNewWordsImpDist = PostCombWordsNoSent.groupWordsNoSentimentNoPOS(rddWordsImplNoInnecDist, sc).cache()
    rddGroupNewWordsImpDist.saveAsTextFile(PrParameters.projectFolder+lblTimeStamp+"WordsNoSentimentImplCombDist.csv")
    val rddGroupNewWordsOrigDist  =  PostCombWordsNoSent.groupWordsNoSentimentNoPOS(rddWordsOrigNoInnecDist, sc).cache()
    rddGroupNewWordsOrigDist.saveAsTextFile(PrParameters.projectFolder+lblTimeStamp+"WordsNoSentimentOrigCombDist.csv")
    
    //Unpersist
    rddTweetsNoInnec.unpersist()    
    rddWordsNoSentImpl.unpersist()
    rddWordsNoSentOrig.unpersist()
    rddWordsNoSentImplDist.unpersist()
    rddWordsNoSentOrigDist.unpersist()    
    
    println("generating the updates for the lexicon case AVG")
    val rddUpdatesAVGImp = rddGroupNewWordsImp.map( line => UpdLexGenerate.MethodAvgUpdateLexicon(line._1,line._2._1, line._2._2, line._2._3 ))
                                              .saveAsTextFile(PrParameters.projectFolder+lblTimeStamp+"AVGSentWordNetTokens.csv")
    val rddUpdatesAVGImpDist = rddGroupNewWordsImpDist.map( line => UpdLexGenerate.MethodAvgUpdateLexicon(line._1,line._2._1, line._2._2, line._2._3 ))
                                                      .saveAsTextFile(PrParameters.projectFolder+lblTimeStamp+"AVGSentWordNetTweets.csv")
    
       
    //case PMI
    println("generating the updates for the lexicon case PMI case tweets")
    val labelDistantSup = false
    //total number tweets, positive negative
    val totalTweets = UpdLexGenerate.calculateTweetsTotal(sc, rddTweetsNoInnecShort, labelDistantSup)    
    val totalTweetsPositive = UpdLexGenerate.calculateTweetsPosiClas(sc, rddTweetsNoInnecShort, labelDistantSup)    
    val totalTweetsNegative = UpdLexGenerate.calculateTweetsNegClas(sc, rddTweetsNoInnecShort, labelDistantSup)
    //calculating min and max values for the PMI equation
    val rddMinMaxPMImp   =   rddGroupNewWordsImpDist.map(line => UpdLexGenerate.CalculateMaxMin(line._1,line._2._1, line._2._2, line._2._3,totalTweets,totalTweetsPositive, totalTweetsNegative ) )
                                                    .cache()
    val valMin = rddMinMaxPMImp.min()
    val valMax = rddMinMaxPMImp.max()
    println("Min "+valMin)
    println("Max "+valMax)
    println("TotalTweets: "+totalTweets)
    println("Total Tweets Positive: "+totalTweetsPositive )
    println("Total Tweets Negative: "+totalTweetsNegative)
    //Saving as a file
    val rddUpdatesPMIImp     =  rddGroupNewWordsImpDist.map(line => UpdLexGenerate.MethodPmiUpdateLexicon(line._1,line._2._1, line._2._2, line._2._3, labelDistantSup,totalTweets,totalTweetsPositive,totalTweetsNegative, valMax, valMin, "tweets" ) )
                                                       .saveAsTextFile(PrParameters.projectFolder+lblTimeStamp+"SentiWordNetPMITweets.txt")
    
    println("...")
    println("generating the updates for the lexicon case PMI case tokens")     
    val labelDistSupMethToks = false    
    val totalToks= UpdLexGenerate.calculateTokensTotal(sc, rddTweetsNoInnecShort, labelDistSupMethToks) //Include all words since were removed the meaningless, typos and so on    
    val totalToksPos = UpdLexGenerate.calculateTokensPosiClas(sc, rddTweetsNoInnecShort, labelDistSupMethToks)    
    val totalToksNeg = UpdLexGenerate.calculateTokensNegClas(sc, rddTweetsNoInnecShort, labelDistSupMethToks)    
    val rddMinMaxPMImpMethToks   =   rddGroupNewWordsImp.map(line => UpdLexGenerate.CalculateMaxMin(line._1,line._2._1, line._2._2, line._2._3,totalToks,totalToksPos, totalToksNeg ) ).cache()
    val valMinMethToks = rddMinMaxPMImpMethToks.min()
    val valMaxMethToks = rddMinMaxPMImpMethToks.max()    
    println("Min "+valMinMethToks)
    println("Max "+valMaxMethToks)
    println("Total Tokens: "+totalToks)
    println("Total Tokens Positive: "+totalToksPos)
    println("Total Tokens Negative: "+totalToksNeg) 
    //saving as a file
    val rddUpdatesPMIImp_     =  rddGroupNewWordsImp.map(line => UpdLexGenerate.MethodPmiUpdateLexicon(line._1,line._2._1, line._2._2, line._2._3, labelDistSupMethToks,totalToks,totalToksPos,totalToksNeg, valMaxMethToks, valMinMethToks, "tokens" ) )
                                                    .saveAsTextFile(PrParameters.projectFolder+lblTimeStamp+"SentiWordNetPMITokens.txt")   
    
  }
  
}