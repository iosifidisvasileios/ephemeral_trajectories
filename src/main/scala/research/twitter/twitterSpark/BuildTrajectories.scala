package research.twitter.twitterSpark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/*method that gather all the updates to the lexicon filter the ephemeral entities and build the trajectories*/
object BuildTrajectories {
  
  val configuration = new SparkConf().setAppName("Entities").setMaster("local") //local  
  val sc = new SparkContext(configuration)  //local
    
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  sc.setLogLevel("ERROR") 
  
  //loading Fel entities
  val distEntitiesFel = loadEntitiesFel(sc).cache()
  
  var mapRDDMonthsCleanUpdLex = scala.collection.Map[String, scala.collection.Map[String, Double]]()
  //storing the dictionaries strings of the 45 months 
  //expected format "hdfs://nameservice1/user/campero/ResMonthsFinal/2013-01/2013-01SentiWordNetPMITokens.txt"
  var seqString = Seq[String]()
  
  seqString :+= PrParameters.trajecFolder+"2013-04AVGSentWordNetTweets.csv"
  seqString :+= PrParameters.trajecFolder+"2013-05AVGSentWordNetTweets.csv"
  seqString :+= PrParameters.trajecFolder+"2013-06AVGSentWordNetTweets.csv"
  seqString :+= PrParameters.trajecFolder+"2013-07AVGSentWordNetTweets.csv"
  seqString :+= PrParameters.trajecFolder+"2013-08AVGSentWordNetTweets.csv"
  seqString :+= PrParameters.trajecFolder+"2013-09AVGSentWordNetTweets.csv"
  seqString :+= PrParameters.trajecFolder+"2013-10AVGSentWordNetTweets.csv"
  seqString :+= PrParameters.trajecFolder+"2013-11AVGSentWordNetTweets.csv"
  seqString :+= PrParameters.trajecFolder+"2013-12AVGSentWordNetTweets.csv"
  seqString :+= PrParameters.trajecFolder+"2014-01AVGSentWordNetTweets.csv"
  seqString :+= PrParameters.trajecFolder+"2014-02AVGSentWordNetTweets.csv"
  seqString :+= PrParameters.trajecFolder+"2014-03AVGSentWordNetTweets.csv"
  seqString :+= PrParameters.trajecFolder+"2014-04AVGSentWordNetTweets.csv"
  seqString :+= PrParameters.trajecFolder+"2014-05AVGSentWordNetTweets.csv"
  seqString :+= PrParameters.trajecFolder+"2014-06AVGSentWordNetTweets.csv"
  seqString :+= PrParameters.trajecFolder+"2014-07AVGSentWordNetTweets.csv"
  seqString :+= PrParameters.trajecFolder+"2014-08AVGSentWordNetTweets.csv"
  seqString :+= PrParameters.trajecFolder+"2014-09AVGSentWordNetTweets.csv"
  seqString :+= PrParameters.trajecFolder+"2014-10AVGSentWordNetTweets.csv"
  seqString :+= PrParameters.trajecFolder+"2014-11AVGSentWordNetTweets.csv"
  seqString :+= PrParameters.trajecFolder+"2014-12AVGSentWordNetTweets.csv"
  seqString :+= PrParameters.trajecFolder+"2015-01AVGSentWordNetTweets.csv"
  seqString :+= PrParameters.trajecFolder+"2015-02AVGSentWordNetTweets.csv"
  seqString :+= PrParameters.trajecFolder+"2015-03AVGSentWordNetTweets.csv"
  seqString :+= PrParameters.trajecFolder+"2015-04AVGSentWordNetTweets.csv"
  seqString :+= PrParameters.trajecFolder+"2015-05AVGSentWordNetTweets.csv"
  seqString :+= PrParameters.trajecFolder+"2015-06AVGSentWordNetTweets.csv"
  seqString :+= PrParameters.trajecFolder+"2015-07AVGSentWordNetTweets.csv"
  seqString :+= PrParameters.trajecFolder+"2015-08AVGSentWordNetTweets.csv"
  seqString :+= PrParameters.trajecFolder+"2015-09AVGSentWordNetTweets.csv"
  seqString :+= PrParameters.trajecFolder+"2015-10AVGSentWordNetTweets.csv"
  seqString :+= PrParameters.trajecFolder+"2015-11AVGSentWordNetTweets.csv"
  seqString :+= PrParameters.trajecFolder+"2015-12AVGSentWordNetTweets.csv"
  seqString :+= PrParameters.trajecFolder+"2016-01AVGSentWordNetTweets.csv"
  seqString :+= PrParameters.trajecFolder+"2016-02AVGSentWordNetTweets.csv"
  seqString :+= PrParameters.trajecFolder+"2016-03AVGSentWordNetTweets.csv"
  seqString :+= PrParameters.trajecFolder+"2016-04AVGSentWordNetTweets.csv"
  seqString :+= PrParameters.trajecFolder+"2016-05AVGSentWordNetTweets.csv"
  seqString :+= PrParameters.trajecFolder+"2016-06AVGSentWordNetTweets.csv"
  seqString :+= PrParameters.trajecFolder+"2016-07AVGSentWordNetTweets.csv"
  seqString :+= PrParameters.trajecFolder+"2016-08AVGSentWordNetTweets.csv"
  seqString :+= PrParameters.trajecFolder+"2016-09AVGSentWordNetTweets.csv"
  seqString :+= PrParameters.trajecFolder+"2016-10AVGSentWordNetTweets.csv"
  seqString :+= PrParameters.trajecFolder+"2016-11AVGSentWordNetTweets.csv"
  seqString :+= PrParameters.trajecFolder+"2016-12AVGSentWordNetTweets.csv"

  createTrajectories(sc, seqString,distEntitiesFel,"avg","AvgTokensCombined.csv", 5)
  /*CreateTrajectories: 
   * method = can be 'avg' or 'pmi' 
   * outputname = it is the name of the trajectory file 
   * expexted format of the dictionary paths
   * //"hdfs:/nameservice1/user/campero/ResMonthsFinal/2013-01/2013-01SentiWordNetPMITokens.txt"
   * posPeriod = the format is going to be split and the position of the time period should be sent "/2013-01/" in this case 5
   * */
  def createTrajectories(sc:SparkContext, seqString:Seq[String], entitiesCombNonAscii:RDD[String], method:String, outputName:String, posPeriod:Int)= 
  { 
     if(method == "avg")
     { 
       var seqRddEntitiesUdpLex = Seq[RDD[(String,Int)]]()
       for(month <- seqString)
       {
         println("month: "+month)
         var arrayMonth =  month.split("/")
         var label = arrayMonth(posPeriod)         
         println("Label: "+label)
         //cleaning the blank spaces and save into a sequence
         val monthUpdLexiconPolarity =  cleanWordsLexPolarity(sc, month).collectAsMap()         
         mapRDDMonthsCleanUpdLex += ( label -> monthUpdLexiconPolarity)
         //
         val monthUpdLexicon      =  filterOutSentiWordNetWords(sc,month)
         val monthEntitiesUpdLexicon =  filterFelEntities(monthUpdLexicon, entitiesCombNonAscii)
         //save each dictionary in a sequence
         seqRddEntitiesUdpLex  :+= monthEntitiesUpdLexicon                    
       }
       //applying union and distinct to the entities of the whole trajectory
       val entitiesDistPeriod = unionDistRdd(sc, seqRddEntitiesUdpLex).cache()
       println("Number of distinct entities: "+entitiesDistPeriod.count())
       
       var rddRes = lookupOnAllMonths(sc, seqString, entitiesDistPeriod)
       var resUlt = filterPerAppereance(sc, rddRes)
       resUlt.saveAsTextFile(PrParameters.projectFolder+outputName)  
         
       
     }else if(method == "pmi")
     {
       //Pmi
       var seqRddEntitiesUdpLex = Seq[RDD[(String, Int)]]()
       for(month <- seqString)
       {
         println("month: "+month)
         var arrayMonth =  month.split("/")
         var label = arrayMonth(posPeriod)
         println("Label: "+label)
         val monthUpdLexiconPolarity = cleanWordsLexPolarityPmi(sc, month).collectAsMap()         
         mapRDDMonthsCleanUpdLex += ( label -> monthUpdLexiconPolarity)         
         val monthUpdLexicon      =  filterOutSentWordNetWordsPmi(sc,month)
         val monthEntitiesUpdLexicon =  filterFelEntitiesPmi(monthUpdLexicon, entitiesCombNonAscii)         
         seqRddEntitiesUdpLex  :+= monthEntitiesUpdLexicon         //Save each month in the sequence,  the entities present on the lexicon updates
       }  
     
       val entitiesDistPeriod = unionDistRddPmi(sc, seqRddEntitiesUdpLex).cache()
       println("Number of distinct entities is: "+entitiesDistPeriod.count())
       var rddRes = lookupOnAllMonthsPmi(sc, seqString, entitiesDistPeriod)
       var resUlt = filterPerAppereance(sc, rddRes)
       resUlt.saveAsTextFile(PrParameters.projectFolder+outputName)
       
     }  
   }
  //cleaning blank spaces and filtering out the sentiwordnet words
  def cleanWordsLexPolarity(sc:SparkContext,urlUpdateLexicon:String):RDD[(String,Double)] = 
  {
    var monthLex  =  sc.textFile(urlUpdateLexicon)
    var sentiWNet = sc.textFile("ExternalFiles/sentiWordNetClean.txt").map(line => (line.trim(), 1)).collectAsMap()     
    var cleanMonthLex  =  monthLex.filter(line =>  checkBlankLine(line))
                                  .filter(line => !sentiWNet.contains(line.split("\t")(1).trim()))
                                  .map(line => { 
                                               var word = line.split("\t")(1)
                                               var pos = line.split("\t")(2).toDouble
                                               var neg =  line.split("\t")(3).toDouble
                                                   (word, (pos-neg))
                                                 } )
    return cleanMonthLex
  }
     
  //filter out words that are part from SentiWordNet
  def filterOutSentiWordNetWords(sc:SparkContext, urlUpdateLexicon:String):RDD[(String,Int)] =
  { 
    var sentiWNet = sc.textFile("ExternalFiles/sentiWordNetClean.txt").map(line => (line.trim(), 1)).collectAsMap()
    var monthLex       =  sc.textFile(urlUpdateLexicon)
    var cleanMonthLex  =  monthLex.filter(line => checkBlankLine(line))
                                   .filter(line => !sentiWNet.contains(line.split("\t")(1).trim()))  
                                   .map(line => (line.split("\t")(1), line.split("\t")(4).toInt ) )
    return cleanMonthLex
  }
  //filter only words that are part of FEL entities 
  def filterFelEntities(cleanWordUpdLex:RDD[(String,Int)], felEntities:RDD[String] ) :RDD[(String,Int)] = //
  {
    val modDistEntNonAscii = felEntities.map(line => (line, ""))
    val intRdd = cleanWordUpdLex.join(modDistEntNonAscii).map(line => (line._1, line._2._1)).distinct()
    return intRdd
  }
  //Given a list of RDD apply them union and distinct operations
  def unionDistRdd(sc:SparkContext, rdds:Seq[RDD[(String,Int)]]) :RDD[(String,Int)] =
  {
    val rddOut = sc.union(rdds).reduceByKey(_+_)         
    return rddOut
  }
  //look in how many months an entity is present and order it accordingly 
  def lookupOnAllMonths(sc:SparkContext,  seqString:Seq[String], rddWordsEntitiesPeriods:RDD[(String,Int)]) :RDD[(String,(String,Int,Int) )]= 
  {       
    var count = 0
    var monthSentimentPolarity  = rddWordsEntitiesPeriods.map( line => {      
                                                            val( result:String, count:Int) = lookUp45Months(line._1, seqString)  
                                                            (line._1, (result, line._2, count )) } )                                                             
                                                         .sortBy(_._2._3 , false)
    return monthSentimentPolarity  
  }
  //ordering the entities to display at the top the entities that are present on the 45 months     
  def filterPerAppereance(sc:SparkContext,  rddIn:RDD[(String, (String, Int, Int))]) :RDD[(String, (String, Int, Int))] = 
  {
    var seqArrayRdd = Seq[RDD[(String, (String, Int, Int) )]]()
    for(i <- 45 to 1 by -1)
    {
      var rddMonth = rddIn.filter(line => (line._2._3 == i))
                          .sortBy( _._2._2,false).cache()
      var countRddMonth =rddMonth.count() 
      println("The number of entities that appear on: "+i+" periods is: "+countRddMonth )
      seqArrayRdd :+= rddMonth
    }
    var returnRdd = unionRddResult(sc, seqArrayRdd )
    return returnRdd     
  }
  
  //cleaning blank spaces and filtering out the sentiwordnet words case Pmi    
  def cleanWordsLexPolarityPmi(sc:SparkContext,urlUpdateLexicon:String):RDD[(String,Double)] = 
  {
    var monthLex  =  sc.textFile(urlUpdateLexicon)
    var sentiWNet = sc.textFile("ExternalFiles/sentiWordNetClean.txt").map(line => (line.trim(), 1)).collectAsMap()    
    var cleanMonthLex  =  monthLex.filter(line =>  checkBlankLine(line))
                                  .filter(line => !sentiWNet.contains(line.split("\t")(1).trim()))
                                  .map(line => {  
                                    var word = line.split("\t")(1)
                                    var sentVal = line.split("\t")(2).toDouble
                                    (word, sentVal)
                                  }).sortBy(_._1, true)
     return cleanMonthLex
  }
  //cleaning blank spaces and filtering out sentiwordnet words   
  def filterOutSentWordNetWordsPmi(sc:SparkContext, urlUpdateLexicon:String):RDD[(String, Int)] =
  { 
    var sentiWNet = sc.textFile("ExternalFiles/sentiWordNetClean.txt").map(line => (line.trim(), 1)).collectAsMap()
    var monthLex       =  sc.textFile(urlUpdateLexicon)
    var cleanMonthLex  =  monthLex.filter(line => checkBlankLine(line))
                                  .filter(line => !sentiWNet.contains(line.split("\t")(1).trim()))
                                  .map(line => (line.split("\t")(1), line.split("\t")(3).toInt ))
                                  .sortBy(_._1, true)
    return cleanMonthLex
  }
  //considering only fel entities   
  def filterFelEntitiesPmi(cleanWordUpdLex:RDD[(String,Int)], felEntities:RDD[String] ) :RDD[(String,Int)] = //
  {
    val modDistEntNonAscii = felEntities.map(line => (line, ""))
    val intRdd = cleanWordUpdLex.join(modDistEntNonAscii).map(line => (line._1, line._2._1)).distinct()
    return intRdd
  }
     
  def unionDistRddPmi(sc:SparkContext, rdds:Seq[RDD[(String, Int)]]) :RDD[(String, Int)] =
  {
    val rddOut = sc.union(rdds).reduceByKey(_+_)
    return rddOut
  }
     
  def lookupOnAllMonthsPmi(sc:SparkContext,  seqString:Seq[String], rddWordsEntitiesPeriods:RDD[(String, Int)] ) :RDD[(String, (String, Int, Int))]=
  {
     var count = 0                                                                             
     var monthSentimentPolarity  = rddWordsEntitiesPeriods.map( line => { val( result:String, count:Int) = lookUp45Months(line._1, seqString)
                                                                           (line._1, (result, line._2, count )) } )                                                             
                                                           .sortBy(_._2._3 , false)
     return monthSentimentPolarity
  }
     
  //check whether a line has blank spaces                                 
  def checkBlankLine(line:String) :Boolean = 
  {
    var flag = true         
    if(line == "")
    {          
      flag = false
    }  
    return flag
  }
  //look if an entity is present in the whole period 
  def lookUp45Months(word:String, seqString:Seq[String] )  = 
  {
    var counterMonths = 0
    var result = ""
    for(month <- seqString)
    {
       val arrayMonth =  month.split("/")
       var label = arrayMonth(4)
       val mapMonthCleanUpdLex = mapRDDMonthsCleanUpdLex.get(label).get
       var initDelim = ""
       if(counterMonths == 0 ){ initDelim = "" }else { initDelim = "," }
       var exists = mapMonthCleanUpdLex.contains(word)
       
       if (exists){
         counterMonths=counterMonths+1
         result += initDelim+mapMonthCleanUpdLex.get(word).get.toString()+"\t"  
        }
       else{ 
         result += initDelim+"?"+"\t" 
       }
    }
    (result, counterMonths)
  }
   
  def unionRddResult(sc:SparkContext, rdds:Seq[RDD[(String, (String, Int, Int) )]]) :RDD[(String, (String, Int, Int) )] =
  {   
    val rddOut = sc.union(rdds)
    return rddOut
  }
  //load entities into rdd    
  def loadEntitiesFel(sc:SparkContext) :RDD[String] = 
  {
    val entitiesFel  =  sc.textFile(PrParameters.entitiesFel)
    var rddOut = entitiesFel.filter(line => checkBlankLine(line))
                             .cache()
                             
    return rddOut
  }
     
     
  
  
  
  
}