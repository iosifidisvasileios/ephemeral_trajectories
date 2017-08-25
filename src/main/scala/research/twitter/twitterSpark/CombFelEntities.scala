package research.twitter.twitterSpark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/*this object takes the list of gathered entities FEL and apply some preprocessing steps */

object CombFelEntities {
  
  def main(args:Array[String]) :Unit =
  {
    val configuration = new SparkConf().setAppName("Entities") //cluster
    val sc = new SparkContext(configuration)  //cluster
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    sc.setLogLevel("ERROR")
    
    PrParameters.setProjectFolder(args(0))
    val inRddFelEnt  =  sc.textFile(PrParameters.srcFelEntities)
    val combEntities = inRddFelEnt.map(line => { var entity = line.split("\t")(1);
                                  entity = Preprocess.lowerCaseTweet(entity);
                                  entity = Preprocess.deleteBlankSpaces(entity);
                                  entity = Preprocess.replacePunctuationMarks(entity);
                                  entity = Preprocess.deleteNumbers(entity);
                                  entity = Preprocess.deleteSpecialChars(entity);
                                  entity = Preprocess.deleteNumbers(entity);
                                  entity = Preprocess.deleteNumbers(entity);
                                  entity = Preprocess.deleteBlankSpaces(entity);
                                  (entity)})
                                  .flatMap(line => line.split(" "))
                                  .distinct()
                                  .sortBy(line => line, true)
                                  .saveAsTextFile(PrParameters.projectFolder+"FelEntitiesComb.csv")
  }
  
  
  
}