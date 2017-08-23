package research.twitter.twitterSpark


import scala.io.Codec
import java.nio.charset.CodingErrorAction

/*Object that contains the paths to the necessary files for the project */

object PrParameters {
  
  implicit val codec = Codec("UTF-8")  
  codec.onMalformedInput(CodingErrorAction.REPLACE)
  codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
  
  var projectFolder = ""
  var projectDataset = ""
  var avgLexicon = ""
  var pmiLexicon = ""  
  var slangList = """/ExternalFiles/SlangList2016.txt"""  
  var clasEnt3Class = """/ExternalFiles/english.all.3class.distsim.crf.ser.gz"""
  var clasEnt3ClassModel = "english.all.3class.distsim.crf.ser.gz"  
  var clasEnt7Class = """/Classifiers/english.muc.7class.distsim.crf.ser.gz"""
  var clasEnt7ClassModel = "english.muc.7class.distsim.crf.ser.gz"
  var sentiWordNetFile = """/ExternalFiles/SentiWordNet_3.0.0_20130122.txt"""  
  var dictBForm = """/ExternalFiles/map_properties.xml"""                          
  
  var tagModelFile = """/ExternalFiles/wsj-0-18-left3words-distsim.tagger"""
  var tagModelName = "wsj-0-18-left3words-distsim.tagger"
  var stopWordsFile = """/ExternalFiles/englishStop.txt"""
  var contractionsFile = """/ExternalFiles/contractionsEnglish.txt"""  
  var thesaurusFile = """/ExternalFiles/thesaurus.txt"""
  
  var listVerbsNegation = """/ExternalFiles/verbs.txt"""
  var listAdjOpp = """/ExternalFiles/opposites.txt"""
  
  var compBooster = 10.0 //30.0 //20.0 //40.0  //50.0
  var supBooster = 20.0  //60.0 //40.0 //80.0 //100.0
  
  var adjFile = """/externalFiles/adj.exc"""  
  var advFile = """"/externalFiles/adv.exc"""
  var dataAdj = """/externalFiles/data.adj"""
  var dataAdv = """/externalFiles/data.adv"""
  var dataNoun = """/externalFiles/data.noun"""
  var dataVerb = """/externalFiles/data.verb"""
  var indAdj = """/externalFiles/index.adj"""
  var indAdv = """/externalFiles/index.adv"""
  var indNoun = """/externalFiles/index.noun"""
  var indVerb = """/externalFiles/index.verb"""
  var nounFile = """/externalFiles/noun.exc"""
  var verbFile = """/externalFiles/verb.exc"""
  
  def setProjectDataset(projectDset:String) = 
  {
    this.projectDataset = projectDset    
  }
  
  def setProjectFolder(projectFolder:String) = 
  {
    this.projectFolder = projectFolder
  }
}