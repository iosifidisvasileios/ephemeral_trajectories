package research.twitter.twitterSpark

import java.io.FileInputStream
import net.didion.jwnl.JWNL
import net.didion.jwnl.data.POS
import net.didion.jwnl.dictionary.Dictionary
import collection.JavaConverters._
import net.didion.jwnl.dictionary.MapBackedDictionary

import java.io.ByteArrayInputStream 
import javax.xml.transform.dom.DOMSource 

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import java.io.File
import java.io.StringWriter
import java.io.InputStream

import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkFiles

import javax.xml.transform.TransformerException 
import javax.xml.transform.TransformerFactory

import javax.xml.transform.stream.StreamResult


import javax.xml.parsers.DocumentBuilder 
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.xpath.XPathFactory 
import javax.xml.xpath.XPath
import javax.xml.xpath.XPathConstants
import javax.xml.xpath.XPathExpression

import org.w3c.dom.Attr
import org.w3c.dom.Document 
import org.w3c.dom.Element 
import org.w3c.dom.Node

/*object that interacts with the library JWNL (java wordnet library)  by J. Didion*/

object FindBaseForm {
  
  // initBaseForm() //local
  initAutomatic()  //for cluster  
  val dictionary = Dictionary.getInstance()
  // converts verbs to base form   
  def convertVerbsBaseForm(tweet:String) : String = 
  {    
    var tweetWithBaseForm:String = ""    
    var arrayTweet:Array[String] = tweet.split(" ")
    val posVerbs  = Array( "VBD","VBG","VBN","VBP","VBZ")
    
    for(word <- tweet.split(" "))    
    {        
      val (word_, pos) =  if (Utils.isNegativeWord(word,"_"))   Utils.splitWordNegative(word, "_") else Utils.splitWord(word, "_")        
      
      if(posVerbs.contains(pos))
      {
        if(Utils.isNegativeWord(word,"_"))  tweetWithBaseForm += "not_"
        tweetWithBaseForm += getBaseFormVerb(word_) +"_"+ pos+" " 
      }else
      {
        tweetWithBaseForm += word+" "
      }        
    }    
    return tweetWithBaseForm.trim()    
  } 
  
  //converts plural to singular form  
  def convertPluralToSingularForm(tweet:String) : String = 
  {    
    var tweetWithBaseForm:String = ""
    for(word <- tweet.split(" "))    //kids_NN 0 1 size 2  or not_kids_NNS 0 1 2 size3 // good_VBG (0 -> word), (1 -> POS)  length  = 2   or   not_good_VBG  0,1,2  length = 3   good_VBG 0,1 == 2
    {        
      var (word_,pos) = if(Utils.isNegativeWord(word,"_")) Utils.splitWordNegative(word, "_") else Utils.splitWord(word, "_")
      if(pos == "NNS" || pos == "NNPS")
      {
        if(Utils.isNegativeWord(word,"_"))  tweetWithBaseForm += "not_"
        tweetWithBaseForm += getSingularNoun(word_) +"_"+pos+" "
      }else
      {
        tweetWithBaseForm += word+" "
      }
    }    
    return tweetWithBaseForm.trim()    
  } 
  
  //base form of a verb 
  def getBaseFormVerb(verb:String) : String =  { 	           
    var baseForm:String = verb     
    try {
        val baseFormList = dictionary.getMorphologicalProcessor.lookupAllBaseForms(POS.VERB, verb)
        if(baseFormList.size!=0)
        {
          var tempString = baseFormList.get(0).toString.split(" ") // comics --> comic strips
          if(tempString.length == 1)
          {
            baseForm = baseFormList.get(0).toString  
          }else
          {
            baseForm = verb
          }
        }          
      
    } catch {
      case t: Throwable => t.printStackTrace() 
    }      
    return baseForm      
  }
  
  //base form form an adverb and adjective   
  def getBaseFormAdverbAdjective(onlyWord:String, posSpecific:String) : String = 
  {
    //Return the base form, if not found return same word
    var baseform = onlyWord
    if(posSpecific == "JJR" || posSpecific == "JJS" )
    { 
      baseform = getBaseFormAdjective(onlyWord)
    }else if (posSpecific == "RBR" || posSpecific =="RBS")
    {
      baseform = getBaseFormAdverb(onlyWord)
    }
    return baseform       
  }
     
     
   def getBaseFormAdjective(adjective:String) : String =  {
     //gett_GTR  0  1  ||  gett_GTR_PERSON  0  1 
    var baseForm:String = ""
    //val dict:Dictionary = Dictionary.getInstance()      
    //val baseFormList = dictBacked.getMorphologicalProcessor.lookupAllBaseForms(POS.VERB, verb)
    val arrayAdj = adjective.split("_")      
    val baseFormList = dictionary.getMorphologicalProcessor.lookupAllBaseForms(POS.ADJECTIVE, arrayAdj(0).toLowerCase())      
    //println("In adjective: "+adjective)     
    if(baseFormList.size!=0)
    {
      var tempString = baseFormList.get(0).toString.split(" ") // comics --> comic strips
      if(tempString.length==1)
      {
        baseForm = baseFormList.get(0).toString
        //baseForm = baseFormList.get(0).toString+"_"+arrayAdj(1)
       
      }else
      {
        baseForm = adjective
      }        
    }else
    {
      baseForm = adjective
    }        
    return baseForm      
  }
     
   def getBaseFormAdverb(adverb:String) : String =  { 	           
    var baseForm:String = ""
    val listAdverb = adverb.split("_") //highest_BRB  0  1
    //val dict:Dictionary = Dictionary.getInstance()      
    //val baseFormList = dictBacked.getMorphologicalProcessor.lookupAllBaseForms(POS.VERB, verb)  
    
    try {
      
      val baseFormList = dictionary.getMorphologicalProcessor.lookupAllBaseForms(POS.ADVERB, listAdverb(0).toLowerCase())      
         
      if(baseFormList.size!=0)
      {
        var tempString = baseFormList.get(0).toString.split(" ") // comics --> comic strips
        if(tempString.length==1)
        {
          baseForm = baseFormList.get(0).toString 
        }else
        {
          baseForm = adverb
        }
        
      }else
      {
        baseForm = adverb
      }
      
    } catch {
      case t: Throwable => t.printStackTrace() 
    }
    
            
    return baseForm      
  }    
  
  //retrieving the singular form of a noun 
  def getSingularNoun(noun:String) : String =  { 	           
    var baseForm:String = noun
    //val dict:Dictionary = Dictionary.getInstance()
    
    try {
      
      val baseFormList = dictionary.getMorphologicalProcessor.lookupAllBaseForms(POS.NOUN, noun)
      if(baseFormList.size!=0)
      {
        var tempString = baseFormList.get(0).toString.split(" ") // comics --> comic strips
        if(tempString.length==1)
        {
          baseForm = baseFormList.get(0).toString  
        }else
        {
          baseForm = noun
        }
        
      }
      
    } catch {
      case t: Throwable => t.printStackTrace() 
    }      
    return baseForm      
  }
   
  def getSingularNoun(noun:String, posSpecific:String) : String =  { 	           
    var baseForm:String = ""
    if(posSpecific=="NNS" || posSpecific== "NNPS")
    {
        //val dict:Dictionary = Dictionary.getInstance()
        val baseFormList = dictionary.getMorphologicalProcessor.lookupAllBaseForms(POS.NOUN, noun)
        if(baseFormList.size!=0)
        {
          var tempString = baseFormList.get(0).toString.split(" ") // comics --> comic strips
          if(tempString.length==1)
          {
            baseForm = baseFormList.get(0).toString  
          }
          
        }else
        {
          baseForm = noun
        }
      
    }else
    {
      baseForm = noun
    }        
    return baseForm      
  }
     
  def getWordAt(posWord:String, wordOffset:String) :String =
  {
     var pos:POS = POS.ADJECTIVE
     posWord match {
       case "a" => pos = POS.ADJECTIVE
       case "r" => pos = POS.ADVERB
       case "n" => pos = POS.NOUN
       case "v" => pos = POS.VERB
     }    
    var outputString = dictionary.getIndexWord(pos, wordOffset)   
    var outputT =  outputString.getSynsetOffsets
    outputT.foreach(println)
    return outputString.toString()
   }
  
   def getSynsetAt(posWord:String, wordOffset:Long) :String =
   {
     var pos:POS = POS.ADJECTIVE
     posWord match {
       case "a" => pos = POS.ADJECTIVE
       case "r" => pos = POS.ADVERB
       case "n" => pos = POS.NOUN
       case "v" => pos = POS.VERB
     }     
     var outputString = dictionary.getSynsetAt(pos, wordOffset)
     return outputString.toString()
   }  
   
   def initBaseForm()  =
   {
    var conf = new Configuration()
    var fileSystem = FileSystem.get(conf)
    var path = new Path(PrParameters.dictBForm)
    val inputStream = fileSystem.open(path)
    JWNL.initialize(inputStream)
    val dictBacked:MapBackedDictionary =  new MapBackedDictionary()     
   }
   
   //used for the cluster to update the xml files to reach the source library files 
   def initAutomatic( ): Unit = {
      
      val conf = new Configuration()
      val fileSystem = FileSystem.get(conf)
      val pathXmlMapProperties = new Path(PrParameters.dictBForm)
      val inputStream = fileSystem.open(pathXmlMapProperties)     
      val al1 = SparkFiles.get("adj.exc" )
      val al2 = SparkFiles.get("adv.exc")
      val al3 = SparkFiles.get("data.adj")
      val al4 = SparkFiles.get("data.adv")
      val al5 = SparkFiles.get("data.noun")
      val al6 = SparkFiles.get("data.verb")
      val al7 = SparkFiles.get("index.adj")
      val al8 = SparkFiles.get("index.adv")
      val al9 = SparkFiles.get("index.noun")
      val al10 = SparkFiles.get("index.verb")
      val al11 = SparkFiles.get("noun.exc")
      val al12 = SparkFiles.get("verb.exc")
      val wordnetPath: String = SparkFiles.getRootDirectory()     
       
      val propertiesStream: InputStream = inputStream
      val fact: DocumentBuilderFactory = DocumentBuilderFactory.newInstance()
      fact.setNamespaceAware(true)
      val parser: DocumentBuilder = fact.newDocumentBuilder()
      val doc: Document = parser.parse(propertiesStream)
      val factory: XPathFactory = XPathFactory.newInstance()
      val xpath: XPath = factory.newXPath()
      val expr: XPathExpression = xpath.compile("//jwnl_properties/dictionary/param[@name='dictionary_path']")
      val result: AnyRef = expr.evaluate(doc, XPathConstants.NODE)
      val node: Node = result.asInstanceOf[Node]
      val param: Element = node.asInstanceOf[Element]
      val value: Attr = param.getAttributeNode("value")
      value.setValue(wordnetPath)
      val out: StringWriter = new StringWriter()
      TransformerFactory.newInstance().newTransformer().transform(new DOMSource(doc), new StreamResult(out))
      val modifiedPropertiesStream: InputStream = new ByteArrayInputStream(out.toString.getBytes)
      JWNL.initialize(modifiedPropertiesStream)
      val dictBacked:MapBackedDictionary =  new MapBackedDictionary()
  }
  
}