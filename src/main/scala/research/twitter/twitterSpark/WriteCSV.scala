package research.twitter.twitterSpark


import java.io.FileWriter
import java.io.BufferedWriter

import com.opencsv.CSVWriter
/*object to write into a csv file*/

object WriteCSV {
    
  def writeTextToCSV(negation: String, csvNewWordsWriter:CSVWriter) : Unit =
  {    
    csvNewWordsWriter.writeNext(Array(negation))
  }
  
  def writeArrayToCSV(negation: Array[String],csvNewWordsWriter:CSVWriter) : Unit =
  { 
    csvNewWordsWriter.writeNext(negation,false)    
  }
  
  def closeWriter(csvNewWordsWriter:CSVWriter) : Unit = 
  {
    csvNewWordsWriter.close()
  }
  
  def createCsvWriter(path:String) :CSVWriter = 
  { 
    val csvBufferWriter = new BufferedWriter(new FileWriter(path));
    val csvWriter:CSVWriter = new CSVWriter(csvBufferWriter,'\t' , CSVWriter.NO_QUOTE_CHARACTER)
    return csvWriter
  }
  
}