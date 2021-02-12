package InputSimulation

import java.io.File
import com.github.tototoshi.csv.CSVReader


trait DataSource[T] {
  def getDataStream():Iterator[T]
  def dataSeparator:String
}

object sourceFactory {

 def readFromCSV(path: String) = new CSVsource(path)

}

class CSVsource(val source: String) extends DataSource[Seq[String]] {

  private val CSV_SEPARATOR = ","

  override def dataSeparator:String = CSV_SEPARATOR

  override def getDataStream(): Iterator[Seq[String]] =  CSVReader.open(new File(source)).iterator
  //val reader = CSVReader.open(new File("C:\\Users\\Alex\\Desktop\\Fraud_Historical_data\\historical_data.csv"))

}
