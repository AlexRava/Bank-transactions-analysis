package InputSimulation

import java.io.File
import com.github.tototoshi.csv.CSVReader


trait DataSource[T] {

  def getDataStream():Iterator[T]

}

object sourceFactory {

 def readFromCSV(path: String) = new CSVsource(path)

}

class CSVsource(val source: String) extends DataSource[Seq[String]] {

  override def getDataStream(): Iterator[Seq[String]] = CSVReader.open(new File(source)).iterator
}
