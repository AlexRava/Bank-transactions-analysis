package InputSimulation

import java.io.File
import java.util.Properties

import com.github.tototoshi.csv.CSVReader
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord}

trait Simulation [T]{

  private val N_MILLISEC_IN_SEC = 1000

  var waitingTime:Int

  def wait(SecWaitingTime: Double):Unit = waitingTime = (SecWaitingTime * N_MILLISEC_IN_SEC).asInstanceOf[Int]


  def init()
  def start( dataStream : DataSource[T])

}

//METTERE A POSTO WAIT E WAITING TIMEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE

object UsersSimulation extends Simulation[Seq[String]]{

  override var waitingTime: Int = _

  private val props: Properties = new Properties()

    override def init(): Unit = {
    props.put("bootstrap.servers", "localhost:9092");
    props.put("acks", "all");
    props.put("retries", "0");
    props.put("linger.ms", "1");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
  }

  //source dovrà ridare un csv reader
  override def start(source : DataSource[Seq[String]] ): Unit = {


    val producer: Producer[String, String] = new KafkaProducer(props)

    //val reader = CSVReader.open(new File("C:\\Users\\Alex\\Desktop\\Fraud_Historical_data\\historical_data.csv"))
    //reader.foreach()
    source.getDataStream().foreach(transaction => {
      producer.send(new ProducerRecord[String,String]("input-topic", transaction.mkString(",")))
      Thread.sleep(waitingTime)
      //println("prova",transaction.getClass.getDeclaredFields.map(_.getName).
      //classOf[Transaction].getDeclaredFields.map{ f => f.setAccessible(true) val res = (f.getName, f.get(u)) f.setAccessible(false) res }
      //println("",
      /* transaction.getClass.getDeclaredFields.map(f => {
       f.setAccessible(true)
       //println("",f.getName)
       //println("",f.get(transaction))
       println(f.get(transaction))//.getName,f.get(transaction).toString)

     })//)// {f.setAccessible(true) val res = (f.getName, f.get(transaction)) f.setAcce)})
     */
      //println(transaction)//.toString().split(","))

      //println(Transaction().createFromSeq(transaction))

      //convert Seq[String] in byteBuffer
      //var transactionToBeSerialize: ByteBuffer = new ByteBuffer()
      //println(transaction.mkString(","))
    })

  }


}

