/*
 * Spark streaming to ingest deregistered data to hbase
 *  
 */

package sparkstream

import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.{HBaseConfiguration,HConstants, HTableDescriptor}
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.hbase.mapreduce.TableInputFormat 
import org.apache.hadoop.hbase.client.Put; 
import org.apache.hadoop.mapreduce.Job
import net.liftweb.json._

object HBaseDeregStreamWithCheckpoint extends Serializable {
  final val tableName = "user_dereg"
  final val cfDeregBytes = Bytes.toBytes("dereg")
  final val coluserBytes = Bytes.toBytes("user_id")
  final val colDateBytes = Bytes.toBytes("dereg_date")
  // schema for Dereg data   
  case class Dereg(user_id: String, dereg_date: String)
  case class Event(id: String, event_time: String,event_id:String,dtrace_id:String)
  case class Dereg_events(event_details: Event)
  implicit val formats = DefaultFormats 
  
  object Dereg extends Serializable  {
    // function to parse line of Dereg data into Dereg class
    def parseDereg(str: String): Dereg = {
      val parsedJson = parse(str)
      val a=parsedJson.extract[Dereg_events]
      val user_id= a.event_details.id
      val dereg_date=a.event_details.event_time
      Dereg(user_id, dereg_date)
    }
    //  Convert a row of Dereg object data to an HBase put object
    def convertToPut(dereg: Dereg): (ImmutableBytesWritable, Put) = {
      // create a composite row key 
      val rowkey = dereg.upm_id
      val put = new Put(Bytes.toBytes(rowkey))
      // add to column family data, column  data values to put object 
      put.add(cfDeregBytes, colDateBytes, Bytes.toBytes(dereg.dereg_date))
      return (new ImmutableBytesWritable(Bytes.toBytes(rowkey)), put)
 
    }

  }


  def createSC(zookeeper_ip:String, inputDir:String,checkpointDir:String ): StreamingContext = {

    // set up HBase Table configuration
    val conf = HBaseConfiguration.create()

    val confPathString = "/usr/lib/hbase/conf/hbase-site.xml" 
    conf.addResource(new Path(confPathString)) 
    conf.set(HConstants.ZOOKEEPER_QUORUM, zookeeper_ip)
    conf.set("hbase.mapred.outputtable", tableName)
    conf.set("mapreduce.outputformat.class", "org.apache.hadoop.hbase.mapreduce.TableOutputFormat");

    val admin = new HBaseAdmin(conf) 

    val sparkConf = new SparkConf().setAppName("HBaseStream")

    val ssc = new StreamingContext(sparkConf,Seconds(5))

    // Add checkpoint directory
    ssc.checkpoint(checkpointDir)

    // parse the lines of data into Dereg objects
    val dereg = ssc.textFileStream(inputDir)

    val deregDStream = dereg.map(Dereg.parseDereg)
 
    deregDStream.print()

    deregDStream.foreachRDD {rdd =>
        rdd.map(Dereg.convertToPut).
          saveAsNewAPIHadoopDataset(conf) }

    ssc
  }

  def main(args: Array[String]) {

    println(args.length)

    if (args.length != 3) {
        println("pass the parameters <zookeeper_quorum> <s3_input_filestream_path> <s3_checkpoint_path>")
        System.exit(1)
    }

    println(args(0))
    println(args(1))
    println(args(2))
    
    val zookeeper_ip = args(0)
    val inputDir = args(1)
    val checkpointDir = args(2)
    
    // create a StreamingContext, the main entry point for all streaming functionality
    val ssc = StreamingContext.getOrCreate(checkpointDir,() => createSC(zookeeper_ip,inputDir,checkpointDir))
   
    // Start the computation
    ssc.start()
    // Wait for the computation to terminate
    ssc.awaitTermination()

  }

}
