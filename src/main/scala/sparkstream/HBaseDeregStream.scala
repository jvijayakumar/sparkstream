/*
 * Spark streaming to ingest identity deregistered data to hbase
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

object HBaseDeregStream extends Serializable {
  final val tableName = "user_dereg"
  final val cfDeregBytes = Bytes.toBytes("dereg")
  final val colUpmBytes = Bytes.toBytes("user_id")
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
      val rowkey = dereg.user_id
      val put = new Put(Bytes.toBytes(rowkey))
      // add to column family data, column  data values to put object 
      put.add(cfDeregBytes, colDateBytes, Bytes.toBytes(dereg.dereg_date))
      return (new ImmutableBytesWritable(Bytes.toBytes(rowkey)), put)
 
    }

  }

  def main(args: Array[String]): Unit = {

    if (args.length != 1) {
        println("pass the parameters <zookeeper_quorum> <s3_input_filestream_path>")
    }
    // set up HBase Table configuration
    val conf = HBaseConfiguration.create()

    val confPathString = "/usr/lib/hbase/conf/hbase-site.xml" 
    conf.addResource(new Path(confPathString)) 
    conf.set(HConstants.ZOOKEEPER_QUORUM, args(0))
    conf.set("hbase.mapred.outputtable", tableName)
    conf.set("mapreduce.outputformat.class", "org.apache.hadoop.hbase.mapreduce.TableOutputFormat");

    val admin = new HBaseAdmin(conf) 

    val sparkConf = new SparkConf().setAppName("HBaseStream")
    
    // create a StreamingContext, the main entry point for all streaming functionality
    val ssc = new StreamingContext(sparkConf, Seconds(3))
   
    // parse the lines of data into Dereg objects
    val deregDStream = ssc.textFileStream(args(1)).map(Dereg.parseDereg)
    deregDStream.print()

    deregDStream.foreachRDD { rdd =>
      rdd.map(Dereg.convertToPut).
        saveAsNewAPIHadoopDataset(conf)

    }

    // Add checkpoint directory
    //ssc.checkpoint(args(2))

    // Start the computation
    ssc.start()
    // Wait for the computation to terminate
    ssc.awaitTermination()

  }

}
