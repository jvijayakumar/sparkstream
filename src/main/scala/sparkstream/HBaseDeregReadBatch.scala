/*
 * Spark read from hbase and write to parquet
 *  
 */

package sparkstream

import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.spark._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration,HConstants, HTableDescriptor}
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.fs.Path; 
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object HBaseDeregReadBatch {
 def main(args: Array[String]) {

  if (args.length != 1) {
        println("pass the parameters <zookeeper_quorum> <s3_path> ")
    }
    // set up HBase Table configuration
    val conf = HBaseConfiguration.create()
    val tableName = "nike_user_dereg"
    val s3_path = args(1)

    val confPathString = "/usr/lib/hbase/conf/hbase-site.xml" 
    conf.addResource(new Path(confPathString)) 
    //conf.set(HConstants.ZOOKEEPER_QUORUM, "10.184.216.115")
    conf.set(HConstants.ZOOKEEPER_QUORUM, args(0))
    

    //Configuration setting for getting hbase data
    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    conf.set("hbase.mapred.outputtable", tableName)
    conf.set("mapreduce.outputformat.class", "org.apache.hadoop.hbase.mapreduce.TableOutputFormat");

    val sparkConf = new SparkConf().setAppName("HBaseRead")
    val sc = new SparkContext(sparkConf)
    val spark = new org.apache.spark.sql.hive.HiveContext(sc)

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
    classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
    classOf[org.apache.hadoop.hbase.client.Result])

    //create RDD of result
    val resultRDD =hBaseRDD.map(x => x._2 )

    val key = resultRDD.map(result => Bytes.toString(result.getRow()))
    //key.take(2).foreach(kv => println(kv)) 

    //read individual column information from RDD
    val deregRDD = resultRDD.map(result => Row(Bytes.toString(result.getRow()),
      Bytes.toString(result.getValue(Bytes.toBytes("dereg"), Bytes.toBytes("dereg_date")))))


    // The schema is encoded in a string
    val schemaString = "upm_id dereg_date"

    // Generate the schema based on the string of schema
    val fields = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)
    // Apply the schema to the RDD
    val df = spark.createDataFrame(deregRDD, schema)

    //deregRDD.take(2).foreach(kv => println(kv)) 
        
    //Save output to Hadoop
    //deregRDD.saveAsTextFile(args(1))

    //Save output to Hadoop parquet
    df.write.mode("overwrite").parquet(s3_path)

    sc.stop()
 }
}
