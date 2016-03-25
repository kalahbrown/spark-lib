package com.bigfishgames.spark.lib

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._


import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.AvroKeyInputFormat
import org.apache.avro.mapreduce.AvroKeyOutputFormat
import org.apache.avro.mapreduce.AvroJob

import org.apache.log4j.Logger;

import java.text.SimpleDateFormat
import java.util.Calendar

//https://github.com/massie/adam/blob/master/adam-commands/src/main/scala/edu/berkeley/cs/amplab/adam/serialization/AdamKryoRegistrator.scala
//
//You need to register all of you Avro SpecificClasses with Kryo and have it
//use the AvroSerializer class to encode/decode them.
//> The problem is that spark uses java serialization requiring serialized
//> objects to implement Serializable, AvroKey doesn't.
//https://ogirardot.wordpress.com/2015/01/09/changing-sparks-default-java-serialization-to-kryo/
//https://blogs.msdn.microsoft.com/bigdatasupport/2015/09/14/understanding-sparks-sparkconf-sparkcontext-sqlcontext-and-hivecontext/

//TODO: This writes a single file to disk, but the path is /gt_event/part-r-00000.avro, this is because it is writing a Hadoop File and using the Avro MR libs
//  the next area to explore is write Object, can this be done to just serialize an RDD.
//TODO: Add the ability to read schema from HDF

object AvroIO {
  
  val logger = Logger.getLogger(this.getClass.getName)
  
  private def createStreamingContext = {
    val conf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer", "24")
      .set("HADOOP_HOME", System.getenv().get("HADOOP_HOME"))
      .setAppName("read avro")

    new StreamingContext(conf, Seconds(10))
  }

  private def parseAvroSchema = {
    val schemaStr = """{                              
              "type": "record",                                            
              "name": "RPT_GT_EVENT_STREAM",                                         
              "fields": [      
              	      {"name": "evtt", "type": ["null","string"]},                    
                      {"name": "tss", "type": ["null","long"]},  
                      {"name": "tsc", "type": ["null","long"]},  
                      {"name": "tzo", "type": ["null","int"]},   
                      {"name": "bfg", "type": ["null","string"]},           
                      {"name": "rid", "type": ["null","string"]},  
                      {"name": "chid", "type": ["null","long"]},   
                      {"name": "plt", "type": ["null","string"]},     
                      {"name": "ver", "type": ["null","string"]},   
                      {"name": "ip", "type": ["null","string"]},     
                      {"name": "bld", "type": ["null","string"]},     
                      {"name": "snid", "type": ["null","string"]},     
                      {"name": "psnid", "type": ["null","string"]},     
                      {"name": "snn", "type": ["null","int"]},                    
                      {"name": "data", "type":{"type":"string","java-class":"com.bigfishgames.biginsights.serialization.GtData"}}, 
                      {"name": "headers", "type": ["null","string"], "default": null}, 
                      {"name": "tsl", "type": ["null","long"], "default": null} 
                      ] }"""

    val parse = new Schema.Parser();
    parse.parse(schemaStr)
  }

  private def createAvroJob = {
    val job = Job.getInstance
    AvroJob.setOutputKeySchema(job, parseAvroSchema)
    job
  }

  def readAvroStream(ssc: StreamingContext) = {
    val inputDirectory = "hdfs://bi-mgmt02.corp.bigfishgames.com:8020/user/kalah.brown/spark_stream_test/watch/"
    ssc.fileStream[AvroKey[GenericRecord], NullWritable, AvroKeyInputFormat[GenericRecord]](inputDirectory + currentDate)
  }

  def writeAvroHadoopFile(avroRdd: RDD[(AvroKey[GenericRecord], NullWritable)]) = {
    avroRdd.reduceByKey((key, value) => key)
      .saveAsNewAPIHadoopFile("/user/kalah.brown/spark_stream_test/final/gt_event", classOf[AvroKey[GenericRecord]], classOf[NullWritable], classOf[AvroKeyOutputFormat[GenericRecord]], createAvroJob.getConfiguration)

  }

  def currentDate = {
    val today = Calendar.getInstance.getTime
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    dateFormat.format(today)
  }

  def main(args: Array[String]) {
    val ssc = createStreamingContext

    val avroStream = readAvroStream(ssc)

    avroStream.foreachRDD(rdd => {
      if (!rdd.partitions.isEmpty)
        writeAvroHadoopFile(rdd)
    })

    ssc.start()
    ssc.awaitTermination()
  }

}