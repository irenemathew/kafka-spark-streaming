package com.twitter.ingest
import com.twitter.tweet.Tweet.TweetData
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.common.serialization.StringDeserializer

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf

object StreamingIngest {
  val logger = Logger.getLogger("StreamingIngest")
  def main (args: Array[String]): Unit ={

    val hbaseTable="TWEETS"
    // set up HBase Table configuration
    val conf = HBaseConfiguration.create()
    conf.set(TableOutputFormat.OUTPUT_TABLE, hbaseTable)
    val jobConfig: JobConf = new JobConf(conf, this.getClass)
    jobConfig.set("mapreduce.output.fileoutputformat.outputdir", "/tmp/out")
    jobConfig.setOutputFormat(classOf[TableOutputFormat])
    jobConfig.set(TableOutputFormat.OUTPUT_TABLE, hbaseTable)


    val sparkConf = new SparkConf().setAppName("Twitter Ingest Data")
    sparkConf.setIfMissing("spark.master", "local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    val kafkaTopics = "sentiment"
    val kafkaBroker = "kafka:9092"

    val topics : Set[String] = kafkaTopics.split(",").map(_.trim).toSet
    val kafkaParams = Map[String,Object](
      "bootstrap.servers" -> kafkaBroker,
              "group.id" -> "spark-streaming",
              "key.deserializer" -> classOf[StringDeserializer],
              "value.deserializer" -> classOf[StringDeserializer]
    )

    logger.info("Connecting to broker...")
    logger.info(s"kafkaParams: $kafkaParams")

    val tweetStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))

    logger.info("Created InputDStream : Parsing starting....")

    val parsedTweetStream : DStream[TweetData] = ingestTweetStream(tweetStream)

    logger.info("Parsing completed....")


    parsedTweetStream.foreachRDD { rdd =>
      // convert tweet data to put object and write to HBase table column family data
      rdd.map(convertToPut(_)).
        saveAsHadoopDataset(jobConfig)
    }

    logger.info("HBase insert completed....")


    ssc.start()
    ssc.awaitTermination()
  }

  //  Convert a row of tweet object data to an HBase put object
  def convertToPut(tweet: TweetData): (ImmutableBytesWritable, Put) = {
    val rowkey = tweet.user
    val put = new Put(Bytes.toBytes(rowkey))
    // add to column family C, column data values to put object
    put.add(Bytes.toBytes("C"), Bytes.toBytes("sentiment"), Bytes.toBytes(tweet.sentiment))
    put.add(Bytes.toBytes("C"), Bytes.toBytes("text"), Bytes.toBytes(tweet.text))
    return (new ImmutableBytesWritable(Bytes.toBytes(rowkey)), put)
  }

  def ingestTweetStream(tweetStream: InputDStream[org.apache.kafka.clients.consumer.ConsumerRecord[String,String]]):  DStream[TweetData] = {
    val parsedTweetStream = tweetStream.map(_.value.split(","))
      .map(TweetData(_))
    parsedTweetStream
  }


}
