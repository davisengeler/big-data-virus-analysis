/**
 * Created by jsmith on 10/14/15.
 */

package org.virus

import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model._
import com.amazonaws.{ClientConfiguration, Protocol}

import org.apache.log4j.{ Level, Logger }

import net.liftweb.json.JsonDSL._
import net.liftweb.json._


object Clustering {

  // Load configuration
  val AWS_ACCESS_KEY = AWSKeyInformation.AWS_ACCESS_KEY
  val AWS_SECRET_KEY = AWSKeyInformation.AWS_SECRET_KEY

  // Log levels
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF) 
  //AdjustLogLevel.setStreamingLogLevels()


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("selectedintrusion").setMaster("local")
    val sc = new SparkContext(conf)
    val hadoopConf = sc.hadoopConfiguration
    //set SC configuration to allow recursive reads of directories for files
    hadoopConf.set("mapreduce.input.fileinputformat.input.dir.recursive","true")
    //set SC configuration to use NativeS3FileSystem
    hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    //set SC configuration with S3 credentials
    hadoopConf.set("fs.s3.awsAccessKeyId", AWS_ACCESS_KEY)
    hadoopConf.set("fs.s3.awsSecretAccessKey", AWS_SECRET_KEY)

    //Example S3 virus file bucket name: "vscanner-mappings"
    //file path for LIBSVM formatted file
    val virusBucketName = AWSBucketInformation.AWS_VIRUS_BUCKET
    val resultsBucketName = AWSBucketInformation.AWS_RESULTS_BUCKET

    //Create LabeledPoint RDD from LIBSVM formatted file stored on S#
    val rawData:RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, "s3://" + resultsBucketName + "/LIBSVM.txt")
    //val rawData:RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, "/home/jsmith/mappings.txt")

    //Convert LabeledPoint RDD to RDD of format (feature Label, Vector) to be used form computing best K value
    val rawDataFeatures = rawData.map(x => (x.label, x.features))


    // Get the clustering structure
    val rawClusterInfo = buildClustering(rawDataFeatures, 10)

    // For prepping the JSON:
    //
    // 3 - API Calls (most basic):      ("name","whateverID") ~ ("size",whateverSize)
    // 2 - Clean or virus in cluster:   ("name","cleanOrVirus") ~ ("children",collectionOfChildren)
    // 1 - Clusters in main group:      ("name","clusterName") ~ ("children",collectionOfVirusOrClean)
    // 0 - Main cluster:                ("name","main-cluster") ~ ("children",collectionOfClusters)
    // 
    // Final result:                    println(compact(render( 0 ~ 1 ~ 2 ~ 3 )))


    sc.stop()

  }

  def searchBestKWithUsingEntropy(rawData: RDD[(Double, Vector)]): Unit = {

    (5 to 100 by 5).map{k =>
      System.out.println("K is " + k)
      (k, buildClustering(rawData, k))}.toList.
      foreach(println)

  }//end of searchBestKWithUsingEntropy function


  // Builds a collection of clusters based on the KMeans predictions.
  def buildClustering(normalizedLabelsAndData: RDD[(Double,Vector)], k: Int): RDD[(Int, Double)] = {
    
    // Set up initial KMeans stuff
    val kmeans = new KMeans()
    kmeans.setK(k)
    kmeans.setRuns(10)
    kmeans.setEpsilon(1.0e-6)

    // TODO: Not sure what this is really doing. Magic?
    val model = kmeans.run(normalizedLabelsAndData.values)
    
    // Map the sample to the predictions (the cluster number for that sample).
    // Swap the keys and values (so the cluster ID points to the sample).
    // Returns the resulting RDD[(Int, Iterable[Double])]  NOTE: "Iterable" ends up being a CompactBuffer
    normalizedLabelsAndData.mapValues(model.predict).map(_.swap)

  }//end of buildClustering

  ///////////////////////////////
  //Using Labels with Entropy//
  ///////////////////////////////
  //entropy: (counts: Iterable[Int])  Double
  def entropy(counts: Iterable[Int]) = {
    val values = counts.filter(_ > 0)
    val n: Double = values.sum
    values.map { v =>
      val p = v / n
      -p * math.log(p)
    }.sum
  }//end of entropy function
}
