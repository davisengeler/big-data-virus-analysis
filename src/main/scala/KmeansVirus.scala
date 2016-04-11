/**
 * Created by jsmith on 10/14/15.
 */

package org.virus

import java.io._

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

    val myAWSCredentials = new BasicAWSCredentials(AWS_ACCESS_KEY, AWS_SECRET_KEY)
    val amazonS3Client = new AmazonS3Client(myAWSCredentials)

    //Example S3 virus file bucket name: "vscanner-mappings"
    //file path for LIBSVM formatted file
    val virusBucketName = AWSBucketInformation.AWS_VIRUS_BUCKET
    val resultsBucketName = AWSBucketInformation.AWS_RESULTS_BUCKET

    //Create LabeledPoint RDD from LIBSVM formatted file stored on S#
    val rawData:RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, "s3://" + resultsBucketName + "/LIBSVM.txt")
    rawData.cache

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

    //rawClusterInfo.foreach(println)

    val fileToUpload = new File("output.txt")
    val bw = new BufferedWriter(new FileWriter(fileToUpload))

    rawClusterInfo.toArray.foreach(x => { bw.write(x._1 + "," + x._2 + "\n") })
    bw.close()

    val putObjectRequest = new PutObjectRequest(AWSBucketInformation.AWS_RESULTS_BUCKET, "output.txt", fileToUpload)
    val acl = CannedAccessControlList.PublicRead
    putObjectRequest.setCannedAcl(acl)
    amazonS3Client.putObject(putObjectRequest)

    sc.stop()
  }

  // This doesn't return the best K, but instead only prints the results by 5
  // Requires furthor research https://en.wikipedia.org/wiki/Determining_the_number_of_clusters_in_a_data_set
  def searchBestKWithUsingEntropy(rawData: RDD[(Double, Vector)]): Unit = {

    (5 to 100 by 5).map{k =>
      System.out.println("K is " + k)
      (k, buildClustering(rawData, k))}.toList.
      foreach(println)

  }

  // Builds a collection of clusters based on the KMeans predictions.
  def buildClustering(normalizedLabelsAndData: RDD[(Double,Vector)], k: Int): RDD[(Int, Double)] = {
    
    // Set up initial KMeans stuff
    val kmeans = new KMeans()
    kmeans.setK(k)
    kmeans.setRuns(10)
    kmeans.setEpsilon(1.0e-6)

    // Train a K-means model on the given set of points; data should be cached for high performance, because this is an iterative algorithm.
    val model = kmeans.run(normalizedLabelsAndData.values)
    
    // Map the sample to the predictions (the cluster number for that sample).
    // Swap the keys and values (so the cluster ID points to the sample).
    // Returns the resulting RDD[(Int, Double)]
    normalizedLabelsAndData.mapValues(model.predict).map(_.swap)

  }

  //Using Labels with Entropy
  def entropy(counts: Iterable[Int]) = {
    val values = counts.filter(_ > 0)
    val n: Double = values.sum
    values.map { v =>
      val p = v / n
      -p * math.log(p)
    }.sum
  }
}
