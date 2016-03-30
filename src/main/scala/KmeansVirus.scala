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


object driver {

  // Load configuration
  val AWS_ACCESS_KEY = AWSKeyInformation.AWS_ACCESS_KEY
  val AWS_SECRET_KEY = AWSKeyInformation.AWS_SECRET_KEY


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

    //Call function to compute best K value
    val bestK = searchBestKWithUsingEntropy(rawDataFeatures)




    sc.stop()

  }

  def searchBestKWithUsingEntropy(rawData: RDD[(Double, Vector)]): Unit = {

    (5 to 100 by 5).map{k =>
      System.out.println("K is " + k)
      (k, costlyWeightedAveEntropyScore(rawData, k))}.toList.
      foreach(println)

  }//end of searchBestKWithUsingEntropy function


  //weighted average of entropy can be used as a cluster score
  //input of this function will RDD. Each element of RDD is label and normalized data
  //return Double value
  def costlyWeightedAveEntropyScore(normalizedLabelsAndData: RDD[(Double,Vector)],
                                    k: Int) = {
    val kmeans = new KMeans()
    kmeans.setK(k)
    kmeans.setRuns(10)
    kmeans.setEpsilon(1.0e-6)
    val test = normalizedLabelsAndData.collect()
    val model = kmeans.run(normalizedLabelsAndData.values)
    // Predict cluster for each datum
    // rdd.RDD[(String, Int)]  each element is the label and cluster num
    //  it belongs to
    val labelsAndClusters = normalizedLabelsAndData.mapValues(model.predict)
    // Swap keys / values
    //rdd.RDD[(Int, String)]
    //lable is 23 type of attacks or normal
    val clustersAndLabels = labelsAndClusters.map(_.swap)
    // Extract collections of labels, per cluster
    // Key is cluster ID
    // rdd.RDD[Iterable[String]]
    //each element is all labels for one cluster
    val labelsInCluster = clustersAndLabels.groupByKey().values
    // Count labels in collections
    //RDD[scala.collection.immutable.Iterable[Int]]
    //each element is total count of each label for each cluster
    val labelCounts = labelsInCluster.map(_.groupBy(l => l).map(_._2.size))
    //n: Long = 494021
    // total sample size
    val n = normalizedLabelsAndData.count()
    // Average entropy weighted by cluster size
    //m is total count for each label for one cluster
    //entropy(m) calcuate entropy for one cluster
    labelCounts.map(m => m.sum * entropy(m)).sum / n
  }//end of costlyWeightedAveEntropyScore

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
