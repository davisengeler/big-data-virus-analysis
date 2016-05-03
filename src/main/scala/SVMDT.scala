/**
 * Created by jsmith on 10/14/15.
 */

import java.io.ByteArrayInputStream

// investigate the impact of model parameters on performance
// create a training function
import org.apache.spark.mllib.optimization.Updater
import org.apache.spark.mllib.optimization.SimpleUpdater
import org.apache.spark.mllib.optimization.L1Updater
import org.apache.spark.mllib.optimization.SquaredL2Updater
import org.apache.spark.mllib.classification.ClassificationModel

import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.{Protocol, ClientConfiguration}
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.model.ObjectMetadata
import org.apache.spark.mllib.classification.{LogisticRegressionWithSGD, SVMWithSGD}
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ListBuffer

//Decison Tree
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.configuration.Algo
import org.apache.spark.mllib.tree.impurity.Entropy
import org.apache.spark.mllib.tree.impurity.Impurity
import org.apache.spark.mllib.tree.impurity.Gini

import org.apache.log4j.{ Level, Logger }


object driver {

	Logger.getLogger("org").setLevel(Level.OFF)
	Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("selectedintrusion").setMaster("local")
    val sc = new SparkContext(conf)
    val hadoopConf = sc.hadoopConfiguration
    //set SC configuration to allow recursive reads of directories for files
    hadoopConf.set("mapreduce.input.fileinputformat.input.dir.recursive","true")
    //set SC configuration to use NativeS3FileSystem
    hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    //set SC configuration with S3 credentials
    hadoopConf.set("fs.s3.awsAccessKeyId", "AKIAIBFP2AAUHINPILYA")
    hadoopConf.set("fs.s3.awsSecretAccessKey", "xHcHUH+g8WDkKW+kmPY6Nhz29eXz5BI0WvudfmWu")

    //Example S3 virus file bucket name: "vscanner-mappings"
    //file path for LIBSVM formatted file
    //val virusBucketName = args(0)

    //Example S3 mappings bucket name: "vscanner-mappings"
    //file path for storing mappings file
    //val mappingsBucketName = args(0)

    val tempList = new ListBuffer[String]

    //Create LabeledPoint RDD from LIBSVM formatted file stored on S#
    //val rawData:RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, "s3://" + virusBucketName + "/mappings.txt").persist()
    val rawData:RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, "LIBSVMOutput.txt").persist()

    //Convert LabeledPoint RDD to RDD of format (feature Label, Vector) to be used form computing best K value
    val rawDataFeatures = rawData.map(x => (x.label, x.features))

    //Call function to compute best K value
    //val bestK = searchBestKWithUsingEntropy(rawDataFeatures)

    //RDD:Vector of features only
    val rddFeatures = rawData.map(x => x.features)

    //RDD:(String,Vector) contains (file label, features)
    val rddLabelandFeatures = rawData.map(x => (this.labelToString(x.label), x.features))


    //calculate clustering
    //this.clusteringTake0(rddFeatures, rddLabelandFeatures, tempList)


    /*Creates Array of RDD with rawData RDD split into three parts
* --splitRDD(0) = training data set that is 50% of original rawData
* --splitRDD(1) = cross validation data set that is 25% of original rawData
* --splitRDD(2) = testing data set that is 25% of original rawData
*/
    val splitRDD = rawData.randomSplit(Array(0.5, 0.25, 0.25))
    val trainingData = splitRDD(0).cache()
    val cvData = splitRDD(1).cache()
    val testData = splitRDD(2).cache()

    //call checkTreeDepth using Entropy
    this.checkTreeDepth("Entropy", Entropy, trainingData, cvData)

    //call checkTreeDepth using Gini
    this.checkTreeDepth("Gini", Gini, trainingData, cvData)

    //call checkCVRegulation using SVM
    checkCVRegulation('3', "SVM", trainingData, cvData)

    //empty string to store tempList contents so buffer and write them to S3
    var tempString = ""

    /*
    //loop through elements of list and write to string to be streamed to S3
    for (x<- tempList) {
      tempString += (x + "\n")
    }

    //create stream object to be used to store data being written to S3
    val s3Writer = new ByteArrayInputStream(tempString.getBytes)

    //create MetaData object that will be passed to function to write mapping data to S3
    val meta = new ObjectMetadata()

    //set length of buffer that will be passed into meta object.
    meta.setContentLength(tempString.length.toLong)

    //write data to s3 File
    this.putString(mappingsBucketName, s3Writer, meta)
    */

    sc.stop()

  }

  //return org.apache.spark.mllib.tree.model.DecisionTreeModel
  def trainDTWithParams(input: RDD[LabeledPoint], maxDepth: Int, impurity: Impurity)
  =  {
    /***
    input: Training dataset RDD of LabeledPoint.
    algo: algorithm, classification or regression
    impurity: impurity criterion used for information gain calculation
    maxDepth: Maximum depth of the tree. E.g., depth 0 means 1 leaf node; depth 1 means
              1 internal node + 2 leaf nodes.
    returns DecisionTreeModel that can be used for prediction
      ***/
    DecisionTree.train(input, Algo.Classification, impurity, maxDepth)
  }

  //investigate tree depth impact
  def checkTreeDepth(AlgorName: String, impuritySelection: Impurity,
                     inputData: RDD[LabeledPoint], cvData: RDD[LabeledPoint]): Unit = {

    //dtResultsEntropy: Seq[(String, Double)]
    val dtResultsEntropy = Seq(1, 2, 3, 4, 5, 10, 20).map { param =>
      val model = trainDTWithParams(inputData, param, impuritySelection)
      val scoreAndLabels = cvData.map { point =>
        val score = model.predict(point.features)
        (score, point.label)
      }
      val metrics = new BinaryClassificationMetrics(scoreAndLabels)
      (s"$param tree depth", metrics.areaUnderROC)
    }
    System.out.println("DT " + AlgorName)

    dtResultsEntropy.foreach {
      case (param, auc) => println(f"$param, AUC = ${auc * 100}%2.2f%%") }
  }//end of decision tree

  /***
  return (String, Double)
  helper function to take the input data and a classification model and generate
  the relevant AUC metrics
    ***/
  def createMetrics(label: String, data: RDD[LabeledPoint],
                    model: ClassificationModel)  = {
    //System.out.println(model.getClass.getSimpleName)
    val scoreAndLabels = data.map { point =>
      (model.predict(point.features), point.label)
    }
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    (label, metrics.areaUnderROC)
  }

  /***
   now we train our model using the 'train' dataset, and compute predictions on
   unseen 'test' data. in addition, we will evaluate the differing performance of
   regularization on training and test datasets
    ***/
  def checkCVRegulation(myModelSelection: Char, AlgorName: String,
                        trainData: RDD[LabeledPoint], testData: RDD[LabeledPoint]): Unit = {
    val myNumIterations = 10
    val regResultsTest = Seq(0.001, 0.01, 0.1, 1.0, 10.0).map { param =>
      //val regResultsTest = Seq(0.0, 0.001, 0.0025, 0.005, 0.01).map { param =>
      val model = trainWithParams(myModelSelection,trainData, param,
        myNumIterations, new SquaredL2Updater, 1.0)
      createMetrics(s"$param L2 regularization parameter", testData, model)
    }
    System.out.println("CV " + AlgorName)
    regResultsTest.foreach {
      case (param, auc) => println(f"$param, AUC = ${auc * 100}%2.6f%%") }
  }//end of checkCVRegulation

  /***
  helper function to train a mode given a set of inputs
  return ClassificationModel
    ***/
  def trainWithParams(modelSelection: Char, input: RDD[LabeledPoint], regParam: Double,
                      numIterations: Int, updater: Updater, stepSize: Double) = {
    val trainObject = modelSelection match {
      case '1' => new LogisticRegressionWithSGD
      case '3' => new SVMWithSGD()
    }

    trainObject.optimizer.setNumIterations(numIterations).
      setUpdater(updater).setRegParam(regParam).setStepSize(stepSize)
    trainObject.run(input)
  }
  //function to write string to S3 bucket
  def putString(bucketName: String, inStream: ByteArrayInputStream, meta: ObjectMetadata) = {
    //create credential object to be used to access S3 buckets to retrieve names of files in buckets
    val credentials = new BasicAWSCredentials("AKIAIBFP2AAUHINPILYA", "xHcHUH+g8WDkKW+kmPY6Nhz29eXz5BI0WvudfmWu")
    //creates configuration settings to be passed to AmazonS3Client object that will read S3 contents
    val clientConfig = new ClientConfiguration()
    clientConfig.setProtocol(Protocol.HTTPS)
    val conn = new AmazonS3Client(credentials, clientConfig)

    //write string to S3
    conn.putObject(bucketName, "clusterResults.txt", inStream, meta)
  }

  //convert Double label type to String so 1.0 = Virus, 0.0 = Clean
  def labelToString(rawLabel: Double): String = {
    //temp var to store Label text value
    var tempString = ""
    if(rawLabel == 0.0){
      tempString = "Clean"
    }else{
      tempString = "Virus"
    }
      tempString
  }

  def searchBestKWithUsingEntropy(rawData: RDD[(Double, Vector)]): Unit = {

    (30 to 160 by 10).map{k =>
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

  // Clustering, Take 0
  //this method contain some useful code to figure out what kind of data
  //is present in each cluster
  //myData only contain feature vector
  //myLableData contain both label and feature vector
  def clusteringTake0(myData: RDD[Vector], myLabelData:RDD[(String,Vector)], tempList: ListBuffer[String]): ListBuffer[String] = {

    //The following code clusters the data to create a KMeansModel, and then prints
    //its centroids. By default, K-means was fitting k = 2 clusters to the data.
    //kmeans: org.apache.spark.mllib.clustering.KMeans
    val kmeans = new KMeans()
    //set K to value computed by searchBestKWithUsingEntropy function.
    kmeans.setK(60)
    //model: org.apache.spark.mllib.clustering.KMeansModel
    val model = kmeans.run(myData)
    // res0: Array[org.apache.spark.mllib.linalg.Vector]
    model.clusterCenters.foreach(println)

    //This is a good opportunity to use the given labels to get an intuitive sense
    // of    what went into these two clusters, by counting the labels within each
    // cluster. The following code uses the model to assign each data point to a
    //cluster, counts occurrences of cluster and label pairs, and prints them nicely

    //label is virus or clean feature type
    //clusterLabelCount: scala.collection.Map[(Int, String),Long]
    val clusterLabelCount = myLabelData.map { case (label, datum) =>
      val cluster = model.predict(datum)
      (cluster, label)
    }.countByValue().toSeq.sortBy(_._1._1).map(x => "Cluster " + x._1._1.toString + " contains " + x._2.toString + " " + x._1._2.toString + " files.")
    /*
    clusterLabelCount.toSeq.sorted.foreach {
      case ((cluster, label), count) => println(s"Cluster $cluster contains $count $label files").toString

    }
    */
    clusterLabelCount.copyToBuffer(tempList)
    tempList



  }//end of clusteringTake0 method
}
