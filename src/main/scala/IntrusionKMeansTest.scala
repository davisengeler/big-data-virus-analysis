//Updated 8/14/2015
//Total time: 666 s, 
//run sampling script to smaller dataset
//Link to download dataset http://kdd.ics.uci.edu/databases/kddcup99/kddcup99.html
//mv /home/jetzhong/Downloads/kddcup.data_10_percent.gz  .
//gunzip kddcup.data_10_percent.gz
//gunzip corrected.gz
//sbt package
//spark-submit \
//   --class "IntrusionKMeans" \
//   target/scala-2.10/scala-clustering-app_2.10-1.0.jar


import org.apache.spark.mllib.clustering._
//import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

import scala.collection.mutable.ArrayBuffer

import net.liftweb.json.JsonDSL._
import net.liftweb.json._

import scala.util.Random

import java.io._
import org.apache.spark.streaming.{Seconds,StreamingContext}
import org.apache.spark.mllib.regression.{StreamingLinearRegressionWithSGD, LabeledPoint}
import java.text.{SimpleDateFormat, DateFormat}
import java.util.Date

import com.amazonaws.services.s3.model.{CannedAccessControlList, PutObjectRequest}
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.auth.BasicAWSCredentials

// import io.keen.client.scala.{ Client, Writer }

//import org.apache.log4j.{Level, Logger}
object IntrusionKMeans {
   def main(args: Array[String]): Unit = {

  // AMAZON S3 Credentials
  val AWS_ACCESS_KEY = "ACCESS_KEY_HERE"
  val AWS_SECRET_KEY = "SECRET_KEY_HERE"

  // Create Amazon S3 client
  val myAWSCredentials = new BasicAWSCredentials(AWS_ACCESS_KEY, AWS_SECRET_KEY)
  val amazonS3Client = new AmazonS3Client(myAWSCredentials)

   //this does not work for sbt run work for spark submit
   //Logger.getRootLogger.setLevel(Level.WARN)
   //change the log level
   AdjustLogLevel.setStreamingLogLevels()

    val conf = new SparkConf()
        .setMaster("local[2]") 
        .setAppName("Midterm NetConsumer")
        .set("spark.executor.memory", "2g")

    // Context for Batch and Streaming, the Streaming context
    // will share the same one as the Batch
    var sc: SparkContext = new SparkContext(conf)
    var ssc: StreamingContext = new StreamingContext(sc, Seconds(10))

    // File to be used for the Batch KMeans Model
    val rawData = sc.textFile("training")
   
   //RDD[(String, org.apache.spark.mllib.linalg.Vector)]
   //Each element of RDD is the label and data vector.
   //RDD[(String, org.apache.spark.mllib.linalg.Vector)]
   //this RDD removes the three categorical value columns starting from index 1
   val batchLabelsAndDataVector = rawData.map { line =>
        val buffer = line.split(',').toBuffer
        buffer.remove(1, 3)
        val label = buffer.remove(buffer.length - 1)
        val vector = Vectors.dense(buffer.map(_.toDouble).toArray)
        (label, vector)
   }
    
   batchLabelsAndDataVector.cache()
   val batchDataVector = batchLabelsAndDataVector.values.cache()
  
   // create nomalization function to normalize input data   
   val normalizeFunction = buildNormalizationFunction(batchDataVector)

   // create the Batch KMeans Model
   val batchKMModel = buildBatchKMeansModel(batchDataVector, normalizeFunction)

   // create the Anomaly Detector function, will return true/false
   val anomalyDetector = buildAnomalyDetector(batchKMModel, batchDataVector, normalizeFunction)

   // STREAMING SECTION
   // streaming context is main entry point for all streaming functionality
   println("Connecting to the server...")
   val stream = ssc.socketTextStream("localhost", 9999)
   
   //ssc.checkpoint("./checkpoint/")

    // create a stream of labeled points
   val labeledDataStream = stream.map { line =>
          val buffer = line.split(',').toBuffer
          buffer.remove(1, 3)
          val label = buffer.remove(buffer.length - 1)
          val vector = Vectors.dense(buffer.map(_.toDouble).toArray)
          (label, vector)
        }

   // return only the vector data from the stream
   val dataStream = labeledDataStream.map { case (label, vector ) => vector }

   // KMeans variables, k, initial weight, and weightvector
   val k = 130
   val weight = 100.0
   val weightVector = Array.fill(k)(weight)
   
   // create Streaming KMeans Model based on information learned from the Batch
   val streamingKMModel = new StreamingKMeans()
   streamingKMModel.setDecayFactor(1.0)
   streamingKMModel.setK(k)
   streamingKMModel.setInitialCenters(batchKMModel.clusterCenters, weightVector)
   streamingKMModel.trainOn(dataStream) 

    // perform actions on the streaming data
    labeledDataStream.foreachRDD { (rdd, time) =>
          // return the vector information from the stream without the label
          val dataRDD = rdd.map { case(label, data) => data }

          // normalize streaming data based on normalization function created in the batch
          val normalizedDataRDD = dataRDD.map(normalizeFunction)

          // return the current streaming model for predictions
          val latestModel = streamingKMModel.latestModel()

          // calculate distance score of the current model / clusters
          val distanceScore = costlyAveDisToCentroidScore(normalizedDataRDD, latestModel)

          // calculate entropy score of the current model / clusters
          val entropyScore = costlyWeightedAveEntropyScore(rdd, latestModel)

          // get anomalies based on latest model
          val anomalies = rdd.filter { case(label, data) => anomalyDetector(data) }
          
          // remove the period at the end of the label and create a count of each type of anomaly
          val labeledAnomalies = anomalies.map { case(label, data) => label.stripSuffix(".") }.countByValue()

        //
        // SECTION FOR JSON OUTPUT
        //

        // predict the cluster for each item in the rdd, and then store
        // with the label and counts of each
        val clusterLabelCount = rdd.map { 
           case (label, datum) =>
           val cluster = latestModel.predict(datum)
           (cluster, label)
         }.countByValue().toSeq.sortBy(_._1)

        //
        // This is building the JSON output format
        //
        // The Data tag will hold:
        // cluster-data: the cluster, label, count information after the streamed data is
        // placed into a cluster through a prediction based on latest Kmeans model
        //
        // cluster-stats: this will contain the current distance score and entropy score 
        // for the overall cluster.  Also anomalies that are detected will be put in this
        // section, and the # of each will be put as it's value.
        // 

        val json =
        ("data" ->
            ("cluster-data" -> 
              clusterLabelCount.map { case ((cluster, label), count) =>
              ("cluster" -> cluster) ~ ("label" -> label.stripSuffix(".")) ~ ("value" -> count) }) ~
            ("cluster-stats" ->
              ("distance" -> ("label" -> "distance-score") ~ ("value" -> distanceScore)) ~
              ("entropy" -> ("label" -> "entropy-score") ~ ("value" -> entropyScore)) ~
              ("anomalies" -> labeledAnomalies.map { case (label, count) =>
                (("label" -> label) ~ ("value" -> count))})))

        // This is the file output sent to the console for debug purposes
        println("\nCluster Information: ")
        println("\n" + pretty(render(json)))

        // create a file and store on the local 
        val fileToUpload = new File("output.txt")
        val bw = new BufferedWriter(new FileWriter(fileToUpload))
        bw.write(pretty(render(json)))
        bw.close() 

        // create s3 upload request
        val putObjectRequest = new PutObjectRequest("csbigdata", "output.txt", fileToUpload)

        // assign file to be public readable
        val acl = CannedAccessControlList.PublicRead
        putObjectRequest.setCannedAcl(acl)

        // upload file
        amazonS3Client.putObject(putObjectRequest)
    }

   ssc.start()
   ssc.awaitTermination()  

   sc.stop()

   }//END OF MAIN

  def buildBatchKMeansModel(data: RDD[Vector], normalizeFunction: (Vector => Vector)): KMeansModel = {
    val normalizedData = data.map(normalizeFunction)
    normalizedData.cache()
    val kmeans = new KMeans()
    kmeans.setK(130)
    kmeans.setRuns(10)
    kmeans.setEpsilon(1.0e-6)
    val model = kmeans.run(normalizedData)
    normalizedData.unpersist()
    model
  }

  def clusteringLabelMakeup(model: KMeansModel, myLabelData:RDD[(String,Vector)]): Unit = {
      //label is 23 distinct network attack type or normal
      //clusterLabelCount: scala.collection.Map[(Int, String),Long] 
      val clusterLabelCount = myLabelData.map { case (label, datum) =>
          val cluster = model.predict(datum)
          (cluster, label)
      }.countByValue()
      clusterLabelCount.toSeq.sorted.foreach { 
         case ((cluster, label), count) => println(f"$cluster%1s$label%18s$count%8s")
      }
  }

   //distance: (a: org.apache.spark.mllib.linalg.Vector, 
   //b: org.apache.spark.mllib.linalg.Vector) Double
   //return Euclidean distance between two vectors, which is the double
   def distance(a: Vector, b: Vector) =
     math.sqrt(a.toArray.zip(b.toArray).
        map(p => p._1 - p._2).map(d => d * d).sum)

  //return distance between the data point and its nearest cluster's centroid
  //which is double
  def distToCentroid(datum: Vector, model: KMeansModel) = {
    //cluster: int  this is cluster to which a sample belongs to
    val cluster = model.predict(datum)
    val centroid = model.clusterCenters(cluster)
    distance(centroid, datum)
  }

  def calculateDistanceScore(data: RDD[Vector], model: KMeansModel): Double = {
    data.map(datum => distToCentroid(datum, model)).mean()
  }
  
  //set number of iteration and minimum convergence condition
  //average distance to the closest centroid as the clustering score
    def costlyAveDisToCentroidScore(data: RDD[Vector], newModel:KMeansModel): Double = {
        data.map(datum => distToCentroid(datum, newModel)).mean()
    }

  /////////////////////////
  //FEATURE NORMALIZATION//
  /////////////////////////
  //this will  function. Given the input vector
  //this will produce the vector with each dimension normalized
  def buildNormalizationFunction(data: RDD[Vector]): (Vector => Vector) = {
    //RDD[Array[Double]]  Basically this is 2-D array
    val dataAsArray = data.map(_.toArray)
    //numCols: Int number of column
    val numCols = dataAsArray.first().length
    //n: Long   total number of samples
    val n = dataAsArray.count()
    //sums: Array[Double]
    //each element is total sum for one attribute or dimension
    val sums = dataAsArray.reduce(
        (a, b) => a.zip(b).map(t => t._1 + t._2))
    //sumSquares: Array[Double]  sum-of-square for each feature
    val sumSquares = dataAsArray.fold(
        new Array[Double](numCols)
    )(
        (a, b) => a.zip(b).map(t => t._1 + t._2 * t._2)
      )
    
    //stdevs: Array[Double] 
    //standard deviation for each attribute
    val stdevs = sumSquares.zip(sums).map {
        case (sumSq, sum) => math.sqrt(n * sumSq - sum * sum) / n
    }
    //means: Array[Double]
   //means for each attribute
   val means = sums.map(_ / n)
   //Define function: Given the input vector
   //this will produce the vector with each dimension normalized
   (datum: Vector) => {
       val normalizedArray = (datum.toArray, means, stdevs).zipped.map(
           (value, mean, stdev) =>
               if (stdev <= 0)  (value - mean) else  (value - mean) / stdev
       )
       Vectors.dense(normalizedArray)
   } 
 }//end of buildNormalizationFunction


  ///////////////////
  //Including CATEGORICAL VARIALBE//
  ///////////////////////
  //this function will include the categorical variable
  //this function will return the function. The returned function takes string of each
  // line and return label and unnormalized data vector for each line
  //buildCategoricalAndLabelFunction: (rawData: org.apache.spark.rdd.RDD[String])
  //    String => (String, org.apache.spark.mllib.linalg.Vector)
  def includeCategoricalFeatureAndLabel(rawData: RDD[String]): (String => (String,Vector)) = {
    
    val splitData = rawData.map(_.split(','))
    //produce the mapping for each protocols
    // protocols: scala.collection.immutable.Map[String,Int] =
    //Map(tcp -> 0, icmp -> 1, udp -> 2)    
   // The zipWithIndex method returns a list of pairs where the second
   //component is the index of each element
    val protocols =  
        splitData.map(_(1)).distinct().collect().zipWithIndex.toMap
    val services =  
        splitData.map(_(2)).distinct().collect().zipWithIndex.toMap
    val tcpStates = 
        splitData.map(_(3)).distinct().collect().zipWithIndex.toMap

    (line: String) => {
        // line.split(',')   res0: Array[String]
        // buffer: scala.collection.mutable.Buffer[String]
        val buffer = line.split(',').toBuffer
        val protocol = buffer.remove(1)
        val service = buffer.remove(1)
        val tcpState = buffer.remove(1)
        val label = buffer.remove(buffer.length - 1)
        // vector: scala.collection.mutable.Buffer[Double]
        val vector = buffer.map(_.toDouble)
        val newProtocolFeatures = new Array[Double](protocols.size)
        newProtocolFeatures(protocols(protocol)) = 1.0
        val newServiceFeatures = new Array[Double](services.size)
        newServiceFeatures(services(service)) = 1.0
        val newTcpStateFeatures = new Array[Double](tcpStates.size)
        newTcpStateFeatures(tcpStates(tcpState)) = 1.0
        vector.insertAll(1, newTcpStateFeatures)
        vector.insertAll(1, newServiceFeatures)
        vector.insertAll(1, newProtocolFeatures)
        (label, Vectors.dense(vector.toArray))
    }
  }//end of includeCategoricalFeatureAndLabel function to include categoriacal feature

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
  
  //weighted average of entropy can be used as a cluster score
  //input of this function will RDD. Each element of RDD is label and normalized data
  //return Double value
  def costlyWeightedAveEntropyScore(normalizedLabelsAndData: RDD[(String,Vector)], model: KMeansModel) = {
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

  /////////////////////////////////////////////////////////
  ///clustering in Action. Detect Anomalous samples////
  ////////////////////////////////////////////////////////
  // Detect anomalies
  //input of RDD[Vector], which only contains data without
  //label
  //normalizeFunction: given vector, produce normalize entry for vector
  //this will return a function.  Given vector, this function will tell
  //whether the given data is anomalous or not.
  def buildAnomalyDetector(
     model: KMeansModel,
     data: RDD[Vector],
     normalizeFunction: (Vector => Vector)): (Vector => Boolean) = {
    val normalizedData = data.map(normalizeFunction)
     //RDD[Double]
    //each element is distance to the closest centroid for all data
    val distances = normalizedData.map(datum => distToCentroid(datum, model))
    //pick 100th farthest data point from among known data
    //top function from RDD
    //def top(num: Int)(implicit ord: Ordering[T]): Array[T]
    //Returns the top k (largest) elements from this RDD as defined by the specified 
     //implicit Ordering[T]. This does the opposite of takeOrdered.
    val threshold = distances.top(100).last
    (datum: Vector) => distToCentroid(normalizeFunction(datum), model) > threshold
  }//end of buildAnomalyDetector function
  
  //this function will print out the anomoulous data
  def anomalies(rawData: RDD[String], model: KMeansModel) : Unit = {
    // parseFunction: String => (String, org.apache.spark.mllib.linalg.Vector)
     val parseFunction = includeCategoricalFeatureAndLabel(rawData)
    //in order to interpret the results, we keep the original line of input
    //with the parsed feature vector
    // originalAndData: RDD[(String, org.apache.spark.mllib.linalg.Vector)]
    //each line is the key and value is data vector
    val originalAndData = rawData.map(line => (line, parseFunction(line)._2))
    val data = originalAndData.values
    val normalizeFunction = buildNormalizationFunction(data)
    val anomalyDetector = buildAnomalyDetector(model, data, normalizeFunction)

   // anomalies: org.apache.spark.rdd.RDD[String]
   val anomalies = originalAndData.filter {
        case (original, datum) => anomalyDetector(datum)
    }.keys
    //anomalies.take(10).foreach(println)
    anomalies.foreach(println)
    //print label only
    anomalies.map(_.split(',').last).foreach(println)
    System.out.println("total anomalous samples " + anomalies.count)
  }//end of anomalies function
}//end of object
