//Date: 9/14/2015

/***
Tutorial build an Uber JAR file (aka Fat JAR, Super JAR).
http://stackoverflow.com/questions/28459333/how-to-build-an-uber-jar-fat-jar-using-sbt-within-intellij-idea

I created my project with the following file structure :
my-project/project/assembly.sbt
my-project/src/main/scala/myPackage/MyMainObject.scala
my-project/build.sbt

Added the sbt-assembly plugin in my assembly.sbt file. Allowing me to build a fat JAR :
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.12.0")

sbt assembly

### empty bucket before running the 

virusdetection-clean-temp
virusdetection-virus-temp
virusdetection-results

spark/bin/spark-submit \
   --class "FeatureSelectionCloud" \
   --master local \
   --executor-memory 1g \
   target/scala-2.10/vScannerAWS-assembly-1.0.jar

   virusdetection-virus virusdetection-clean 100 virusdetection-results
***/

package org.virus

import java.io._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model._
import com.amazonaws.{ClientConfiguration, Protocol}

import org.apache.log4j.{Level, Logger}

object FeatureSelectionCloud {

  val AWS_ACCESS_KEY = AWSKeyInformation.AWS_ACCESS_KEY
  val AWS_SECRET_KEY = AWSKeyInformation.AWS_SECRET_KEY
  val LOCAL = ProjectConfig.RUN_LOCALLY;

  def main(args: Array[String]): Unit = {

    //change the log level

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
    //AdjustLogLevel.setStreamingLogLevels()

    val conf = new SparkConf()
        .setAppName("Final App")
        .setMaster("local[2]")

    val sc = new SparkContext(conf)
    sc.setCheckpointDir("./checkpoint")
    val hadoopConf = sc.hadoopConfiguration

    //set SC configuration to allow recursive reads of directories for files
    hadoopConf.set("mapreduce.input.fileinputformat.input.dir.recursive","true")
    
    //set SC configuration to use NativeS3FileSystem
    hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    
    //set SC configuration with S3 credentials
    hadoopConf.set("fs.s3.awsAccessKeyId", AWS_ACCESS_KEY)

    hadoopConf.set("fs.s3.awsSecretAccessKey", AWS_SECRET_KEY)

    /***
    Example S3 virus file bucket name: "virusdetection-virus"
    file path for source virus file hexdumps
    ***/
    //val virusBucketName = "vd-virus-small"
    val virusBucketName = AWSBucketInformation.AWS_VIRUS_BUCKET

    /***
    Example S3 clean file bucket name: "virusdetection-clean"
    file path for source clean file hexdumps
    ***/
    //val cleanBucketName = "vd-clean-small"
    val cleanBucketName = AWSBucketInformation.AWS_CLEAN_BUCKET

    //Number of features to be selected
    val featureNumber = 2000

    //Example S3 results file bucket name: "virusdetection-results"
    //file path for storing results file
    val resultsBucketName = AWSBucketInformation.AWS_RESULTS_BUCKET

    /***
    Stage 1
    Creates virus features, saves the unique features per file to disk, and returns a
    count of how many virus files were processed.  
    The vFileCount variable is used by the featureSelectionByInfoGain to 
    calculate info gain.

    def createByteFeatures(sc: SparkContext, FilePath: String,
                                       tempFilePath: String)
    ***/

    // Creates features
    val tempVirus = createByteFeatures(sc, virusBucketName)
    val tempClean = createByteFeatures(sc, cleanBucketName)

    // use s3
    val vFileCount = getFileNames(virusBucketName).length.toDouble 
    val cFileCount = getFileNames(cleanBucketName).length.toDouble 
    
    // use locl
    //val vFileCount = new File(virusBucketName).listFiles.length.toDouble 
    //val cFileCount = new File(cleanBucketName).listFiles.length.toDouble 

    /***
    Stage 2
    def countFeatures(sc: SparkContext, FilePath: String) : RDD[(String, Int)] = {
    Count number of times each features occurs in virus file 
    Count number of times each features occurs in clean file
    In the returned RDD, key is the feature and value is the number of times features
    occur at virus/clean file 
    ***/

    val countVirusRDD = countFeatures(sc, tempVirus).cache
    val countCleanRDD = countFeatures(sc, tempClean).cache

    //returned Array[(featureID, inforGainValue)]
    val topFeatures = featureSelectionByInfoGain(sc, vFileCount, vFileCount+cFileCount, featureNumber, countVirusRDD, countCleanRDD)

    // path and file to save topFeatures Array to disk
    val file = "topFeatures.txt"
    
    //create new StreamWriter to write topFeatures Array contents to disk
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))
    
    //loop through elements of topFeatures Array and write to disk
     for (x <- topFeatures) {
       writer.write(x + "\n")
     }
    
    // //close StreamWriter object
     writer.close()

    // //read file in to be transfered to S3
     val fileToUpload = new File(file)

    //save the topfeature.txt to AWS
     putFile(resultsBucketName, "topFeatures.txt", fileToUpload)
    
    //val test = createMappings(sc, "vd-virus", "virus")
    val features = sc.parallelize(topFeatures.map { case(feature, value) => feature }).cache

    // start LIBSVM Formatting
    val outputVirusLIBSVM = createLIBSVMFormat(sc, tempVirus, "virus", features)
    val outputCleanLIBSVM = createLIBSVMFormat(sc, tempClean, "clean", features)
    val outputCombinedLIBSVM = outputVirusLIBSVM ++ outputCleanLIBSVM

    val file2 = "LIBSVMOutput.txt"
    val writer2 = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file2)))
    for (x <- outputCombinedLIBSVM) {
      writer2.write(x + "\n")
    }

    writer2.close()

    //read file in to be transfered to S3
    val fileToUpload2 = new File(file2)

    //save the topfeature.txt to AWS
    putFile(resultsBucketName, "LIBSVM.txt", fileToUpload2)
  }

//function to write file to S3 bucket
def putFile(bucketName: String, fileName: String, newFile: File) = {
   //create credential object to be used to access S3 buckets to retrieve names of
   //files in buckets
   val credentials = new BasicAWSCredentials(AWS_ACCESS_KEY, AWS_SECRET_KEY)
    //creates configuration settings to be passed to AmazonS3Client object that will read S3 contents
    val clientConfig = new ClientConfiguration()
    clientConfig.setProtocol(Protocol.HTTPS)
    val conn = new AmazonS3Client(credentials, clientConfig)

    //copy file from master node to S3
    conn.putObject(bucketName, fileName, newFile)
  }

//create array with names of files to be accessed from S3 buckets
def getFileNames(bucketName: String) : scala.collection.mutable.MutableList[String] = {
    //create credential object to be used to access S3 buckets to retrieve names of files in buckets
    val credentials = new BasicAWSCredentials(AWS_ACCESS_KEY, AWS_SECRET_KEY)
    //creates configuration settings to be passed to AmazonS3Client object that will read S3 contents
    val clientConfig = new ClientConfiguration()
    clientConfig.setProtocol(Protocol.HTTPS)
    //val bucketName = "vscanner-clean"

    val conn = new AmazonS3Client(credentials, clientConfig)
    val listObjectsRequest = new ListObjectsRequest().withBucketName(bucketName)
    //listObjectsRequest.setMaxKeys(2)
    var objectListing = conn.listObjects(listObjectsRequest)

    var fileNameArray = scala.collection.mutable.MutableList[String]()

    if(objectListing.isTruncated.equals(true)) {
      while (objectListing.isTruncated.equals(true)) {
        val listObjectsSize = objectListing.getObjectSummaries.size()
        var counter = 0
        while (counter < listObjectsSize) {
          val objectSum = objectListing.getObjectSummaries
          val fileName = objectSum.get(counter).getKey
          fileNameArray += fileName
          counter += 1
        }
        //val nextMarker = objectListing.getNextMarker
        objectListing = conn.listNextBatchOfObjects(objectListing)
      } //end of while loop
    }//end of if

    if(objectListing.isTruncated.equals(false)){
      val listObjectsSize = objectListing.getObjectSummaries.size()
      var counter = 0
      while (counter < listObjectsSize) {
        val objectSum = objectListing.getObjectSummaries
        val fileName = objectSum.get(counter).getKey
        fileNameArray += fileName
        counter += 1
      }
    }

    fileNameArray
  }

/***
creates unique features with 7 bytes long using sliding window method from 
each file type. Duplicate features are removed from each file
and dedupicated features are saved back to disk to be accessed later
FilePath is the folder for source file
temFilePath is the folder to store files for distinctive features
***/
def createByteFeatures(sc: SparkContext, FilePath: String) : Array[RDD[String]] =  {
    
    // if s3
    val fileNames = getFileNames(FilePath)
    // if local
    //val fileNames = new File(FilePath).listFiles

    //Number of files that will be processed.  This is returned to the main method.
    //val fileCount = fileNames.length.toDouble
    //System.out.println("Number of files to processed " + fileCount)

    var tempArray = new Array[RDD[String]](0)

    /***
      loop through files in source folder and 
      create distinctive feature sets for each file
      var i is the counter used to control when to exit the while loop.
    ***/

    // for (file <- fileNames) {
    for (file <- fileNames) {
    //create RDD from source file

    // if s3
    val fileName = "s3://" + FilePath + "/" + file.toString

    // if local
    //val fileName = file.toString

    System.out.println(fileName)

    val sourceRDD = sc.textFile(fileName)

    /***
    read in lines of data, remove new line character at the end of each line
    convert the contents to UpperCase to better match HEX
    ***/
    //val removeNewLineRDD = sourceRDD.map(_.mkString("").replaceAll("\n","").toUpperCase)

     /***
     break data into 7 byte chunks with a sliding window that 
     advances one character at a time. Appends a space between each feature
     ***/
     val slidingWindowRDD = sourceRDD.map(_.sliding(14).mkString("" + " ").toUpperCase)
     
     /***
     places each feature into a seperate element
     removes duplicate features
     ***/
     val distinctFeatureRDD = slidingWindowRDD.flatMap(_.split(" ")).distinct
    
     // add deduplicated data to the array to be returned
     tempArray = tempArray :+ distinctFeatureRDD
    }
    //Double with number of files processed
    tempArray
  }

/***
this function will calculate entropy to decide whether we use one feature to divide the tree. x is the number of the virus files for one feature, y is number of files (virus + clean files) y-x is the number of clean files 
***/
def entropy(x: Double, y: Double) : Double = {
  val solution = -(x / y) * ((Math.log(x) - Math.log(y)) / Math.log(2)) - ((y - x) / y) * ((Math.log(y - x) - Math.log(y)) / Math.log(2))
  solution
}

/***
compute inforGain function
tg: total number of time that one feature occurs in virus and clean file
pg:total number of time that one feature occurs in virus file
p: total number of virus files
t:total number of files (virus + clean files)
***/
def infoGain(p: Double, t: Double, tg: Double, pg: Double) : Double = {
  val infogainSol = entropy(p, t) - (tg / t) * entropy(pg, tg) - ((t - tg) / t) * entropy((p - pg), (t - tg))
  infogainSol
}

/*** 
this method will count how many times each unique feature occurs in virus files.
this method return RDD, key is the feature, value is how many times this feature
occurs in virus file.
***/
def countFeatures(sc: SparkContext, tempArray: Array[RDD[String]]) : RDD[(String, Int)] = {
   /***
   Combine RDD of ALL deduplicated feature files into a single RDD
   Create RDD with key-value pair

   create RDD with list of features that combines matching features from individual
   files and counts the number of times the features exists across all Virus files.
   ***/

   val combineFeatureRDD = sc.union(tempArray).map(x => (x,1)).reduceByKey(_ + _)
   combineFeatureRDD
}

//this function will replace NaN with 0.0
def replaceNaN(v: Double) : Double = {
  var convertedNaN = v
  if(v.isNaN()==true){
    convertedNaN = 0.0
  }
  convertedNaN
}

/***
v the number of virus files
t the number of virus + clean files
fNumber the number of selected features
This method will compute information gain for each feature. Sort the feature based on
information Gain and select the top feature
***/
def featureSelectionByInfoGain(sc: SparkContext, v: Double, t: Double, fNumber: Int, 
  virusRDD: RDD[(String, Int)], cleanRDD: RDD[(String, Int)]) : Array[(String, Double)] = {
    /***
    Join of virusRDD and cleanRDD where they have matching features.
    Join RDD of virus and clean features where features exists in both.
    Compute infoGain on each feature.
    scala> virusRDD.join(cleanRDD)
    res2: org.apache.spark.rdd.RDD[(String, (Int, Int))]
    */
    val joinRDD = virusRDD.join(cleanRDD)

    val infoGainRDD = joinRDD.map { case(feature, (timesFoundInVirusFiles, timesFoundInCleanFiles)) =>
      (feature, infoGain(v, t, (timesFoundInVirusFiles + timesFoundInCleanFiles).toDouble, 
        timesFoundInVirusFiles.toDouble)) }.mapValues(values => replaceNaN(values))

    /***
    sort features by infoGain value in descending order
    infoGainSorted: RDD[(String, Double)
    ***/
    
    //val infoGainSorted = infoGainRDD.map(item => item.swap).sortByKey(false).map(item => item.swap)
    //val selectedFeatures = infoGainSorted.take(fNumber)
    
    //Create Array[(String, Double)] of desired number of top features.
    //Array[(String, Double)
    val selectedFeatures = infoGainRDD.takeOrdered(fNumber)(Ordering[Double].reverse.on{ case(feature, value) => value })

    //return topFeatures Array to driver class
    selectedFeatures
  }

  //function to read in feature mapping files and insert them into the ListBuffer in LIBSVM format.
  def createLIBSVMFormat(sc: SparkContext, tempArray: Array[RDD[String]], fileType: String, features: RDD[String]) : Array[String] = {
    // holds the output to be written to a file in LIBSVM format
    var LIBSVMOuputArray : Array[String] = new Array[String](0)
    var LIBSVMOutputString = ""
    //Create Array of fileNames found in the S3 bucket
    //val fileNames = getFileNames(FilePath)

    //variable to store file type value as a number 1=virus, 0=clean
    var label = "0"

    if (fileType == "virus")
      label = "1"

      /***
      format data to the LIBSVM format
      ***/

      // track whether the label has been added to the string, if no label, no features matched
      var labelAdded = false

      // loop through features
      for (file <- tempArray)
      {
        LIBSVMOutputString = ""
        val testFileOutput = doesFeatureExist(file, features)

        if (testFileOutput.length > 0)
        for (num <- testFileOutput) {
          if (labelAdded == true)
          LIBSVMOutputString += " " + num + ":1"
          else {
            LIBSVMOutputString += label + " " + num + ":1"
            labelAdded = true
          }
        }

        if (labelAdded)
          LIBSVMOuputArray = LIBSVMOuputArray :+ LIBSVMOutputString

        labelAdded = false
      }

      LIBSVMOuputArray
    }

  /*
  lookup features from topFeatures file and save the results to file for to read in later.
  */
  def doesFeatureExist(tempRDD: RDD[String], featureRDD: RDD[String]) : Array[Long] = {

      // x = feature
      val tempDataRDD = tempRDD.map(x => (x,1))
      val tempFeatureRDD = featureRDD.map(x => (x,1))

      //Create RDD that lists if features from the featureRDD also exist in the tempDataRDD and removes duplicates.
      val joinedRDD = tempFeatureRDD.leftOuterJoin(tempDataRDD).distinct()

      //Create RDD with data from the joinedRDD that contains an index and then reverses the
      //(key,value) pair so that the index value becomes the key so the data can be order based on the index.
      val indexedRDD = joinedRDD.zipWithIndex().map{case (k,v) => (v,k)}

      /*Create RDD that contains only features that existed in both the tempData and featureRDD data sets.
       *  Returned RDD contains feature Index value and a 1.
       *  Example results - (33,1)
       *  .filter is based on the elements matching the "=> a == Some(1)" condition.  Only feature elements that were found to
       *  exist in both the tempDataRDD and the featureRDD will test true and be included in the filteredRDD.
       *  The matching elements are then converted to a format of (index + 1, 1)
       *  --index + 1 is required to convert the index to a 1's based index due to the fact that when the data is
       *  read back in by the MLUtils.loadLibSVMFile API in future steps the index automatically reduced by 1.
        */

      val filteredRDD = indexedRDD.filter { case (x: Long, (y: String, (z: Int, a:Option[Int]))) => a == Some(1)}.map { case (x: Long, (y: String, (z: Int, a:Option[Int]))) => (x + 1)}
      filteredRDD.collect()
  }
}