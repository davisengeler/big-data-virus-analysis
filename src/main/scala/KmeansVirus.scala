/**
 * Created by jsmith on 10/14/15.
 * Modified for API Calls by Alexander Sniffin and Davis Engeler on 4/16/16
 */

package org.virus

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model._
import com.amazonaws.{ ClientConfiguration, Protocol }

import java.io._
import scala.io.Source

import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd._
import org.apache.spark.{ SparkConf, SparkContext }

import org.apache.log4j.{ Level, Logger }


/**
 * K-mean clustering program for CSCI 599 Virus Detection Project
 * Formats the results from the K-mean clustering algorithm into /output.txt and
 * S3://resultBucketName/output.txt
 * Layers are then formated into JSON from the /exportJSON.php file
 * 
 * Different layers of the formated results, from smallest to biggest that are displayed in D3:
 * 
 * 4 - API Layer :              Show each API call for the index and it's size is based on it's rank in topFeatures
 * 3 - Indices Layer :          This layer shows each individual index from the LIBSVMOutput.txt
 * 2 - Clean and Virus Layer :  Label layer inside clusters, each cluster must have a clean and virus
 * 1 - Clusters Layer :         Cluster layer inside of the main layer (K value is manually set to 10)
 * 0 - Main Layer :             Outer most layer that holds all everything
 * 
 * (Note that the indices are the collection of each API_LOG file)
 * 
 * @date 4/16/16
 * @author jsmith, Alexander Sniffin, David Engeler
 */
object Clustering {

	// Load configuration
	val AWS_ACCESS_KEY = AWSKeyInformation.AWS_ACCESS_KEY
	val AWS_SECRET_KEY = AWSKeyInformation.AWS_SECRET_KEY

	// Log levels
	Logger.getLogger("org").setLevel(Level.OFF)
	Logger.getLogger("akka").setLevel(Level.OFF)

	def main(args : Array[String]) : Unit = {
		val conf = new SparkConf().setAppName("selectedintrusion").setMaster("local")
		val sc = new SparkContext(conf)
		val hadoopConf = sc.hadoopConfiguration
		// set SC configuration to allow recursive reads of directories for files
		hadoopConf.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")
		// set SC configuration to use NativeS3FileSystem
		hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
		// set SC configuration with S3 credentials
		hadoopConf.set("fs.s3.awsAccessKeyId", AWS_ACCESS_KEY)
		hadoopConf.set("fs.s3.awsSecretAccessKey", AWS_SECRET_KEY)

		val myAWSCredentials = new BasicAWSCredentials(AWS_ACCESS_KEY, AWS_SECRET_KEY)
		val amazonS3Client = new AmazonS3Client(myAWSCredentials)

		// Bucket name for results bucket, where LIBSVM, topFeatures, and output should be and update to
		val resultsBucketName = AWSBucketInformation.AWS_RESULTS_BUCKET

		println("Loading in LIBSVM.txt from S3...")
		// Create LabeledPoint RDD from LIBSVM formatted file stored on S3
		val rawData: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, "s3://" + resultsBucketName + "/LIBSVM.txt")
		rawData.cache
		
		println("Parsing indices to array...")
		// Accessing rawData.features(1) will not return the indices for each sparse vector(not sure why).
		// This is a somewhat costly parsing hack to get an RDD[Array[Int]] of all of the indices.
		// The array will be in order from the LIBSVMoutput.txt file and can be used
		// to match each API call with it's correctly corresponding feature name, we need
		// to do this so that when we display the clusters in D3 so that we can see each individual API call. 
		val pat = "\\[(.*?)\\]".r //Regular expression for everything inside [ ... ]
		val featureIndicesNumbers: RDD[Array[Int]] = rawData.map(labeledPoint => labeledPoint.toString()).map { str => 
			pat.findFirstIn(str).get match {
				case pat(s) => s.split("\\],\\[")(0).split(",").map(strNum => strNum.toInt) // Parsing
			}
		}
		//featureIndicesNumbers.foreach(x => println(x.mkString(",")))
		
		println("Loading topFeatures.txt from S3...")
		// Load in the topFeatures file
		val topFeatures: RDD[String] = sc.textFile("s3://" + resultsBucketName + "/topFeatures.txt")
		
		println("Formatting featureNames...");
		// This gets the feature names in order from 0 to n, replaces the bad characters out (could be improved with regex)
		// and gets rid of the value of the feature. It converts from an RDD[Array[String]] to an Array[String] at the end.
		val featureNames: Array[String] = topFeatures.map(x => x.replace("(", "").replace(")", "").split(",").map(_.trim).head).toArray()
		
		// Total amount of features
		val totalFeatures: Int = featureNames.size
		
		println("Mapping indices to feature names...")
		// Maps the indices to the corresponding name
		val featureIndicesNames: RDD[Array[String]] = featureIndicesNumbers.map(indices => 
			indices.map(index => "\"" + featureNames(index) + "\": " 
					// Get the features occurrence and display it in descending order (n to 1)
					// This way we can use this as a "rank" to display how big the size of the API cluster
					// inside of D3 should be, this will show how common it is compared to all the other API's
					+ (totalFeatures - index))) 
		//featureIndicesNames.foreach { x => println(x.mkString(",")) }
		
		// Convert LabeledPoint RDD to RDD of format (feature Label, Vector) to be used for computing best K value
		val rawDataFeatures: RDD[(Double, Vector)] = rawData.map(x => (x.label, x.features))
		
		println("Building K-Mean model...")
		// Get the clustering structure
		val rawClusterInfo: RDD[(Int, Double)] = buildClustering(rawDataFeatures, 10)
		
		println("Formatting predictions, labels, and features...")
		// Format the data into an RDD tuple3 of (ClusterNumber: Int, Label: Double, APIJsonData: String)
		val formatedClusterInfo: RDD[(Int, Double, String)] = (rawClusterInfo zip featureIndicesNames).map { 
			case (a, b) => (a._1, a._2, "{[" + b.mkString(",") + "]}") 
		}
		//formatedClusterInfo.foreach(x => println(x._1 + "," + x._2 + "," + x._3))
		
		println("Writing formatted results to output.txt...")
		// Create the File and BufferedWriter to output the data to
		val file = new File("output.txt")
		val bw = new BufferedWriter(new FileWriter(file))
		
		// We must change formatedClusterInfo to an array from an RDD because the BufferedWriter is not serializable,
		// therefore we must iterate through the array on the master node to print out the results
		formatedClusterInfo.toArray.foreach(x => { bw.write(x._1 + "," + x._2 + "," + x._3 + "\n") })
		bw.close()
		
		println("Uploading output.txt to S3...")
		//Upload the output data to S3
		uploadFile("output.txt", file, amazonS3Client)
		
		println("Closing session and exiting...")
		sc.stop()
	}
	
	/**
	 * Uploads a File to S3
	 * 
	 * @param name Name of the file
	 * @param amazonS3Client Client for S3
	 */
	def uploadFile(name: String, fileToUpload: File, amazonS3Client: AmazonS3Client) : Unit = {
		val putObjectRequest = new PutObjectRequest(AWSBucketInformation.AWS_RESULTS_BUCKET, name, fileToUpload)
		val acl = CannedAccessControlList.PublicRead
		putObjectRequest.setCannedAcl(acl)
		amazonS3Client.putObject(putObjectRequest)
	}

	/**
	 * This doesn't return the best K, but instead only prints the results by 5
	 * Requires further research https://en.wikipedia.org/wiki/Determining_the_number_of_clusters_in_a_data_set
	 * 
	 * @param rawData RDD tuple of label and vector of indices and values
	 */
	def searchBestKWithUsingEntropy(rawData : RDD[(Double, Vector)]) : Unit = {

		(5 to 100 by 5).map { k =>
			System.out.println("K is " + k)
			(k, buildClustering(rawData, k))
		}.toList.
			foreach(println)

	}

	/**
	 * Builds a collection of clusters based on the KMeans predictions
	 * 
	 * @param normalizedLabelsAndData RDD tuple of label and vector of indices and values
	 * @param k K value determined for setting the total clusters to create
	 */
	def buildClustering(normalizedLabelsAndData : RDD[(Double, Vector)], k : Int) : RDD[(Int, Double)] = {
		// Set up initial KMeans stuff
		val kmeans = new KMeans()
		kmeans.setK(k)
		kmeans.setRuns(10)
		kmeans.setEpsilon(1.0e-6)

		// Train a K-means model on the given set of points; data should be cached for high performance, because this is an iterative algorithm.
		val model = kmeans.run(normalizedLabelsAndData.values)
		
		// Returns an RDD of tuples of the predicted cluster number (Int), the label number (Double)
		// RDD[Tuple3(Int, Double)]
		normalizedLabelsAndData.map { case (x:Double, y:Vector) => (model.predict(y), x) }
	}

	/**
	 * Using Labels with Entropy
	 */
	def entropy(counts : Iterable[Int]) = {
		val values = counts.filter(_ > 0)
		val n : Double = values.sum
		values.map { v =>
			val p = v / n
			-p * math.log(p)
		}.sum
	}
}
