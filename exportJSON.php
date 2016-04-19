<?php

	// TODO: This should work seamlessly when changing the number of clusters. 
	//       This technique should be changed to work well with the others.
	$numberOfClusters = 10;

	$rawClusteringInfo = explode("\n", file_get_contents("output.txt"));

	// Set up the clusters
	$clusters = Array();
	for ($i = 0; $i < $numberOfClusters; $i++)
	{
		$apiCallsClean = Array();
		$apiCallsVirus = Array();
		$clusters[$i]["name"] = "Cluster #" . ($i+1);
			$cleanSamples = Array("name"=>"CleanSample", "children"=>$apiCallsClean);
			$virusSamples = Array("name"=>"VirusSample", "children"=>$apiCallsVirus);
		$cleanCluster = Array("name"=>"Clean");
		$virusCluster = Array("name"=>"Virus");
		$clusters[$i]["children"] = Array($cleanCluster, $virusCluster);
	}

	// Load up the clusters
	foreach($rawClusteringInfo as $currentSample) 
	{
		if (strcmp($currentSample, "") != 0) {
			$sampleInfo = explode(';', $currentSample);  // each one looks like "id,type"
			$clusterID = intval($sampleInfo[0]);
			$sampleType = intval($sampleInfo[1]);
			$apiCalls = json_decode($sampleInfo[2]);

			echo "\n\n\n";

			print_r($apiCalls);

			$clusters[$clusterID]["children"][$sampleType]["children"][] = Array("name"=>"Sample", "children"=>$apiCalls); // TODO: We need to calculate the size somehow.
		}
	}

	// Load up the main container
	$mainContainer = Array("name"=>"Main Container", "children"=>array_values($clusters));

	// Output to file
	$outputFile = fopen("www/html/data.json", "w");


	


	fwrite($outputFile, json_encode($mainContainer));

	echo json_encode($mainContainer);


	
?>