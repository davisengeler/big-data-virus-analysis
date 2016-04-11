<?php

	// TODO: This should work seamlessly when changing the number of clusters. 
	//       This technique should be changed to work well with the others.
	$numberOfClusters = 10;

	$rawClusteringInfo = explode("\n", file_get_contents("output.txt"));
	print_r($rawClusteringInfo);

	// Set up the clusters
	$clusters = Array();
	for ($i = 0; $i < $numberOfClusters; $i++)
	{
		$clusters[$i]["name"] = "Cluster";
		$cleanAPIs = Array();
		$virusAPIs = Array();
		$cleanCluster = Array("name"=>"Clean", "children"=>$cleanAPIs);
		$virusCluster = Array("name"=>"Virus", "children"=>$virusAPIs);
		$clusters[$i]["children"] = Array($cleanCluster, $virusCluster);
	}

	// Load up the clusters
	foreach($rawClusteringInfo as $apiCall) 
	{
		if (strcmp($apiCall, "") != 0) {
			$apiInfo = explode(',', $apiCall);  // each one looks like "id,type"
			$clusterID = intval($apiInfo[0]);
			$apiType = intval($apiInfo[1]);

			$clusters[$clusterID]["children"][$apiType]["children"][] = Array("name"=>"API Call", "size"=>100); // TODO: We need to calculate the size somehow.
		}
	}

	// Load up the main container
	$mainContainer = Array("name"=>"Main Container", "children"=>$clusters);

	// Output to file
	$outputFile = fopen("www/html/data.json", "w");
	fwrite($outputFile, json_encode($mainContainer));
	
?>