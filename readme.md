#### Config File

A config.scala file should be populated with your AWS information and saved to `/src/main/scala/config.scala`. You may also rename `/src/main/scala/config-sample.scala` and update the information.

```
package org.virus

object AWSKeyInformation {
	val AWS_ACCESS_KEY = "EXAMPLEKEY"
	val AWS_SECRET_KEY = "ExaMPleSEcretKEY"
}

object AWSBucketInformation {
	val AWS_CLEAN_BUCKET = "clean-bucket-name"
	val AWS_VIRUS_BUCKET = "virus-bucket-name"
	val AWS_RESULTS_BUCKET = "results-bucket-name"
}

object ProjectConfig {
	val RUN_LOCALLY = false;
}
```

==============


#### Feature Selection

The API files are loaded and converted into the *LIBSVM* format and saved to `/LIBSMVOutput.txt`. It identifies the top features using the `informationGain` function and outputs to `/topFeatures.txt`.

==============


#### K-Means Clustering

Determines which cluster to put each sample using a k-means clustering algorithm. The results are saved in `/output.txt`

###### Format of the resulting file:

*Each new line contains information about the samples.*

```
clusterNumber;labelType;{["apiName":size,"apiName":size,...]}
...
```

Where each component is defined by:

- `clusterNumber`: The cluster index determined by k-means `predict` function.

- `labelType`: `1.0` for viruses; `0.0` for clean.

- `{["apiName":size,"apiName":size,...]}`: JSON-ecoded array of API call titles; `size` is the API's ranking in the top features (in descending order).

###### Example output:

```
4;1.0;[{"name": "SuspendThread", "size": 59},{"name": "CreatePipe", "size": 36},{"name": "DeleteService", "size": 28},{"name": "ResumeThread", "size": 21},{"name": "SetWindowPos", "size": 17}]
9;1.0;[{"name": "CreateToolhelp32Snapshot", "size": 83},{"name": "SetProcessDEPPolicy", "size": 82},{"name": "SystemParametersInfo", "size": 77},{"name": "GetUserName", "size": 75},{"name": "FindWindow", "size": 74},{"name": "DeleteFile", "size": 73},{"name": "Sleep", "size": 72},{"name": "GetVolumeInformation", "size": 71},{"name": "GetWindowTextLength", "size": 70},{"name": "LdrFindEntryForAddress", "size": 66},{"name": "OpenProcessToken", "size": 64},{"name": "OpenSCManager", "size": 62},{"name": "SuspendThread", "size": 59},{"name": "LoadLibrary", "size": 58},{"name": "OpenProcess", "size": 56},{"name": "ControlService", "size": 55},{"name": "ConnectNamedPipe", "size": 54},{"name": "CreateMutex", "size": 52},{"name": "connect", "size": 50},{"name": "CreateThread", "size": 48},{"name": "DeviceIoControl", "size": 44},{"name": "CryptHashData", "size": 43},{"name": "GetKeyState", "size": 41},{"name": "OutputDebugString", "size": 37},{"name": "EnumProcesses", "size": 35},{"name": "FreeLibrary", "size": 34},{"name": "SetNamedSecurityInfo", "size": 32},{"name": "StartService", "size": 31},{"name": "OpenMutex", "size": 30},{"name": "InternetSetOption", "size": 29},{"name": "DeleteService", "size": 28},{"name": "ResumeThread", "size": 21},{"name": "GetComputerName", "size": 18},{"name": "ConnectServerWMI", "size": 16},{"name": "IsDebuggerPresent", "size": 15},{"name": "ChangeServiceConfig", "size": 14},{"name": "HttpSendRequest", "size": 12},{"name": "NtSetInformationThread", "size": 10},{"name": "SetClipboardViewer", "size": 4},{"name": "QuerySystemInformation", "size": 1}]
....
```

=================

#### Converting output.txt to JSON for D3 visualization

Use `/exportJSON.php` to prepare the content of `/output.txt` to be visualized via D3. The script is set up to organize to satisfy the structure for the open source [Zoomable Circle Packing](https://bl.ocks.org/mbostock/7607535) cluster visualization.

<<<<<<< HEAD
###### Example usage:

```
> php exportJSON.php
=======
================

#### Classification

Determines the ROC curve and area under ROC curve (AUC) to predict the accuracy of the cluster classifcation. Sample output:

```
0.001 L2 regularization parameter, AUC = 74.761537%
0.01 L2 regularization parameter, AUC = 72.390801%
0.1 L2 regularization parameter, AUC = 69.726810%
1.0 L2 regularization parameter, AUC = 64.863405%
10.0 L2 regularization parameter, AUC = 50.239234%
>>>>>>> origin/master
```