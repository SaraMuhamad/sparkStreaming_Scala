# sparkStreaming_Scala
Telecom Company X is aiming to increase the data usage of its customer base as a key objective for 2021. So X decided to target their data customer base with specialized campaigns to upsell their data packages. X also partnered up with various famous applications to give the customers extra promotions to increase the traffic on the applications. X is looking for aspiring data engineers to transform such strategic decision into a reality. Aspiring data engineers will work with streaming data capturing the data usage traffic for X customers. Since the customer base generates ridiculous amount of traffic, X is only interested in a subset of the base “Segment” to try the new initiative on. To insure a good customer experience, X wants to target the segment with only one campaign a day per application.

//to execute the jar 
Spark-Scala Streaming Project
Objective:
The main objective of the application is to produce the data each app is interested in for their campaign. To do so, we used spark-streaming framework to listen for incoming data, process it on the fly according to the desired set of rules.

How to use:
Using our application is fairly simple. All you need is the attached jar file.  You can run it using (java -jar) command as follows:

java -jar scala_project-assembly-0.1.jar <path#1> <path#2> <path#3>

Note that:

<path#1> is the path to the directory containing both SEGMENT.csv  and RULES.csv, both files must be placed in that directory and both files must be named as mentioned earlier.
<path#2> is the path to the directory of the input data stream
<path#3> is the path to the desired directory of the output data stream

All 3 input arguments are necessary for the application to run properly
All paths must not contain any spaces
The path for input stream directory must end with a slash [ \ ]
