# dReasoning
The test code used in Rethinking Defeasible Reasoning: A Scalable Approach

0, The operating system is Linux kernel version 3.10.0-693 and the software stack consists of Spark version 2.3.1, Hadoop version 2.7.3, Scala version 2.11.8 and Java version 1.8.0_191.

1, The exported jar is available for testing and can be also exported by the source codes.

2, The 1.sh script can be used for batch tests after changing the systems configurations.

3, A sample of the job submission commands as described in 1.sh is shown as below. Namely, besides the first four system parameters, there are 3 other parameters in our codes. There, "spark://n53:7077" is the sparkcontext, "hdfs://n53:9000/" is the file input path over HDFS, and "$i" is the number of executor cores.

> spark-submit \ <br/>
  --class demoTest \ <br/>
  --master spark://n53:7077 \ <br/>
   --executor-memory 160GB \ <br/>
  DefLogic.jar  \ <br/>
  spark://n53:7077 \ <br/>
  hdfs://n53:9000/ \ <br/>
  $i  <br/>
  
3, How to set Spark, please refer to http://spark.apache.org/.

4, If any questions, please email to long.cheng(AT)ucd.ie
