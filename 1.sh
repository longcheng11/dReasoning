#!/bin/bash
# i is the number of copies

#upload files to hadoop
hdfs dfs -put /ichec/home/users/lcheng/ndcom011c/*.csv /

sleep 30

for i in 1 3 6 12
do
       # data generation
        spark-submit	--class DataGen	--master spark://n53:7077 --executor-memory 160GB DefLogic.jar spark://n53:7077 hdfs://n53:9000/ $i 320
        sleep 30

       # reasoning
        spark-submit    --class demoTest  --master spark://n53:7077  --executor-memory 160GB DefLogic.jar  spark://n53:7077 hdfs://n53:9000/  $i
        echo "job 1 is done"
        sleep 10
        
        spark-submit    --class drugTest   --master spark://n53:7077  --executor-memory 160GB DefLogic.jar  spark://n53:7077 hdfs://n53:9000/  $i
        echo "job 2 is done"
        sleep 10

        spark-submit    --class outcTest  --master spark://n53:7077  --executor-memory 160GB DefLogic.jar  spark://n53:7077 hdfs://n53:9000/  $i
        echo "job 3 is done"
        sleep 10
  
        spark-submit    --class reacTest --master spark://n53:7077  --executor-memory 160GB DefLogic.jar  spark://n53:7077 hdfs://n53:9000/  $i
        echo "job 4 is done"
        sleep 10
        
        spark-submit    --class rpsrTest  --master spark://n53:7077  --executor-memory 160GB DefLogic.jar  spark://n53:7077 hdfs://n53:9000/  $i
        echo "job 5 is done"
        sleep 10        
 
        spark-submit    --class wholeTest --master spark://n53:7077  --executor-memory 160GB DefLogic.jar  spark://n53:7077 hdfs://n53:9000/  $i
        echo "job 6 is done"
        sleep 10    
         
done

hdfs dfs -rm -r /*

