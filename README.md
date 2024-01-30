# HadoopSparkProjetPMN
## This repository contains :

- 'Code' folder: that contains a sbt structure of a scala spark project
- 'HDFS_scripts': that contains some scripts that will prepare for input data and folders in Hadoop environment and will also run the scala and spark code, Note that these must be executed with Spark, Hadoop and Java installed 
- 'ressources' folder: that contains the input files
- tp_scala_2.12-1.0.jar: the executable version of the project

## Execution :
- to execute you will need to execute this file main_script.sh with 3 parameters (user in the hadoop clusters , local_directory_path, the jar file)
    exemple :  ./main_script.sh moundji ressources/ tp_scala_2.12-1.0.jar
- if there's a problem in execution it's possible to directly execute the 'spark_run.sh' file with 2 parameters (user in the hadoop clusters, the jar file)
  
## Dependencies 
  - scala = "2.12.13"
  - spark-core = "3.2.0"
  - spark-sql = "3.2.0"
  - sbt 
  - hadoop
