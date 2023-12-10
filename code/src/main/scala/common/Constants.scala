case class Constants(
    val hadoop_user:String
)
{
 //getter for hadoop_user 
 def getHadoopUser:String = {
    return hadoop_user
 }

 def getUrlHdfsCSV(number:String):String={
    "hdfs://localhost:9000/user/"+this.hadoop_user+"/requete_"+number+"/csv"
 }

 def getUrlHdfsParquet(number:String):String={
    "hdfs://localhost:9000/user/"+this.hadoop_user+"/requete_"+number+"/parquet"
 }
}