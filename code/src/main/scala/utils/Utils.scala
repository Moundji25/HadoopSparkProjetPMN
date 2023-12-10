//import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode}

case class Utils() 
{
def createSparkSession() : SparkSession = {
    
    /*val config = ConfigFactory.load("src/scala/application.properties")
    val spark_app_name = config.getString("application.spark_app_name")
    val spark_master = config.getString("application.spark_master")
    */
    SparkSession.builder
      .appName("HadoopSparkPmnProject")
      .master("local[*]")
      .getOrCreate()
}

// function for reading csv files from local 
def readCSVLocal(spark : SparkSession,filePath: String, schema: Option[StructType] = None): DataFrame = 
  {
    // LECTURE A PARTIR DE LOCAL  >>>>>>>>>>

    val df = schema match {
      case Some(s) => spark.read.schema(s).csv(filePath)
      case None => spark.read.option("inferSchema", "true").option("header","true").csv(filePath)
    }

    return df;
  }

  // function for reading csv files from local 
def readCSVinHDFS(spark : SparkSession,filePath: String, hadoop_user:String,schema: Option[StructType] = None): DataFrame = 
  {
      val df = schema match {
      case Some(s) => spark.read.schema(s).csv(filePath)
      case None => spark.read.option("inferSchema", "true").option("header","true").csv("hdfs://localhost:9000/user/"+hadoop_user+"/" + filePath)
    }

    return df;
  }
  // function for wirting csv files into local 
  def writeToCSV(df: DataFrame, outputPath: String): Unit = {
    df.write
      .mode(SaveMode.Overwrite) // Mode d'écriture : écraser si le fichier existe déjà
      .option("header", "true") 
      .csv(outputPath)
  }

// function for wirting parquet files into local 
  def writeToParquet(df: DataFrame, outputPath: String): Unit = {
    df.write
      .mode(SaveMode.Overwrite) // Mode d'écriture : écraser si le fichier existe déjà
      .parquet(outputPath) 
  }

 /* // Fonction pour écrire un fichier CSV dans HDFS
  def writeToCSVInHDFS(df: DataFrame, outputPath: String): Unit = {
    df.write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv("hdfs://localhost:9000/user/moundji/" + outputPath)
  }

  // Fonction pour écrire un fichier Parquet dans HDFS
  def writeToParquetInHDFS(df: DataFrame, outputPath: String): Unit = {
    df.write
      .mode(SaveMode.Overwrite)
      .parquet("hdfs://localhost:9000/user/moundji/" + outputPath)
  }*/

}