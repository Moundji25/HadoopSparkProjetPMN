import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame

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

// function for reading csv files from local or HDFS
def readCSV(spark : SparkSession,filePath: String, schema: Option[StructType] = None): DataFrame = 
  {
    // LECTURE A PARTIR DE LOCAL  >>>>>>>>>>

    val df = schema match {
      case Some(s) => spark.read.schema(s).csv(filePath)
      case None => spark.read.option("inferSchema", "true").option("header","true").csv(filePath)
    }
    // fonction de verfi
    
    // LECTURE A PARTIR DE HDFS >>>>>>>>>>

    return df;
  }
}