import org.apache.spark.sql.SparkSession

object Hello
{
  def main(args: Array[String]): Unit = 
  {
    println("HEY !!")

    // Create a SparkSession
    val spark = SparkSession.builder
      .appName("SparkScalaExample")
      .master("local[*]")
      .getOrCreate()
  }
}
