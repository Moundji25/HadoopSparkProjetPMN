import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame

object Hello
{
  def main(args: Array[String]): Unit = 
  {
    println("HEY !!")
    // Create a util object 
    val utils = new Utils()

    // Create a SparkSession
    val spark :SparkSession = utils.createSparkSession()
    
    

    // Create a ApplicationProperties object and it's possible to specify wich parameter to change and leave the others values by default
    val applicationProperties = new ApplicationProperties()
    
    //Create a ApplicationProperties object 
    val constants = new Constants()
    
    // Create a util object 
    val DataFrameResult = new DataFrameResult(spark)
    
    // Read files
    val country_classes = utils.readCSV(spark,applicationProperties.local_path_file_1)
    val goods_classes = utils.readCSV(spark,applicationProperties.local_path_file_2)
    val output_full = utils.readCSV(spark,applicationProperties.local_path_file_3)
    val services_classes = utils.readCSV(spark,applicationProperties.local_path_file_4)

  /* 
    val df_requete_1 = DataFrameResult.Requete_1(output_full)
    df_requete_1.cache()
     
    println("REQUETE 01 ")
    //utils.afficher(df_requete_1)
    df_requete_1.show()
    df_requete_1.printSchema()
    
    val df_requete_2 = DataFrameResult.Requete_2(df_requete_1)
    df_requete_2.cache()

    println("REQUETE 02 ")
    df_requete_2.show()
    df_requete_2.printSchema()
  
    
    val df_requete_3 = DataFrameResult.Requete_3(output_full,country_classes)
    df_requete_3.cache()

    println("REQUETE 03 ")
    df_requete_3.show()    
    df_requete_3.printSchema()

    val df_requete_4 = DataFrameResult.Requete_4(output_full,services_classes)
    df_requete_4.cache()

    println("REQUETE 04 ")
    df_requete_4.show()
    df_requete_4.printSchema()
  */ 
    val df_requete_5 = DataFrameResult.Requete_5(output_full,goods_classes)
    df_requete_5.cache()

    println("REQUETE 05 ")
    //df_requete_5.show()
    //df_requete_5.printSchema()

  }

}
