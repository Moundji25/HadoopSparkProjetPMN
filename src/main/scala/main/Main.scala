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
  
    val df_requete_5 = DataFrameResult.Requete_5(output_full,goods_classes)
    df_requete_5.cache()

    println("REQUETE 05 ")
    df_requete_5.show()
    df_requete_5.printSchema()

   
  val tmp_map = DataFrameResult.Requete_6(output_full)
  val df_requete_6_goods = tmp_map("goods")
  val df_requete_6_services = tmp_map("services")

  df_requete_6_goods.cache()
  df_requete_6_services.cache()

  println("REQUETE 06 ")
  println(" les pays exporatateurs par goods ")
  df_requete_6_goods.show()
  df_requete_6_goods.printSchema()
  println(" les pays exporatateurs par services ")
  df_requete_6_services.show()
  df_requete_6_services.printSchema()

  val tmp_map = DataFrameResult.Requete_7(output_full)
  val df_requete_7_goods = tmp_map("goods")
  val df_requete_7_services = tmp_map("services")

  df_requete_7_goods.cache()
  df_requete_7_services.cache()

  println("REQUETE 07 ")
  println(" les pays imporatateurs par goods ")
  df_requete_7_goods.show()
  df_requete_7_goods.printSchema()
  println(" les pays imporatateurs par services ")
  df_requete_7_services.show()
  df_requete_7_services.printSchema()

val df_requete_8 = DataFrameResult.Requete_8(output_full)
    //df_requete_8.cache()

    println("REQUETE 08 ")
    df_requete_8.show()
    df_requete_8.printSchema()

val df_requete_9 = DataFrameResult.Requete_8(output_full)
    //df_requete_9.cache()

    println("REQUETE 09 ")
    df_requete_9.show()
    df_requete_9.printSchema()

val df_requete_10 = DataFrameResult.Requete_8(output_full)
    //df_requete_10.cache()

    println("REQUETE 10 ")
    df_requete_10.show()
    df_requete_10.printSchema()

val df_requete_11 = DataFrameResult.Requete_11(output_full)
    //df_requete_11.cache()

    println("REQUETE 11 ")
    df_requete_11.show()
    df_requete_11.printSchema()


val df_requete_12 = DataFrameResult.Requete_12(output_full,services_classes)
    df_requete_12.cache()

    println("REQUETE 12 ")
    df_requete_12.show()
    df_requete_12.printSchema()

val df_requete_13 = DataFrameResult.Requete_13(output_full,goods_classes)
    df_requete_13.cache()

    println("REQUETE 13 ")
    df_requete_13.show()
    df_requete_13.printSchema()

val df_requete_14 = DataFrameResult.Requete_14(output_full,country_classes)
    df_requete_14.cache()

    println("REQUETE 14 ")
    df_requete_14.show()
    //df_requete_14.printSchema()

val df_requete_15 = DataFrameResult.Requete_15(output_full,country_classes)
    //df_requete_15.cache()

    println("REQUETE 15 ")
    df_requete_15.show()
    df_requete_15.printSchema()

val df_requete_16 = DataFrameResult.Requete_16(output_full)
    //df_requete_16.cache()

    println("REQUETE 16 ")
    df_requete_16.show()
    df_requete_16.printSchema()

val df_requete_17 = DataFrameResult.Requete_17(output_full)
    //df_requete_17.cache()

    println("REQUETE 17 ")
    df_requete_17.show()
    df_requete_17.printSchema()
    
val tmp_map = DataFrameResult.Requete_19(output_full)
    //df_requete_19.cache()

val df_requete_19_imports_prctg_goods = tmp_map("imports_prctg_goods")
val df_requete_19_exports_prctg_goods = tmp_map("exports_prctg_goods")

  //df_requete_19_exports_prctg_goods.cache()
  //df_requete_19_imports_prctg_goods.cache()

  println("REQUETE 19 ")
  println(" le pourcentage de goods pour les imports par pays : ")
  df_requete_19_imports_prctg_goods.show()
  df_requete_19_imports_prctg_goods.printSchema()
  println(" le pourcentage de goods pour les exports par pays : ")
  df_requete_19_exports_prctg_goods.show()
  df_requete_19_exports_prctg_goods.printSchema()
*/
  val tmp_map = DataFrameResult.Requete_20(output_full)
    //df_requete_20.cache()

val df_requete_20_imports_prctg_services = tmp_map("imports_prctg_services")
val df_requete_20_exports_prctg_services = tmp_map("exports_prctg_services")

  //df_requete_20_exports_prctg_goods.cache()
  //df_requete_20_imports_prctg_goods.cache()

  println("REQUETE 20 ")
  println(" le pourcentage de goods pour les imports par pays : ")
  df_requete_20_imports_prctg_services.show()
  df_requete_20_imports_prctg_services.printSchema()
  println(" le pourcentage de goods pour les exports par pays : ")
  df_requete_20_exports_prctg_services.show()
  df_requete_20_exports_prctg_services.printSchema()



}
}
