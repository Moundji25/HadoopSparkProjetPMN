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
    val data_frame_result = new DataFrameResult(spark)
    
    // Read files
    val country_classes = utils.readCSVLocal(spark,applicationProperties.local_path_file_1)
    val goods_classes = utils.readCSVLocal(spark,applicationProperties.local_path_file_2)
    val output_full = utils.readCSVLocal(spark,applicationProperties.local_path_file_3)
    val services_classes = utils.readCSVLocal(spark,applicationProperties.local_path_file_4)

    // Afficher les options à l'utilisateur
    println(">>>>>>>>>>>>> Choisissez une requete en tappant son numero :")
    for (i <- 1 to 23) {
        println(i+". Requete "+i)
    }
    println("24_bonus. Requete 24")
    println("<<<<<<<<<<<<<")

    // Lire l'entrée de l'utilisateur
    val input = scala.io.StdIn.readLine()
    
    val selectedQuery: Int = input.toInt
    selectedQuery match {
        case 1 => executeQuery1(spark,utils,data_frame_result,output_full)
        case 2 => executeQuery2(spark,data_frame_result,output_full)
        case 3 => executeQuery3(spark,data_frame_result,output_full,country_classes)
        case 4 => executeQuery4(spark,data_frame_result,output_full,services_classes)
        case 5 => executeQuery5(spark,data_frame_result,output_full,goods_classes)
        case 6 => executeQuery6(spark,data_frame_result,output_full)
        case 7 => executeQuery7(spark,data_frame_result,output_full)
        case 8 => executeQuery8(spark,data_frame_result,output_full)
        case 9 => executeQuery9(spark,data_frame_result,output_full)
        case 10 => executeQuery10(spark,data_frame_result,output_full)
        case 11 => executeQuery11(spark,data_frame_result,output_full)
        case 12 => executeQuery12(spark,data_frame_result,output_full,services_classes)
        case 13 => executeQuery13(spark,data_frame_result,output_full,goods_classes)
        case 14 => executeQuery14(spark,data_frame_result,output_full,country_classes)
        case 15 => executeQuery15(spark,data_frame_result,output_full,country_classes)
        case 16 => executeQuery16(spark,data_frame_result,output_full)
        case 17 => executeQuery17(spark,data_frame_result,output_full)
        case 18 => executeQuery18(spark,data_frame_result,output_full)
        case 19 => executeQuery19(spark,data_frame_result,output_full)
        case 20 => executeQuery20(spark,data_frame_result,output_full,goods_classes)
        case 21 => executeQuery21(spark,data_frame_result,output_full)
        case 22 => executeQuery22(spark,data_frame_result,output_full)
        case 23 => executeQuery23(spark,data_frame_result,output_full)
        case 24 => executeQuery24(spark,data_frame_result,output_full,country_classes)
        case _ => println("Choix non valide")
    }
    }
  def executeQuery1(spark: SparkSession,utils:Utils,data_frame_result:DataFrameResult,output_full:DataFrame): Unit = {
        val df_requete_1 = data_frame_result.Requete_1(output_full)
        df_requete_1.cache()
         
        println("REQUETE 01 ")
        df_requete_1.show()
        df_requete_1.printSchema()
        utils.writeToCSVInHDFS(df_requete_1,"localhost:9000/user/moundji/requete_1")
  } 
  def executeQuery2(spark: SparkSession,data_frame_result:DataFrameResult,output_full:DataFrame): Unit = {
    val df_requete_2 = data_frame_result.Requete_2(output_full)
    df_requete_2.cache()

    println("REQUETE 02 ")
    df_requete_2.show()
    df_requete_2.printSchema()
  }
  def executeQuery3(spark: SparkSession,data_frame_result:DataFrameResult,output_full:DataFrame, country_classes:DataFrame): Unit = {
    val df_requete_3 = data_frame_result.Requete_3(output_full,country_classes)
    df_requete_3.cache()

    println("REQUETE 03 ")
    df_requete_3.show()    
    df_requete_3.printSchema()
  }
  def executeQuery4(spark: SparkSession,data_frame_result:DataFrameResult,output_full:DataFrame,services_classes:DataFrame): Unit = {
        val df_requete_4 = data_frame_result.Requete_4(output_full,services_classes)
    df_requete_4.cache()

    println("REQUETE 04 ")
    df_requete_4.show()
    df_requete_4.printSchema()
  }
  def executeQuery5(spark: SparkSession,data_frame_result:DataFrameResult,output_full:DataFrame,goods_classes:DataFrame): Unit = {
    val df_requete_5 = data_frame_result.Requete_5(output_full,goods_classes)
    df_requete_5.cache()

    println("REQUETE 05 ")
    df_requete_5.show()
    df_requete_5.printSchema()
  }
  def executeQuery6(spark: SparkSession,data_frame_result:DataFrameResult,output_full:DataFrame): Unit = {
    val tmp_map = data_frame_result.Requete_6(output_full)
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
  }
  def executeQuery7(spark: SparkSession,data_frame_result:DataFrameResult,output_full:DataFrame): Unit = {
        val tmp_map = data_frame_result.Requete_7(output_full)
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
  }
  def executeQuery8(spark: SparkSession,data_frame_result:DataFrameResult,output_full:DataFrame): Unit = {
    val df_requete_8 = data_frame_result.Requete_8(output_full)
    df_requete_8.cache()

    println("REQUETE 08 ")
    df_requete_8.show()
    df_requete_8.printSchema()
  }
  def executeQuery9(spark: SparkSession,data_frame_result:DataFrameResult,output_full:DataFrame): Unit = {
    val df_requete_9 = data_frame_result.Requete_8(output_full)
    df_requete_9.cache()

    println("REQUETE 09 ")
    df_requete_9.show()
    df_requete_9.printSchema()
  }
  def executeQuery10(spark: SparkSession,data_frame_result:DataFrameResult,output_full:DataFrame): Unit = {
    val df_requete_10 = data_frame_result.Requete_8(output_full)
    df_requete_10.cache()

    println("REQUETE 10 ")
    df_requete_10.show()
    df_requete_10.printSchema()
  }
  def executeQuery11(spark: SparkSession,data_frame_result:DataFrameResult,output_full:DataFrame): Unit = {
    val df_requete_11 = data_frame_result.Requete_11(output_full)
    df_requete_11.cache()

    println("REQUETE 11 ")
    df_requete_11.show()
    df_requete_11.printSchema()
  }
  def executeQuery12(spark: SparkSession,data_frame_result:DataFrameResult,output_full:DataFrame,services_classes:DataFrame): Unit = {
    val df_requete_12 = data_frame_result.Requete_12(output_full,services_classes)
    df_requete_12.cache()

    println("REQUETE 12 ")
    df_requete_12.show()
    df_requete_12.printSchema()
  }
  def executeQuery13(spark: SparkSession,data_frame_result:DataFrameResult,output_full:DataFrame,goods_classes:DataFrame): Unit = {
    val df_requete_13 = data_frame_result.Requete_13(output_full,goods_classes)
    df_requete_13.cache()

    println("REQUETE 13 ")
    df_requete_13.show()
    df_requete_13.printSchema()
  }
  def executeQuery14(spark: SparkSession,data_frame_result:DataFrameResult,output_full:DataFrame,country_classes:DataFrame): Unit = {
    val df_requete_14 = data_frame_result.Requete_14(output_full,country_classes)
    df_requete_14.cache()

    println("REQUETE 14 ")
    df_requete_14.show()
    df_requete_14.printSchema()
  }
  def executeQuery15(spark: SparkSession,data_frame_result:DataFrameResult,output_full:DataFrame,country_classes:DataFrame): Unit = {
    val df_requete_15 = data_frame_result.Requete_15(output_full,country_classes)
    df_requete_15.cache()

    println("REQUETE 15 ")
    df_requete_15.show()
    df_requete_15.printSchema()
  }
  def executeQuery16(spark: SparkSession,data_frame_result:DataFrameResult,output_full:DataFrame): Unit = {
    val df_requete_16 = data_frame_result.Requete_16(output_full)
    df_requete_16.cache()

    println("REQUETE 16 ")
    df_requete_16.show()
    df_requete_16.printSchema()
  }
  def executeQuery17(spark: SparkSession,data_frame_result:DataFrameResult,output_full:DataFrame): Unit = {
    val df_requete_17 = data_frame_result.Requete_17(output_full)
    df_requete_17.cache()

    println("REQUETE 17 ")
    df_requete_17.show()
    df_requete_17.printSchema()
  }
  def executeQuery18(spark: SparkSession,data_frame_result:DataFrameResult,output_full:DataFrame): Unit = {
    val tmp_map = data_frame_result.Requete_18(output_full)

    val df_requete_18_imports_prctg_goods = tmp_map("imports_prctg_goods")
    val df_requete_18_exports_prctg_goods = tmp_map("exports_prctg_goods")

    df_requete_18_exports_prctg_goods.cache()
    df_requete_18_imports_prctg_goods.cache()

        println("REQUETE 18 ")
        println(" le pourcentage de goods pour les imports par pays : ")
        df_requete_18_imports_prctg_goods.show()
        df_requete_18_imports_prctg_goods.printSchema()
        println(" le pourcentage de goods pour les exports par pays : ")
        df_requete_18_exports_prctg_goods.show()
        df_requete_18_exports_prctg_goods.printSchema()
  }
  def executeQuery19(spark: SparkSession,data_frame_result:DataFrameResult,output_full:DataFrame): Unit = {
    val tmp_map = data_frame_result.Requete_19(output_full)

    val df_requete_19_imports_prctg_services = tmp_map("imports_prctg_services")
    val df_requete_19_exports_prctg_services = tmp_map("exports_prctg_services")

    df_requete_19_exports_prctg_services.cache()
    df_requete_19_imports_prctg_services.cache()

    println("REQUETE 19 ")
    println(" le pourcentage de goods pour les imports par pays : ")
    df_requete_19_imports_prctg_services.show()
    df_requete_19_imports_prctg_services.printSchema()
    println(" le pourcentage de goods pour les exports par pays : ")
    df_requete_19_exports_prctg_services.show()
    df_requete_19_exports_prctg_services.printSchema()
  }
  def executeQuery20(spark: SparkSession,data_frame_result:DataFrameResult,output_full:DataFrame,goods_classes:DataFrame): Unit = {
    val df_requete_20 = data_frame_result.Requete_20(output_full,goods_classes)
    df_requete_20.cache()

    println("REQUETE 20 ")
    df_requete_20.show()
    df_requete_20.printSchema()
  }
  def executeQuery21(spark: SparkSession,data_frame_result:DataFrameResult,output_full:DataFrame): Unit = {
    val df_requete_21 = data_frame_result.Requete_21(output_full)
    df_requete_21.cache()

    println("REQUETE 21 ")
    df_requete_21.show()
    df_requete_21.printSchema()
  }
  def executeQuery22(spark: SparkSession,data_frame_result:DataFrameResult,output_full:DataFrame): Unit = {
    val df_requete_22 = data_frame_result.Requete_22(output_full)
    df_requete_22.cache()

    println("REQUETE 22 ")
    df_requete_22.show()
    df_requete_22.printSchema()
  }
  def executeQuery23(spark: SparkSession,data_frame_result:DataFrameResult,output_full:DataFrame): Unit = {
    val df_requete_23 = data_frame_result.Requete_23(output_full)
    df_requete_23.cache()

    println("REQUETE 23 ")
    df_requete_23.show()
    df_requete_23.printSchema()
  }
  def executeQuery24(spark: SparkSession,data_frame_result:DataFrameResult,output_full:DataFrame,country_classes:DataFrame): Unit = {
    val df_requete_24_bonus = data_frame_result.Requete_24_bonus(output_full,country_classes)
    df_requete_24_bonus.cache()

    println("REQUETE 24_bonus ")
    print(df_requete_24_bonus.select("description").head().get(0))
    df_requete_24_bonus.printSchema()
  }
}
