import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame

object Main
{
  def main(args: Array[String]): Unit = 
  {
     if (args.length != 1) {
      println("Usage: VotreClasseMain <hadoopUser>")
      sys.exit(1)
    }

    val hadoopUser = args(0)

    println("HEY SPARK PROJECT !!")
    // Create a util object 
    val utils = new Utils()

    // Create a SparkSession
    val spark :SparkSession = utils.createSparkSession()
    
    val applicationProperties = new ApplicationProperties()
    
    val constants = new Constants(hadoopUser)
       
    // Create a util object 
    val data_frame_result = new DataFrameResult(spark)
    

    println("Voulez-vous lire depuis HDFS ou en local ?")
    println("1. Lire depuis HDFS")
    println("2. Lire depuis local")

    val choixLecture = scala.io.StdIn.readLine("Entrez le numéro correspondant à votre choix : ")
    
    var country_classes: DataFrame = null
    var goods_classes: DataFrame = null
    var output_full: DataFrame = null
    var services_classes: DataFrame = null
    

    if (choixLecture == "1") {
        // L'utilisateur choisit de lire depuis HDFS
        country_classes = utils.readCSVinHDFS(spark,applicationProperties.hdfs_path_file_1,hadoopUser)
        goods_classes = utils.readCSVinHDFS(spark,applicationProperties.hdfs_path_file_2,hadoopUser)
        output_full = utils.readCSVinHDFS(spark,applicationProperties.hdfs_path_file_3,hadoopUser)
        services_classes = utils.readCSVinHDFS(spark,applicationProperties.hdfs_path_file_4,hadoopUser)

    } else if (choixLecture == "2") {
        // L'utilisateur choisit de lire en local
        country_classes = utils.readCSVLocal(spark,applicationProperties.local_path_file_1)
        goods_classes = utils.readCSVLocal(spark,applicationProperties.local_path_file_2)
        output_full = utils.readCSVLocal(spark,applicationProperties.local_path_file_3)
        services_classes = utils.readCSVLocal(spark,applicationProperties.local_path_file_4)

    } else {
        println("Choix invalide. Veuillez sélectionner 1 ou 2.")
        }

    // Afficher les options à l'utilisateur
    println(">>>>>>>>>>>>> Choisissez une requete en tappant son numero :")
    for (i <- 1 to 23) {
        println(i+". Requete "+i)
    }
    println("24_bonus. Requete 24")
    println("0 Sauvgarder toutes les requetes")
    println("<<<<<<<<<<<<<")

    // Lire l'entrée de l'utilisateur
    val input = scala.io.StdIn.readLine()
    
    val selectedQuery: Int = input.toInt
    selectedQuery match {
        case 1 =>       executeQuery1(spark,hadoopUser,utils,data_frame_result,output_full)
        case 2 =>       executeQuery2(spark,hadoopUser,utils,data_frame_result,output_full)
        case 3 =>       executeQuery3(spark,hadoopUser,utils,data_frame_result,output_full,country_classes)
        case 4 =>       executeQuery4(spark,hadoopUser,utils,data_frame_result,output_full,services_classes)
        case 5 =>       executeQuery5(spark,hadoopUser,utils,data_frame_result,output_full,goods_classes)
        case 6 =>       executeQuery6(spark,hadoopUser,utils,data_frame_result,output_full)
        case 7 =>       executeQuery7(spark,hadoopUser,utils,data_frame_result,output_full)
        case 8 =>       executeQuery8(spark,hadoopUser,utils,data_frame_result,output_full)
        case 9 =>       executeQuery9(spark,hadoopUser,utils,data_frame_result,output_full)
        case 10 =>       executeQuery10(spark,hadoopUser,utils,data_frame_result,output_full)
        case 11 =>       executeQuery11(spark,hadoopUser,utils,data_frame_result,output_full)
        case 12 =>       executeQuery12(spark,hadoopUser,utils,data_frame_result,output_full,services_classes)
        case 13 =>       executeQuery13(spark,hadoopUser,utils,data_frame_result,output_full,goods_classes)
        case 14 =>       executeQuery14(spark,hadoopUser,utils,data_frame_result,output_full,country_classes)
        case 15 =>       executeQuery15(spark,hadoopUser,utils,data_frame_result,output_full,country_classes)
        case 16 =>       executeQuery16(spark,hadoopUser,utils,data_frame_result,output_full)
        case 17 =>       executeQuery17(spark,hadoopUser,utils,data_frame_result,output_full)
        case 18 =>       executeQuery18(spark,hadoopUser,utils,data_frame_result,output_full)
        case 19 =>       executeQuery19(spark,hadoopUser,utils,data_frame_result,output_full)
        case 20 =>       executeQuery20(spark,hadoopUser,utils,data_frame_result,output_full,goods_classes)
        case 21 =>       executeQuery21(spark,hadoopUser,utils,data_frame_result,output_full)
        case 22 =>       executeQuery22(spark,hadoopUser,utils,data_frame_result,output_full)
        case 23 =>       executeQuery23(spark,hadoopUser,utils,data_frame_result,output_full)
        case 24 =>       executeQuery24(spark,hadoopUser,utils,data_frame_result,output_full,country_classes)
        case 0 => {
            executeQuery1(spark,hadoopUser,utils,data_frame_result,output_full)
            executeQuery2(spark,hadoopUser,utils,data_frame_result,output_full)
            executeQuery3(spark,hadoopUser,utils,data_frame_result,output_full,country_classes)
            executeQuery4(spark,hadoopUser,utils,data_frame_result,output_full,services_classes)
            executeQuery5(spark,hadoopUser,utils,data_frame_result,output_full,goods_classes)
            executeQuery6(spark,hadoopUser,utils,data_frame_result,output_full)
            executeQuery7(spark,hadoopUser,utils,data_frame_result,output_full)
            executeQuery8(spark,hadoopUser,utils,data_frame_result,output_full)
            executeQuery9(spark,hadoopUser,utils,data_frame_result,output_full)
             executeQuery10(spark,hadoopUser,utils,data_frame_result,output_full)
             executeQuery11(spark,hadoopUser,utils,data_frame_result,output_full)
             executeQuery12(spark,hadoopUser,utils,data_frame_result,output_full,services_classes)
             executeQuery13(spark,hadoopUser,utils,data_frame_result,output_full,goods_classes)
             executeQuery14(spark,hadoopUser,utils,data_frame_result,output_full,country_classes)
             executeQuery15(spark,hadoopUser,utils,data_frame_result,output_full,country_classes)
             executeQuery16(spark,hadoopUser,utils,data_frame_result,output_full)
             executeQuery17(spark,hadoopUser,utils,data_frame_result,output_full)
             executeQuery18(spark,hadoopUser,utils,data_frame_result,output_full)
             executeQuery19(spark,hadoopUser,utils,data_frame_result,output_full)
             executeQuery20(spark,hadoopUser,utils,data_frame_result,output_full,goods_classes)
             executeQuery21(spark,hadoopUser,utils,data_frame_result,output_full)
             executeQuery22(spark,hadoopUser,utils,data_frame_result,output_full)
             executeQuery23(spark,hadoopUser,utils,data_frame_result,output_full)
             executeQuery24(spark,hadoopUser,utils,data_frame_result,output_full,country_classes)
        }
        case _ => println("Choix non valide")
    }
    }
  def executeQuery1(spark:SparkSession,hadoop_user:String,utils:Utils,data_frame_result:DataFrameResult,output_full:DataFrame): Unit = {
    val df_requete_1 = data_frame_result.Requete_1(output_full)
    //df_requete_1.cache()

    println("REQUETE 01 ")
    df_requete_1.show()
    df_requete_1.printSchema()

    val C1 = new Constants(hadoop_user) 
    utils.writeToCSV(df_requete_1,C1.getUrlHdfsCSV("1"))
    utils.writeToParquet(df_requete_1,C1.getUrlHdfsParquet("1"))
    
  } 
  def executeQuery2(spark:SparkSession,hadoop_user:String,utils:Utils,data_frame_result:DataFrameResult,output_full:DataFrame): Unit = {
    val df_requete_2 = data_frame_result.Requete_2(output_full)
    df_requete_2.cache()

    println("REQUETE 02 ")
    df_requete_2.show()
    df_requete_2.printSchema()

    val C1 = new Constants(hadoop_user) 
    utils.writeToCSV(df_requete_2,C1.getUrlHdfsCSV("2"))
    utils.writeToParquet(df_requete_2,C1.getUrlHdfsParquet("2"))
  }
  def executeQuery3(spark:SparkSession,hadoop_user:String,utils:Utils,data_frame_result:DataFrameResult,output_full:DataFrame, country_classes:DataFrame): Unit = {
    val df_requete_3 = data_frame_result.Requete_3(output_full,country_classes)
    df_requete_3.cache()

    println("REQUETE 03 ")
    df_requete_3.show()    
    df_requete_3.printSchema()

    val C1 = new Constants(hadoop_user) 
    utils.writeToCSV(df_requete_3,C1.getUrlHdfsCSV("3"))
    utils.writeToParquet(df_requete_3,C1.getUrlHdfsParquet("3"))
  }
  def executeQuery4(spark:SparkSession,hadoop_user:String,utils:Utils,data_frame_result:DataFrameResult,output_full:DataFrame,services_classes:DataFrame): Unit = {
        val df_requete_4 = data_frame_result.Requete_4(output_full,services_classes)
    df_requete_4.cache()

    println("REQUETE 04 ")
    df_requete_4.show()
    df_requete_4.printSchema()

    val C1 = new Constants(hadoop_user) 
    utils.writeToCSV(df_requete_4,C1.getUrlHdfsCSV("4"))
    utils.writeToParquet(df_requete_4,C1.getUrlHdfsParquet("4"))
  }
  def executeQuery5(spark:SparkSession,hadoop_user:String,utils:Utils,data_frame_result:DataFrameResult,output_full:DataFrame,goods_classes:DataFrame): Unit = {
    val df_requete_5 = data_frame_result.Requete_5(output_full,goods_classes)
    df_requete_5.cache()

    println("REQUETE 05 ")
    df_requete_5.show()
    df_requete_5.printSchema()

     val C1 = new Constants(hadoop_user) 
    utils.writeToCSV(df_requete_5,C1.getUrlHdfsCSV("5"))
    utils.writeToParquet(df_requete_5,C1.getUrlHdfsParquet("5"))
  }
  def executeQuery6(spark:SparkSession,hadoop_user:String,utils:Utils,data_frame_result:DataFrameResult,output_full:DataFrame): Unit = {
    val tmp_map = data_frame_result.Requete_6(output_full)
    val df_requete_6_goods = tmp_map("goods")
    val df_requete_6_services = tmp_map("services")

    df_requete_6_goods.cache()
    df_requete_6_services.cache()

    println("REQUETE 06 ")
    println(" les pays exporatateurs par goods ")
    df_requete_6_goods.show()
    df_requete_6_goods.printSchema()

     val C1 = new Constants(hadoop_user) 
    utils.writeToCSV(df_requete_6_goods,C1.getUrlHdfsCSV("6")+"/goods_csv")
    utils.writeToParquet(df_requete_6_goods,C1.getUrlHdfsParquet("6")+"/goods_parquet")

    println(" les pays exporatateurs par services ")
    df_requete_6_services.show()
    df_requete_6_services.printSchema()

    utils.writeToCSV(df_requete_6_services,C1.getUrlHdfsCSV("6")+"/services_csv")
    utils.writeToParquet(df_requete_6_services,C1.getUrlHdfsParquet("6")+"/services_parquet")
  }
  def executeQuery7(spark:SparkSession,hadoop_user:String,utils:Utils,data_frame_result:DataFrameResult,output_full:DataFrame): Unit = {
        val tmp_map = data_frame_result.Requete_7(output_full)
        val df_requete_7_goods = tmp_map("goods")
        val df_requete_7_services = tmp_map("services")

        df_requete_7_goods.cache()
        df_requete_7_services.cache()

        println("REQUETE 07 ")
        println(" les pays imporatateurs par goods ")
        df_requete_7_goods.show()
        df_requete_7_goods.printSchema()
        val C1 = new Constants(hadoop_user) 
        utils.writeToCSV(df_requete_7_goods,C1.getUrlHdfsCSV("7")+"/goods_csv")
        utils.writeToParquet(df_requete_7_goods,C1.getUrlHdfsParquet("7")+"/goods_parquet")

        println(" les pays imporatateurs par services ")
        df_requete_7_services.show()
        df_requete_7_services.printSchema()
        utils.writeToCSV(df_requete_7_services,C1.getUrlHdfsCSV("7")+"/services_csv")
        utils.writeToParquet(df_requete_7_services,C1.getUrlHdfsParquet("7")+"/services_parquet")
  }
  def executeQuery8(spark:SparkSession,hadoop_user:String,utils:Utils,data_frame_result:DataFrameResult,output_full:DataFrame): Unit = {
    val df_requete_8 = data_frame_result.Requete_8(output_full)
    df_requete_8.cache()

    println("REQUETE 08 ")
    df_requete_8.show()
    df_requete_8.printSchema()
    val C1 = new Constants(hadoop_user) 
    utils.writeToCSV(df_requete_8,C1.getUrlHdfsCSV("8"))
    utils.writeToParquet(df_requete_8,C1.getUrlHdfsParquet("8"))
  }
  def executeQuery9(spark:SparkSession,hadoop_user:String,utils:Utils,data_frame_result:DataFrameResult,output_full:DataFrame): Unit = {
    val df_requete_9 = data_frame_result.Requete_8(output_full)
    df_requete_9.cache()

    println("REQUETE 09 ")
    df_requete_9.show()
    df_requete_9.printSchema()
    val C1 = new Constants(hadoop_user) 
    utils.writeToCSV(df_requete_9,C1.getUrlHdfsCSV("9"))
    utils.writeToParquet(df_requete_9,C1.getUrlHdfsParquet("9"))
  }
  def executeQuery10(spark:SparkSession,hadoop_user:String,utils:Utils,data_frame_result:DataFrameResult,output_full:DataFrame): Unit = {
    val df_requete_10 = data_frame_result.Requete_8(output_full)
    df_requete_10.cache()

    println("REQUETE 10 ")
    df_requete_10.show()
    df_requete_10.printSchema()
    val C1 = new Constants(hadoop_user) 
    utils.writeToCSV(df_requete_10,C1.getUrlHdfsCSV("10"))
    utils.writeToParquet(df_requete_10,C1.getUrlHdfsParquet("10"))
  }
  def executeQuery11(spark:SparkSession,hadoop_user:String,utils:Utils,data_frame_result:DataFrameResult,output_full:DataFrame): Unit = {
    val df_requete_11 = data_frame_result.Requete_11(output_full)
    df_requete_11.cache()

    println("REQUETE 11 ")
    df_requete_11.show()
    df_requete_11.printSchema()
    val C1 = new Constants(hadoop_user) 
    utils.writeToCSV(df_requete_11,C1.getUrlHdfsCSV("11"))
    utils.writeToParquet(df_requete_11,C1.getUrlHdfsParquet("11"))
  }
  def executeQuery12(spark:SparkSession,hadoop_user:String,utils:Utils,data_frame_result:DataFrameResult,output_full:DataFrame,services_classes:DataFrame): Unit = {
    val df_requete_12 = data_frame_result.Requete_12(output_full,services_classes)
    df_requete_12.cache()

    println("REQUETE 12 ")
    df_requete_12.show()
    df_requete_12.printSchema()
    val C1 = new Constants(hadoop_user) 
    utils.writeToCSV(df_requete_12,C1.getUrlHdfsCSV("12"))
    utils.writeToParquet(df_requete_12,C1.getUrlHdfsParquet("12"))
  }
  def executeQuery13(spark:SparkSession,hadoop_user:String,utils:Utils,data_frame_result:DataFrameResult,output_full:DataFrame,goods_classes:DataFrame): Unit = {
    val df_requete_13 = data_frame_result.Requete_13(output_full,goods_classes)
    df_requete_13.cache()

    println("REQUETE 13 ")
    df_requete_13.show()
    df_requete_13.printSchema()
    val C1 = new Constants(hadoop_user) 
    utils.writeToCSV(df_requete_13,C1.getUrlHdfsCSV("13"))
    utils.writeToParquet(df_requete_13,C1.getUrlHdfsParquet("13"))
  }
  def executeQuery14(spark:SparkSession,hadoop_user:String,utils:Utils,data_frame_result:DataFrameResult,output_full:DataFrame,country_classes:DataFrame): Unit = {
    val df_requete_14 = data_frame_result.Requete_14(output_full,country_classes)
    df_requete_14.cache()

    println("REQUETE 14 ")
    df_requete_14.show()
    df_requete_14.printSchema()
    val C1 = new Constants(hadoop_user) 
    utils.writeToCSV(df_requete_14,C1.getUrlHdfsCSV("14"))
    utils.writeToParquet(df_requete_14,C1.getUrlHdfsParquet("14"))
  }
  def executeQuery15(spark:SparkSession,hadoop_user:String,utils:Utils,data_frame_result:DataFrameResult,output_full:DataFrame,country_classes:DataFrame): Unit = {
    val df_requete_15 = data_frame_result.Requete_15(output_full,country_classes)
    df_requete_15.cache()

    println("REQUETE 15 ")
    df_requete_15.show()
    df_requete_15.printSchema()
    val C1 = new Constants(hadoop_user) 
    utils.writeToCSV(df_requete_15,C1.getUrlHdfsCSV("15"))
    utils.writeToParquet(df_requete_15,C1.getUrlHdfsParquet("15"))
  }
  def executeQuery16(spark:SparkSession,hadoop_user:String,utils:Utils,data_frame_result:DataFrameResult,output_full:DataFrame): Unit = {
    val df_requete_16 = data_frame_result.Requete_16(output_full)
    df_requete_16.cache()

    println("REQUETE 16 ")
    df_requete_16.show()
    df_requete_16.printSchema()
    val C1 = new Constants(hadoop_user) 
    utils.writeToCSV(df_requete_16,C1.getUrlHdfsCSV("16"))
    utils.writeToParquet(df_requete_16,C1.getUrlHdfsParquet("16"))
  }
  def executeQuery17(spark:SparkSession,hadoop_user:String,utils:Utils,data_frame_result:DataFrameResult,output_full:DataFrame): Unit = {
    val df_requete_17 = data_frame_result.Requete_17(output_full)
    df_requete_17.cache()

    println("REQUETE 17 ")
    df_requete_17.show()
    df_requete_17.printSchema()
    val C1 = new Constants(hadoop_user) 
    utils.writeToCSV(df_requete_17,C1.getUrlHdfsCSV("17"))
    utils.writeToParquet(df_requete_17,C1.getUrlHdfsParquet("17"))
  }
  def executeQuery18(spark:SparkSession,hadoop_user:String,utils:Utils,data_frame_result:DataFrameResult,output_full:DataFrame): Unit = {
    val tmp_map = data_frame_result.Requete_18(output_full)

    val df_requete_18_imports_prctg_goods = tmp_map("imports_prctg_goods")
    val df_requete_18_exports_prctg_goods = tmp_map("exports_prctg_goods")

    df_requete_18_exports_prctg_goods.cache()
    df_requete_18_imports_prctg_goods.cache()

        println("REQUETE 18 ")
        println(" le pourcentage de goods pour les imports par pays : ")
        df_requete_18_imports_prctg_goods.show()
        df_requete_18_imports_prctg_goods.printSchema()
       
       val C1 = new Constants(hadoop_user) 
        utils.writeToCSV(df_requete_18_imports_prctg_goods,C1.getUrlHdfsCSV("18"))
        utils.writeToParquet(df_requete_18_imports_prctg_goods,C1.getUrlHdfsParquet("18"))

        println(" le pourcentage de goods pour les exports par pays : ")
        df_requete_18_exports_prctg_goods.show()
        df_requete_18_exports_prctg_goods.printSchema()

        
        utils.writeToCSV(df_requete_18_exports_prctg_goods,C1.getUrlHdfsCSV("18"))
        utils.writeToParquet(df_requete_18_exports_prctg_goods,C1.getUrlHdfsParquet("18"))
  }
  def executeQuery19(spark:SparkSession,hadoop_user:String,utils:Utils,data_frame_result:DataFrameResult,output_full:DataFrame): Unit = {
    val tmp_map = data_frame_result.Requete_19(output_full)

    val df_requete_19_imports_prctg_services = tmp_map("imports_prctg_services")
    val df_requete_19_exports_prctg_services = tmp_map("exports_prctg_services")

    df_requete_19_exports_prctg_services.cache()
    df_requete_19_imports_prctg_services.cache()

    println("REQUETE 19 ")
    println(" le pourcentage de goods pour les imports par pays : ")
    df_requete_19_imports_prctg_services.show()
    df_requete_19_imports_prctg_services.printSchema()

    val C1 = new Constants(hadoop_user) 
    utils.writeToCSV(df_requete_19_imports_prctg_services,C1.getUrlHdfsCSV("19"))
    utils.writeToParquet(df_requete_19_imports_prctg_services,C1.getUrlHdfsParquet("19"))

    println(" le pourcentage de goods pour les exports par pays : ")
    df_requete_19_exports_prctg_services.show()
    df_requete_19_exports_prctg_services.printSchema()
 
    utils.writeToCSV(df_requete_19_exports_prctg_services,C1.getUrlHdfsCSV("19"))
    utils.writeToParquet(df_requete_19_exports_prctg_services,C1.getUrlHdfsParquet("19"))
  }
  def executeQuery20(spark:SparkSession,hadoop_user:String,utils:Utils,data_frame_result:DataFrameResult,output_full:DataFrame,goods_classes:DataFrame): Unit = {
    val df_requete_20 = data_frame_result.Requete_20(output_full,goods_classes)
    df_requete_20.cache()

    println("REQUETE 20 ")
    df_requete_20.show()
    df_requete_20.printSchema()
    val C1 = new Constants(hadoop_user) 
    utils.writeToCSV(df_requete_20,C1.getUrlHdfsCSV("20"))
    utils.writeToParquet(df_requete_20,C1.getUrlHdfsParquet("20"))
  }
  def executeQuery21(spark:SparkSession,hadoop_user:String,utils:Utils,data_frame_result:DataFrameResult,output_full:DataFrame): Unit = {
    val df_requete_21 = data_frame_result.Requete_21(output_full)
    df_requete_21.cache()

    println("REQUETE 21 ")
    df_requete_21.show()
    df_requete_21.printSchema()
    val C1 = new Constants(hadoop_user) 
    utils.writeToCSV(df_requete_21,C1.getUrlHdfsCSV("21"))
    utils.writeToParquet(df_requete_21,C1.getUrlHdfsParquet("21"))
  }
  def executeQuery22(spark:SparkSession,hadoop_user:String,utils:Utils,data_frame_result:DataFrameResult,output_full:DataFrame): Unit = {
    val df_requete_22 = data_frame_result.Requete_22(output_full)
    df_requete_22.cache()

    println("REQUETE 22 ")
    df_requete_22.show()
    df_requete_22.printSchema()
    val C1 = new Constants(hadoop_user) 
    utils.writeToCSV(df_requete_22,C1.getUrlHdfsCSV("22"))
    utils.writeToParquet(df_requete_22,C1.getUrlHdfsParquet("22"))
  }
  def executeQuery23(spark:SparkSession,hadoop_user:String,utils:Utils,data_frame_result:DataFrameResult,output_full:DataFrame): Unit = {
    val df_requete_23 = data_frame_result.Requete_23(output_full)
    df_requete_23.cache()

    println("REQUETE 23 ")
    df_requete_23.show()
    df_requete_23.printSchema()
    val C1 = new Constants(hadoop_user) 
    utils.writeToCSV(df_requete_23,C1.getUrlHdfsCSV("23"))
    utils.writeToParquet(df_requete_23,C1.getUrlHdfsParquet("23"))
  }
  def executeQuery24(spark:SparkSession,hadoop_user:String,utils:Utils,data_frame_result:DataFrameResult,output_full:DataFrame,country_classes:DataFrame): Unit = {
    val df_requete_24_bonus = data_frame_result.Requete_24_bonus(output_full,country_classes)
    df_requete_24_bonus.cache()

    println("REQUETE 24_bonus ")
    print(df_requete_24_bonus.select("description").head().get(0))
    df_requete_24_bonus.printSchema()
    val C1 = new Constants(hadoop_user) 
    utils.writeToCSV(df_requete_24_bonus,C1.getUrlHdfsCSV("24"))
    utils.writeToParquet(df_requete_24_bonus,C1.getUrlHdfsParquet("24"))
  }

 
}
