import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window


case class DataFrameResult(val spark : SparkSession)
{
    def Requete_1 (df :DataFrame): DataFrame = 
    {
        // time_ref,"account","code"
        //,"country_code","product_type",value,"status"
        df.withColumn("date", to_timestamp(col("time_ref").cast("String"), "yyyyMM"))
    }
    def Requete_2 (df :DataFrame): DataFrame = 
    {
        df.withColumn("date", to_timestamp(col("time_ref").cast("String"), "yyyyMM")).withColumn("year",year(col("date")))
    }
    def Requete_3 (df_output_full :DataFrame,df_country_classes :DataFrame ): DataFrame = 
    {
        //country_code,country_label
        df_output_full.join(
            df_country_classes.withColumnRenamed("country_code","country_code_1"),
            col("country_code") === col("country_code_1"),
            "left_outer"
            ).withColumnRenamed("country_label","nom_Pays").drop("country_code_1")
    }
    
    def Requete_4 (df_output_full :DataFrame,df_services_classes :DataFrame): DataFrame = 
    {
        df_output_full.filter(col("product_type") === "Services").join(
            df_services_classes.withColumnRenamed("code","code_1"),
            col("code") === col("code_1"),
            "left_outer"
            ).withColumnRenamed("service_label","details_services").drop("code_1")
    }
    def Requete_5 (df_output_full :DataFrame,df_goods_classes :DataFrame): DataFrame = 
    {
         df_output_full.filter(col("product_type") === "Goods").join(
            df_goods_classes.withColumnRenamed("NZHSC_Level_2_Code_HS4","code_1"),
            col("code") === col("code_1"),
            "left_outer"
            ).withColumnRenamed("NZHSC_Level_1","details_goods").drop("code_1").drop("NZHSC_Level_2").drop("NZHSC_Level_1_Code_HS2")
    }
    def Requete_6 (df :DataFrame): Map[String,DataFrame] = 
    {
        val window_spec = Window.orderBy(col("count").desc)

        val tmp_grouped = df.filter(col("product_type") === "Goods").filter(col("account") === "Exports").groupBy("country_code").count()
        val rank_goods = tmp_grouped.withColumn("rank", rank().over(window_spec)).drop(col("count"))
        
        
        val tmp_grouped_2 = df.filter(col("product_type") === "Services").filter(col("account") === "Exports").groupBy("country_code").count()
        val rank_services= tmp_grouped_2.withColumn("rank", rank().over(window_spec)).drop(col("count"))

        return Map("goods" -> rank_goods, "services" -> rank_services)
    }
    def Requete_7 (df :DataFrame): Map[String,DataFrame] = 
    {
        val window_spec = Window.orderBy(col("count").desc)

        val tmp_grouped = df.filter(col("product_type") === "Goods").filter(col("account") === "Imports").groupBy("country_code").count()
        val rank_goods = tmp_grouped.withColumn("rank", rank().over(window_spec)).drop(col("count"))
        
        val tmp_grouped_2 = df.filter(col("product_type") === "Services").filter(col("account") === "Imports").groupBy("country_code").count()
        val rank_services= tmp_grouped_2.withColumn("rank", rank().over(window_spec)).drop(col("count"))

        return Map("goods" -> rank_goods, "services" -> rank_services)
    }
    def Requete_8 (df :DataFrame): DataFrame = 
    {
        df.filter(col("product_type") === "Goods" ).groupBy(col("product_type")).count()
    }
    def Requete_9 (df :DataFrame): DataFrame = 
    {
        df.filter(col("product_type") === "Services" ).groupBy(col("product_type")).count()
    }
    def Requete_10 (df :DataFrame): DataFrame = 
    {
        df.where((col("account") === "Exports") and (col("country_code") === "FR") and (col("product_type") === "Goods" ))
    }
    def Requete_11 (df :DataFrame): DataFrame = 
    {
        df.where((col("account") === "Imports") and (col("country_code") === "FR")  (col("product_type") === "Services" ))
    }
    def Requete_12 (df :DataFrame, df_services : DataFrame): DataFrame = 
    {
        // SOLUTION WINDOW MARCHE :)
        //val window_spec = Window.partitionBy(col("service_label"))

        //df.filter(col("product_type") === "Services").join(df_services.withColumnRenamed("code","code_1"),col("code") === col("code_1"),"left_outer").withColumn(
        //    "nbre_occurence", count("*").over(window_spec)).orderBy(col("nbre_occurence")).select("service_label","nbre_occurence").distinct()
        val window_spec = Window.orderBy(("nbre_occurences"))

        df_services.join(df.filter(col("product_type") === "Services").withColumnRenamed("code","code_1"), col("code") === col("code_1"),"left_outer").groupBy(
            col("code_1"),col("service_label")).count().withColumnRenamed("count","nbre_occurences").withColumn("rang",rank().over(window_spec))
    }
    def Requete_13 (df :DataFrame, df_goods : DataFrame): DataFrame = 
    {
        // SOLUTION WINDOW MARCHE :)
        //val window_spec = Window.partitionBy(col("NZHSC_Level_2_Code_HS4"),col("NZHSC_Level_1_Code_HS2"))

        //df.filter(col("product_type") === "Goods").join(df_goods,( col("code") === col("NZHSC_Level_2_Code_HS4") || col("code") === col("NZHSC_Level_1_Code_HS2")),"left_outer").withColumn(
        //    "nbre_occurence", count("*").over(window_spec)).orderBy(desc("nbre_occurence")).select("NZHSC_Level_2","NZHSC_Level_1","nbre_occurence").distinct()

        val window_spec = Window.orderBy(desc("nbre_occurences"))

        df_goods.join(df.filter(col("product_type") === "Goods"), col("code") === col("NZHSC_Level_2_Code_HS4") || col("code") === col("NZHSC_Level_1_Code_HS2"),"left_outer").groupBy(
            col("NZHSC_Level_2_Code_HS4"),col("NZHSC_Level_1_Code_HS2"),col("NZHSC_Level_1")).count().withColumnRenamed("count","nbre_occurences").withColumn("rang",rank().over(window_spec))
    }
    def Requete_14 (df :DataFrame, df_countries: DataFrame): DataFrame = 
    {
        // SOLUTION WINDOW A REVOIR !!! 
        //val window_spec = Window.partitionBy(col("country_code"))

        //val df_imports = df.filter(col("account") === "Imports").withColumn("status_import", count("*").over(window_spec)).withColumnRenamed("code","code_1")

        //val tmp_df = df.filter(col("account") === "Exports").withColumn("status_export", count("*").over(window_spec)).withColumnRenamed("country_code","country_code_1").join(
        //    df_imports,col("code") === col("code_1"),"left_outer"
        //).select("status_import","status_export","country_code_1")

        /*df_countries.join( tmp_df,
            col("country_code") === col("country_code_1"),
            "left_outer"
        ).drop("country_code_1").withColumn("status_import_export", when(col("status_import") > col("status_export"), lit("N")).otherwise(lit("P"))).select(
            "country_code","country_label","status_import_export"
        ).distinct()*/

        // SOLUTION GROUP BY 
        val df_imports = df.filter(col("account") === "Imports").groupBy(col("country_code")).sum("value").withColumnRenamed("sum(value)","status_import").withColumnRenamed("country_code","country_code_1")
        val df_exports = df.filter(col("account") === "Exports").groupBy(col("country_code")).sum("value").withColumnRenamed("sum(value)","status_export").withColumnRenamed("country_code","country_code_1")
        df_countries.join(df_exports,col("country_code") === col("country_code_1") ,"left_outer").drop(col("country_code_1")).join(df_imports,col("country_code") === col("country_code_1"),"left_outer").withColumn(
            "status_import_export", when(col("status_import") > col("status_export"), lit("N")).otherwise(lit("P"))).select("country_code","country_label","status_import_export")
    }
    def Requete_15 (df :DataFrame, df_countries: DataFrame): DataFrame = 
    {
        val df_imports = df.filter(col("account") === "Imports").groupBy(col("country_code")).sum("value").withColumnRenamed("sum(value)","status_import").withColumnRenamed("country_code","country_code_1")
        val df_exports = df.filter(col("account") === "Exports").groupBy(col("country_code")).sum("value").withColumnRenamed("sum(value)","status_export").withColumnRenamed("country_code","country_code_1")
        df_countries.join(df_exports,col("country_code") === col("country_code_1") ,"left_outer").drop(col("country_code_1")).join(df_imports,col("country_code") === col("country_code_1"),"left_outer").withColumn(
            "difference_import_export", col("status_export") - col("status_import") ).select("country_code","country_label","difference_import_export")
    }
    def Requete_16 (df :DataFrame): DataFrame = 
    {
       df.filter(col("product_type") === "Goods").groupBy(col("country_code")).sum("value").withColumnRenamed("sum(value)","somme_good")
    }
    def Requete_17 (df :DataFrame): DataFrame = 
    {
        df.filter(col("product_type") === "Services").groupBy(col("country_code")).sum("value").withColumnRenamed("sum(value)","somme_service")
    }
    def Requete_18 (df :DataFrame): Map[String,DataFrame]  = 
    {
       val window_spec = Window.partitionBy(col("product_type"))
       val window_spec_2 = Window.partitionBy(col("country_code"))

       val df_goods_prctg_imports = df.filter(col("product_type") === "Goods").filter(col("account") === "Imports").withColumn("nbre_totale_goods", count("*").over(window_spec)).withColumn(
        "nbre_goods_pays",count("*").over(window_spec_2)
       ).withColumn("prctg_goods",col("nbre_goods_pays") / col("nbre_totale_goods")).groupBy(col("country_code"),col("product_type"),col("prctg_goods")).count().drop("count")


       val df_goods_prctg_exports = df.filter(col("product_type") === "Goods").filter(col("account") === "Exports").withColumn("nbre_totale_goods", count("*").over(window_spec)).withColumn(
        "nbre_goods_pays",count("*").over(window_spec_2)
       ).withColumn("prctg_goods",col("nbre_goods_pays") / col("nbre_totale_goods")).drop("nbre_totale_goods","nbre_goods_pays").groupBy(col("country_code"),col("product_type"),col("prctg_goods")).count().drop("count")

       return Map("imports_prctg_goods" -> df_goods_prctg_imports, "exports_prctg_goods" -> df_goods_prctg_exports)
    }

    def Requete_19(df :DataFrame): Map[String,DataFrame]  = 
    {
       val window_spec = Window.partitionBy(col("product_type"))
       val window_spec_2 = Window.partitionBy(col("country_code"))

       val df_goods_prctg_imports = df.filter(col("product_type") === "Services").filter(col("account") === "Imports").withColumn("nbre_totale_services", count("*").over(window_spec)).withColumn(
        "nbre_services_pays",count("*").over(window_spec_2)
       ).withColumn("prctg_services",col("nbre_services_pays") / col("nbre_totale_services")).groupBy(col("country_code"),col("product_type"),col("prctg_services")).count().drop("count")


       val df_goods_prctg_exports = df.filter(col("product_type") === "Services").filter(col("account") === "Exports").withColumn("nbre_totale_services", count("*").over(window_spec)).withColumn(
        "nbre_services_pays",count("*").over(window_spec_2)
       ).withColumn("prctg_services",col("nbre_services_pays") / col("nbre_totale_services")).drop("nbre_totale_services","nbre_services_pays").groupBy(col("country_code"),col("product_type"),col("prctg_services")).count().drop("count")

       return Map("imports_prctg_services" -> df_goods_prctg_imports, "exports_prctg_services" -> df_goods_prctg_exports)
    }

    def Requete_20 (df :DataFrame, df_goods:DataFrame): DataFrame = 
    {
        val tmp_df = df.filter(col("product_type") === "Goods").withColumn("code_HS2", when(length(col("code")) > 2, substring(col("code"), 1, 2)).otherwise(col("code"))
            ).groupBy("code_HS2").count().withColumnRenamed("count","nbre_de_goods")
        
        df_goods.join(
                tmp_df,col("code_HS2") === col("NZHSC_Level_1_Code_HS2"),"left_outer"
            ).drop("code_HS2")  
    }

    def Requete_21 (df :DataFrame): DataFrame = 
    {
        val window_spec = Window.orderBy(desc("exported_oil_value"))

        df.filter(col("code").isin("2709", "2710")).filter(col("account") === "Exports").groupBy(col("country_code")).sum("value").withColumnRenamed("sum(value)","exported_oil_value").withColumn("exports_rank_oil",rank().over(window_spec))
    }

    def Requete_22 (df :DataFrame): DataFrame = 
    {
        val window_spec = Window.orderBy(desc("imported_meat_value"))

        df.filter(col("code").isin(
            "0201", "0202", "0203", "0204", "0205", "0206", "0207", "0208", "0209", "0210")).filter(
                col("account") === "Imports").groupBy(col("country_code")).sum("value").withColumnRenamed("sum(value)","imported_meat_value").withColumn("imports_rank_meat",rank().over(window_spec))
    }
    
    def Requete_23 (df :DataFrame): DataFrame = 
    {
        val window_spec = Window.orderBy(desc("demand_exported_tech_services"))    
         df.filter(col("code").startsWith("A1209")).filter(
                col("account") === "Exports").groupBy(col("country_code")).count().withColumnRenamed("count","demand_exported_tech_services").withColumn(
                    "demand_for_exported_tech_rank", rank().over(window_spec))

    }

    def Requete_24_bonus (df :DataFrame, df_countries : DataFrame): DataFrame = 
    {
        df.join(
            df_countries.withColumnRenamed("country_code","country_code_1"),col("country_code") === col("country_code_1"),"left_outer").drop("country_code_1").withColumn(
                "description", concat(lit("le pays "),col("country_label"),lit(" fait un "),
                when(col("account") === "Imports", lit("IMPORT")).otherwise(lit("EXPORT")), lit(" sur "), col("product_type")))
    }
}