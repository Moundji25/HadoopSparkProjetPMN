import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame

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
        df.withColumn("year",year(col("date")))
    }
    def Requete_3 (df_output_full :DataFrame,df_country_classes :DataFrame ): DataFrame = 
    {
        //val tmp_df = df_country_classes.withColumnRenamed("country_code","country_code_1")
        //tmp_df.cache()

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
            df_goods_classes.withColumnRenamed("NZHSC_Level_1_Code_HS2","code_1"),
            col("code") === col("code_1"),
            "left_outer"
            ).withColumnRenamed("NZHSC_Level_1","details_goods").drop("code_1").drop("NZHSC_Level_2").drop("NZHSC_Level_2_Code_HS4")
    }
    def Requete_6 (df :DataFrame): Map[String,DataFrame] = 
    {
        window_spec = Window.orderBy(col("count").desc)

        var tmp_grouped = df.filter(col("product_type") === "Goods").filter(col("account") === "Exports")\
        .groupBy("country_code").count()

        // Classer les données en fonction du nombre d'occurences
        val rank_goods = tmp_grouped.withColumn("rank", rank().over(window_spec)).drop(col("rank"))
        
        tmp_grouped = df.filter(col("product_type") === "Service").filter(col("account") === "Exports")\
        .groupBy("country_code").count()

        val rank_services= tmp_grouped.withColumn("rank", rank().over(window_spec)).drop(col("rank"))

        return Map("goods" -> rank_goods, "services" -> rank_services)


    }
    /*def Requete_7 (df :DataFrame): DataFrame = 
    {

    }
    def Requete_8 (df :DataFrame): DataFrame = 
    {

    }*/
}