class ApplicationProperties(
    val local_path_file_1 :String = "ressources/country_classification.csv",
    val local_path_file_2 :String = "ressources/goods_classification.csv",
    val local_path_file_3 :String = "ressources/output_csv_full.csv",
    val local_path_file_4 :String = "ressources/services_classification.csv",

    //val local_path_file_1 :String = "src/ressources/country_classification.csv",
    //val local_path_file_2 :String = "src/ressources/goods_classification.csv",
    //val local_path_file_3 :String = "src/ressources/output_csv_full.csv",
    //val local_path_file_4 :String = "src/ressources/services_classification.csv",

    val hdfs_path_file_1 :String = "input/country_classification.csv",
    val hdfs_path_file_2 :String = "input/goods_classification.csv",
    val hdfs_path_file_3 :String = "input/output_csv_full.csv",
    val hdfs_path_file_4 :String = "input/services_classification.csv",
)
{}