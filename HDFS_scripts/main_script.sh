#!/bin/bash

# Vérification des arguments passés en ligne de commande
if [ "$#" -ne 3 ]; then
    echo "Usage: <hadoop_user> <local_file_path> <jar_file>"
    exit 1
fi

# Récupération des arguments
hadoop_user="$1"
local_file_path="$2"
jar_file="$3"

echo "Début du script principal..."

# Exécution des scripts dans l'ordre

echo "Début de verify_input.sh..."
./scripts/verify_input.sh "$hadoop_user"
echo "Fin de verify_input.sh"

verify_ret=$?

# Si verify_input.sh a retourné 1, exécute copy_data_input.sh
if [ "$verify_ret" -eq 1 ]; then
    echo "Début du script copy_data_input.sh"
    ./scripts/copy_data_input.sh "$hadoop_user" "$local_file_path"
    echo "Fin du script copy_data_input.sh"
fi

echo "Début de copy_data_tmp.sh..."
./scripts/copy_data_tmp.sh "$hadoop_user"
echo "Fin de copy_data_tmp.sh."

echo "Début de run_spark.sh..."
./scripts/run_spark.sh "$hadoop_user" "$jar_file"
echo "Fin de run_spark.sh."

echo "Début de delete_tmp.sh..."
./scripts/delete_tmp.sh "$hadoop_user"
echo "Fin de delete_tmp.sh."

echo "Début de delete_content_input.sh..."
./scripts/delete_content_input.sh "$hadoop_user"
echo "Fin de delete_content_input.sh."

echo "Script principal terminé."

