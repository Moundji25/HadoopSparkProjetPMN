#!/bin/bash


if [ $# -eq 0 ]; then
    echo "Veuillez spécifier le préfixe du chemin 'user/nom_user' en argument."
    exit 1
fi

user_prefix="$1"

# Chemin complet vers le dépôt input
hdfs_input_dir="/user/$user_prefix/input"

# Chemin complet vers le dossier data_tmp
hdfs_data_tmp_dir="/user/$user_prefix/data_tmp"

# Création du dossier data_tmp sur HDFS
hdfs dfs -mkdir -p "$hdfs_data_tmp_dir"

# Copie des fichiers présents dans l'input vers data_tmp
hdfs dfs -cp "$hdfs_input_dir/*" "$hdfs_data_tmp_dir/"

