#!/bin/bash

# Vérifie si un argument est passé
if [ $# -eq 0 ]; then
    echo "Veuillez spécifier le préfixe du chemin 'user/nom_user' en argument."
    exit 1
fi

user_prefix="$1"

# Chemin complet vers le dossier data_tmp
hdfs_data_tmp_dir="/user/$user_prefix/data_tmp"

# Suppression du dossier data_tmp et de son contenu
hdfs dfs -rm -r -f "$hdfs_data_tmp_dir"

