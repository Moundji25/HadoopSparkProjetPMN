#!/bin/bash

# Vérifie si un argument est passé
if [ $# -eq 0 ]; then
    echo "Veuillez spécifier le préfixe du chemin 'user/nom_user' en argument."
    exit 1
fi

user_prefix="$1"

# Chemin complet vers le dépôt input
hdfs_input_dir="/user/$user_prefix/input"

# Suppression du contenu du dépôt input
hdfs dfs -rm -r -f "$hdfs_input_dir/*"

