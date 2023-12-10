#!/bin/bash

# Vérifie si deux arguments sont passés
if [ $# -ne 2 ]; then
    echo "Veuillez spécifier le préfixe du chemin 'user/nom_user' et le chemin local des fichiers ou du dossier en argument."
    exit 1
fi

user_prefix="$1"
local_path="$2"

# Chemin complet vers le dépôt input
hdfs_input_dir="/user/$user_prefix/input"

# Vérifie si le chemin local est un dossier ou un fichier
if [ -d "$local_path" ]; then
    # Si c'est un dossier, copie tous les fichiers du dossier dans le dépôt input
    for file in "$local_path"/*; do
        hdfs dfs -put "$file" "$hdfs_input_dir/"
    done
    echo "Tous les fichiers du dossier '$local_path' ont été copiés dans le dépôt input."    

else
    # Si c'est un fichier, copie ce fichier dans le dépôt input
    hdfs dfs -put "$local_path" "$hdfs_input_dir/"
    echo "Le fichier '$local_path' a été copié dans le dépôt input."
fi

