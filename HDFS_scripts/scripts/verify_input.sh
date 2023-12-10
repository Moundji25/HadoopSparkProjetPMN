#!/bin/bash

# Vérifie si un argument est passé
if [ $# -eq 0 ]; then
    echo "Veuillez spécifier le préfixe du chemin 'user/nom_user' en argument."
    exit 1
fi

user_prefix="$1"

# Chemin complet vers le dépôt input
hdfs_input_dir="/user/$user_prefix/chemin/vers/le/depot/input"

if hdfs dfs -test -z "$hdfs_input_dir"; then
    echo "Le dépôt input est vide."
    exit 0
else
    echo "Le dépôt input n'est pas vide."
    exit 1
fi

