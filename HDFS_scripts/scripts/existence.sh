#!/bin/bash

verifier_argument_existence() {
    local argument="$1"
    if [ -n "$argument" ]; then
        echo "L'argument existe et n'est pas vide."
    else
        echo "L'argument est vide ou n'existe pas."
        exit 1  # Quitte le script en cas d'argument manquant.
    fi
}

echo -e "\n\n   >>>>Script qui teste l'existence d'un fichier ou d'un répertoire sur HDFS\n Attention: pour les fichiers il faut fournir le chemin complet du fichier <<<< \n\n"

# Récupère le premier argument
argument="$1"

# Appelle la fonction pour vérifier l'existence de l'argument
verifier_argument_existence "$argument"

# Vérifiez si le fichier ou répertoire existe
if hadoop fs -test -e "$argument" ; then
    echo "Le fichier/répertoire existe."
    exit 0
else
    echo "Le fichier/répertoire n'existe pas."
    exit 1
fi



