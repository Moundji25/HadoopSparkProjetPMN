#!/bin/bash

# Fonction qui permet de vérifier l'existence d'un argument
verifier_argument_existence() {
    local argument="$1"
    if [ -n "$argument" ]; then
        echo -e "L'argument existe et n'est pas vide.\n"
    else
        echo -e "L'argument est vide ou n'existe pas.\n"
        exit 1  # Quitte le script en cas d'argument manquant.
    fi
}

echo -e " >>>> Script qui permet de vérifier l'existence d'un fichier/répertoire puis de le supprimer\n EXEMPLE : -r ou -R pour les répertoires, exemple : script.sh -r mon-repertoire\n-f ou -F pour les fichiers, exemple : script.sh -f mon-fichier <<<<\n\n"

# Récupère le premier argument
argument="$1"
argument_2="$2"  # Pas d'espace entre le signe égal (=)

# Appelle la fonction pour vérifier l'existence de l'argument
echo "Vérification du premier argument : "
verifier_argument_existence "$argument"
echo "Vérification du deuxième argument : "
verifier_argument_existence "$argument_2"

# Appel du script_1.sh pour vérifier l'existence de l'argument 2
./existence.sh "$argument_2"
existence_res=$?

if [ "$existence_res" -eq 0 ]; then  # Notez l'utilisation de [ ] et -eq pour la comparaison
    echo -e "Le fichier/répertoire existe bien (dans script_1.sh) \n"

    # Vérifiez si l'argument est un répertoire
    if [[ "$argument" == "-r" || "$argument" == "-R" ]]; then
        echo "L'argument correspond à un répertoire\n En cours de suppression ..."
        hadoop fs -rm -r "$argument_2"

    # Vérifiez si l'argument est un fichier
    elif [[ "$argument" == "-f" || "$argument" == "-F" ]]; then
        echo "L'argument correspond à un fichier.\n En cours de suppression ..."
        hadoop fs -rm "$argument_2"

    else
        echo -e "Argument non spécifié\n-r ou -R pour les répertoires, exemple : script.sh -r mon-repertoire\n-f ou -F pour les fichiers, exemple : script.sh -f mon-fichier\n"
    fi

fi

