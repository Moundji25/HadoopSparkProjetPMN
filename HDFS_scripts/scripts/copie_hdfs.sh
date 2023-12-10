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

echo -e " >>>> Script qui permet de vérifier l'existence d'un fichier/répertoire puis de le copier en tapant un des ses choix en argument : \n- \"-lh\" du local vers HDFS \n- \"-hl\" de HDFS vers local \n- \"-hh\" de HDFS à HDFS\n"

# Récupère les arguments
argument="$1"
argument_2="$2"
argument_3="$3"

# Appelle la fonction pour vérifier l'existence de l'argument
echo "Vérification du premier argument : "
verifier_argument_existence "$argument"
echo "Vérification du deuxiéme argument : "
verifier_argument_existence "$argument_2"
echo "Vérification du troisiéme argument : "
verifier_argument_existence "$argument_3"

# FONCTION A FAIRE : verifier_argument_nombre

case "$argument" in
    -lh)
        echo "L'argument correspond à une copie Locale >> HDFS\n En cours de copie locale ..."

        # Appel de existence.sh pour vérifier l'existence de l'argument sur HDFS
        ./existence.sh "$argument_3"
        existence_res=$?
        
        if [ "$existence_res" -eq 0 ]; then
            hadoop fs -copyFromLocal "$argument_2" "$argument_3"
        fi
        ;;
    -hl)
        echo "L'argument correspond à une copie HDFS >> Locale.\n En cours de copie locale ..."

        # Appel de existence.sh pour vérifier l'existence de l'argument sur HDFS
        ./existence.sh "$argument_2"
        existence_res=$?

        if [ "$existence_res" -eq 0 ]; then
            hadoop fs -copyToLocal "$argument_2" "$argument_3"
        fi
        ;;
    -hh)
        echo -e "L'argument correspond à une copie HDFS >> HDFS.\n En cours de copie locale ..."

        # Appel de existence.sh pour vérifier l'existence des arguments sur HDFS
        ./existence.sh "$argument_2"
        existence_res=$?

        ./existence.sh "$argument_3"
        existence_res_2=$?

        if [ "$existence_res" -eq 0 ] && [ "$existence_res_2" -eq 0 ]; then
            hadoop fs -cp "$argument_2" "$argument_3"
        fi
        ;;
    *)
        echo -e "Argument non spécifié\n- \"-lh\" du local vers HDFS \n- \"-hl\" de HDFS vers local \n- \"-hh\" de HDFS à HDFS\n"
        ;;
esac


