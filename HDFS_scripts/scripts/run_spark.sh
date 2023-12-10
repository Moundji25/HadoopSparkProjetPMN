#!/bin/bash

# arguments passés en entrée
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <hadoop_user> <jar_file>"
    exit 1
fi


hadoop_user="$1"
jar_file="$2"

# si le dossier 'input' existe dans HDFS sinon le creer
if ! hadoop fs -test -d "/user/$hadoop_user/input"; then
    hadoop fs -mkdir "/user/$hadoop_user/input"
    echo "Dossier 'input' créé dans HDFS."
fi

# dossiers requetes
for i in {1..24}
do
    hadoop fs -mkdir -p "/user/$hadoop_user/requete_$i"
done

# Spark submit avec le nom d'utilisateur Hadoop et le fichier .jar
spark-submit "$jar_file" "$hadoop_user"

