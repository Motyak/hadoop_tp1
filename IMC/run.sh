#!/bin/sh

# Nom de la classe contenant la méthode main (sans .java)
MAIN=$1
SRC=src/*.java
HADOOP_CLASSPATH="$(${HADOOP_HOME}/bin/hadoop classpath)"
OUT_CLASS_DIR=bin
OUT_JAR_DIR=.

mkdir -p "$OUT_CLASS_DIR" "$OUT_JAR_DIR"

# compilation avec le classpath de hadoop
javac -classpath "$HADOOP_CLASSPATH" -d $OUT_CLASS_DIR $SRC &&

# creation du jar executable
jar -cf $OUT_JAR_DIR/out.jar -C $OUT_CLASS_DIR . &&

(
# nettoyage du dossier output du hdfs
${HADOOP_HOME}/bin/hdfs dfs -rm -r output

# execution du jar en passant input et output en arguments
${HADOOP_HOME}/bin/hadoop jar out.jar $MAIN input output
)
