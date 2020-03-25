#!/bin/bash

WORK_DIR=$(cd $(dirname $0); pwd)
cd $WORK_DIR

echo $(pwd)

echo "JAVA_OPTS=$JAVA_OPTS"
echo "DEFAULT_JAVA_OPTS=$DEFAULT_JAVA_OPTS"
echo "PROGRAM_ARGS=$PROGRAM_ARGS"

if [ -z "$PROGRAM_ARGS" ]; then
    echo 'No program arguments present'
fi

RUNNABLE_JAR=$(find /opt/app/pkg -name '*.jar')

echo "Starting fat jar Java application with:"
echo "GLOBAL_JAVA_OPTS=$GLOBAL_JAVA_OPTS"
echo "JAVA_OPTS=$JAVA_OPTS"
echo "PROGRAM_ARGS=$PROGRAM_ARGS"
java $JAVA_OPTS $GLOBAL_JAVA_OPTS -jar $RUNNABLE_JAR $PROGRAM_ARGS
