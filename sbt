#!/bin/bash

SCALA="-XX:MaxPermSize=256m -Xms1G -Xmx1G -Xss8M"
# More Optional Scala Opts
# -XX:+UseTLAB -XX:+AggressiveOpts -XX:+UseFastAccessorMethods"

# OPTIONAL GC STRATEGY
# GC_STRATEGY="-XX:+UseParallelGC -XX:+UseParallelOldGC"

java $SCALA  -jar `dirname $0`/sbt-launch.jar "$@"

# old version 
# java -Xmx512M -jar `dirname $0`/sbt-launch.jar "$@"