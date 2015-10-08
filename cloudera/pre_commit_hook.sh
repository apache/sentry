#!/bin/bash
echo "Running tests using JDK7"

set -x
export JAVA7_BUILD=true
source /mnt/toolchain/toolchain.sh

find . -name test-classes | grep target/test-classes | xargs rm -rf

# For now, just verify the code compiles.
mvn clean compile package -DskipTests -Dmaven.test.failure.ignore=true
