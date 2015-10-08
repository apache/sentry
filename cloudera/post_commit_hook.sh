#!/bin/bash
echo "Running tests using JDK7"

set -x
export JAVA7_BUILD=true
source /mnt/toolchain/toolchain.sh

find . -name test-classes | grep target/test-classes | xargs rm -rf

mvn clean compile package -DskipTests=true
mvn test
