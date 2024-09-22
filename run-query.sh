#!/bin/bash

# Function to display usage information
usage() {
    echo "Usage: $0 -i [INPUT]"
    exit 1
}

# Check if the correct number of arguments are provided
if [ $# -eq 0 ]; then
    usage
fi

# Parse the input flag
while getopts "i:" opt; do
  case $opt in
    i)
      INPUT=$OPTARG
      ;;
    *)
      usage
      ;;
  esac
done

# Check if INPUT is set
if [ -z "$INPUT" ]; then
    usage
fi

# Find the full path to storm command
STORM_PATH=$(which storm)

# Script to execute the steps
sudo -i <<EOF
cd /local/repository/storm
mvn clean package

# Run the Storm jar with the full path to storm
${STORM_PATH} jar ./target/storm-benchmarking-1.0-SNAPSHOT.jar pdsp.${INPUT}.Main

# Wait for 60 seconds
sleep 120

# Kill the Storm topology
${STORM_PATH} kill ${INPUT}
EOF
