#!/bin/bash

# Tests all possible feature combinations.
# Usage: ./test_all_features.sh [-c], -c runs cargo check instead of cargo test

# Define an array of feature names
features=("logging" "toml_config" "unicache")

# Initialize a variable to track whether to run the tests or not
check_only=false

# Process command line arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        -c)
            check_only=true
            shift
            ;;
        *)
            echo "Invalid argument: $1"
            exit 1
            ;;
    esac
done

# Get the total number of features
num_features=${#features[@]}

# Calculate the total number of feature combinations (2^num_features)
total_combinations=$((1 << num_features))

# Initialize a variable to store the failing combination
failing_combinations=""

# Loop through all possible feature combinations
for ((i = 0; i < total_combinations; i++)); do
    feature_flags="--features macros"

    # Generate feature flags for the current combination
    for ((j = 0; j < num_features; j++)); do
        if (( (i >> j) & 1 )); then
            feature_flags+=",${features[j]}"
        fi
    done

    if [ "$check_only" = true ]; then
        # Run cargo check with the current feature combination
        echo "Checking with features: ${feature_flags}"
        cargo check $feature_flags
    else
        # Run cargo test with the current feature combination
        echo "Testing with features: ${feature_flags}"
        cargo test $feature_flags
    fi

    # Check if cargo test failed
    if [ $? -ne 0 ]; then
        echo "Cargo failed with combination ${feature_flags}"
        failing_combinations+="\n${feature_flags}"
    fi
done

# Check if any combination failed and print the failing combinations
if [ -n "$failing_combinations" ]; then
    echo -e "Failed feature combinations:$failing_combinations"
    exit 1
fi

echo "All feature combinations tested successfully!"