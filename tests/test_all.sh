#! /bin/bash

# Need to more to local dir to run tests
starting_working_dir=$(pwd)
script_name=$(basename "$0")
script_dir=$(dirname "$(realpath "$0")")

cd $script_dir

# Gather all other test files and run pytest
search_dir=.
for entry in "$search_dir"/*
do
  if  [[ $entry == ./test* ]] && [[ $entry != ./$script_name ]];
  then
    pytest $entry "-W ignore::DeprecationWarning"
  fi
done

# Move back to where we called from
cd $starting_working_dir