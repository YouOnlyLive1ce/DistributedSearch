#!/bin/bash
# Start ssh server
service ssh restart 

# Starting the services
bash start-services.sh

# Creating a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install any packages
pip install -r requirements.txt  

# Package the virtual env.
venv-pack -o .venv.tar.gz

# Collect data
# bash prepare_data.sh

# Run the indexer (not running automatically?)
# bash index.sh

# Fill cassandra db
# bash create_tables.sh

# Run the ranker
# bash search.sh "this is a query!"

# do not stop cluster master docker
tail -f /dev/null