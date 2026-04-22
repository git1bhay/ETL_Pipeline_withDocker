#!/bin/bash



# Create subfolders
mkdir -p api etl db

# Create files inside api
touch api/main.py
touch api/requirements.txt
touch api/Dockerfile

# Create files inside etl
touch etl/pipeline.py
touch etl/requirements.txt
touch etl/Dockerfile

# Create files inside db
touch db/init.sql

# Create root-level files
touch docker-compose.yml
touch .env
touch README.md

echo "✅ Project structure created successfully!"