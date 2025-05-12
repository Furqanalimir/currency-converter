#!/bin/bash

# Base directory
BASE_DIR="perusha"

# Create directories
mkdir -p $BASE_DIR/apps/serviceA/{cmd,internal/{user/{handler,service,repository,model,usecase},product,order,middleware},api,config,tests}
mkdir -p $BASE_DIR/apps/serviceB/{cmd,internal,api,config,tests}
mkdir -p $BASE_DIR/commonLibs/{auth,database,utils,errors,logging,testing,validators}
mkdir -p $BASE_DIR/configs/{dev,staging,production,common}
mkdir -p $BASE_DIR/scripts/{build,deploy,ci,cleanup,setup}
mkdir -p $BASE_DIR/docs

# Create files
touch $BASE_DIR/apps/serviceA/{Dockerfile,README.md}
touch $BASE_DIR/apps/serviceB/{Dockerfile,README.md}
touch $BASE_DIR/commonLibs/README.md
