#!/bin/bash

. ./.pws

export ECHOCONFIG=1
export DEBUGLEVEL=0
# 0 Info
# 1 Error
# 2 Debug
export ECHORECORDS=0
export FLUSHCAP=2000
export RECCAP=999999999999999999
#export RECCAP=999

#export SOURCEPATHS=/Users/george/Desktop/bsa-dev/python/fs_aric_aws/AsyncOut/year=2024/month=08/day=01/hour=05
#export SOURCEPATHS=/Users/george/Desktop/bsa-dev/python/fs_aric_aws/AsyncOut/year=2024/month=08/day=01
export SOURCEPATHS=/Users/george/Desktop/bsa-dev/python/fs_aric_aws/AsyncOut/year=2024/month=08

export DEST=1
# IF 0 then no DB send
# IF 1 then MongoDB

# MongoDB
export MONGO_ROOT=mongodb
#export MONGO_USERNAME=
#export MONGO_PASSWORD=  
export MONGO_HOST=localhost
export MONGO_PORT=27017
export MONGO_DIRECT=directConnection=true 
export MONGO_DATASTORE=FiMongoDb         
export MONGO_COLLECTION=AsyncOut
export MONGO_BATCH_SIZE=1000  


python3 main.py
