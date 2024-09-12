#######################################################################################################################
#
#
#  	Project     	: 	TFM 2.0, *.gz loader -> Post to MongoDB Collections
#
#   File            :   main.py
#
#   Description     :   Consume the Outbound ASyncOut *.gz files from FeatureSpace and post to MongoDB Collections
#
#	By              :   Fraud Intelligence - BankservAfrica
#
#   Created     	:   10 Sept 2024
#
#   Changelog       :   See bottom
#
#   starting  S3    :   https://www.youtube.com/watch?v=sFvYUE8M7zY
#   JSON Viewer     :   https://jsonviewer.stack.hu
#
#   Mongodb         :   https://www.mongodb.com/cloud/atlas      
#                   :   https://hub.docker.com/r/mongodb/mongodb-atlas-local
#
#
########################################################################################################################

__author__ = "Fraud"
__email__ = "-"
__version__ = "0.0.1"
__copyright__ = "Copyright 2024, -"


import json
from datetime import datetime
from time import perf_counter
import os
import logging
import gzip
import io
import bson

try:
    import pymongo
    print("Module Import Successful")
except ImportError as error:
    print("Module Import Error")
    print(error)

config_params = {}
# General
config_params["ECHOCONFIG"] = int(os.environ["ECHOCONFIG"])
config_params["DEBUGLEVEL"] = int(os.environ["DEBUGLEVEL"])
config_params["ECHORECORDS"] = int(os.environ["ECHORECORDS"])
config_params["FLUSHCAP"] = int(os.environ["FLUSHCAP"])
config_params["RECCAP"] = int(os.environ["RECCAP"])
config_params["SOURCEPATHS"] = os.environ["SOURCEPATHS"].split(",")
config_params["DEST"] = int(os.environ["DEST"])

# Mongo
config_params["MONGO_HOST"] = os.environ["MONGO_HOST"]
config_params["MONGO_PORT"] = os.environ["MONGO_PORT"]
config_params["MONGO_DIRECT"] = os.environ["MONGO_DIRECT"]
config_params["MONGO_ROOT"] = os.environ["MONGO_ROOT"]
config_params["MONGO_USERNAME"] = os.environ["MONGO_USERNAME"]
config_params["MONGO_PASSWORD"] = os.environ["MONGO_PASSWORD"]
config_params["MONGO_DATASTORE"] = os.environ["MONGO_DATASTORE"]
config_params["MONGO_COLLECTION"] = os.environ["MONGO_COLLECTION"]
config_params["MONGO_BATCH_SIZE"] = int(os.environ["MONGO_BATCH_SIZE"])
   

# Logging Handler
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# create console handler with a higher log level
ch = logging.StreamHandler()
fh = logging.FileHandler('loader.log')

if config_params["DEBUGLEVEL"] == 0:
    ch.setLevel(logging.INFO)
    fh.setLevel(logging.INFO)
elif config_params["DEBUGLEVEL"] == 1:
    ch.setLevel(logging.ERROR)
    fh.setLevel(logging.ERROR)
elif config_params["DEBUGLEVEL"] > 1:
    ch.setLevel(logging.DEBUG)
    fh.setLevel(logging.DEBUG)
    
ch.setFormatter(formatter)
logger.addHandler(ch)

fh.setFormatter(formatter)
logger.addHandler(fh)


# Initialize DocumentDB client outside of the handler
mongodbclient = None

def pp_json(json_thing, sort=True, indents=4):
    if type(json_thing) is str:
        logger.info(json.dumps(json.loads(json_thing), sort_keys=sort, indent=indents))
    else:
        logger.info(json.dumps(json_thing, sort_keys=sort, indent=indents))
    return None

# end pp_json

def connect_to_mongodb():
    global mongodbclient
    if mongodbclient is None:
        try:
            if config_params["MONGO_USERNAME"] != "": 
               config_params["MONGO_URI"] = f'{config_params["MONGO_ROOT"]}://{config_params["MONGO_USERNAME"]}:{config_params["MONGO_PASSWORD"]}@{config_params["MONGO_HOST"]}:{int(config_params["MONGO_PORT"])}/?{config_params["MONGO_DIRECT"]}'
            
            else:
               config_params["MONGO_URI"] = f'{config_params["MONGO_ROOT"]}://{config_params["MONGO_HOST"]}:{int(config_params["MONGO_PORT"])}/?{config_params["MONGO_DIRECT"]}'
                           
            logger.info('Mongo Connection, URI: {uri} '.format(
                uri=config_params["MONGO_URI"]
            ))

            try:
                myclient = pymongo.MongoClient(config_params["MONGO_URI"])
                myclient.server_info() # force connection on a request as the
                                    # connect=True parameter of MongoClient seems
                                    # to be useless here 
                                    
            except pymongo.errors.ServerSelectionTimeoutError as err:
                logger.error('Mongo Connection FAILED: {err} '.format(
                    err=err
                ))
                os._exit(1)
                
            mydb            = myclient[config_params["MONGO_DATASTORE"]]
            my_collection   = mydb[config_params["MONGO_COLLECTION"]]

            logger.info("")

        except Exception as e:
            logger.error('Failed to create Mongo connection {err}'.format(
                err=e
            ))                               

            return None
    
        if config_params["DEBUGLEVEL"] == 0:
            logger.info('Mongo Connection, Established'.format(
            ))
            
        elif config_params["DEBUGLEVEL"] == 1 :
            logger.error('Mongo Connection, Established {ServerInfo}'.format(
                ServerInfo=myclient.server_info()
            ))
            
        elif config_params["DEBUGLEVEL"] > 1:
            logger.debug('Mongo Connection, Established {ServerInfo}'.format(
                ServerInfo=myclient.server_info()
            ))
        
        logger.info("")

    return my_collection

# end connect_to_mongodb


def fileProcessor(fileName, mongodb_collection):
    
    logger.info("")
    logger.info("  File                      :{file}".format(
        file=fileName
    ))
    
    step0starttime = datetime.now()
    step0start = perf_counter()
    
    with gzip.open(fileName,'rt') as fh:

        flushcap = config_params["FLUSHCAP"]
        flush = 0
        reccap = config_params["RECCAP"]
        recs = 0

        if flushcap > 1:
            mydocs = []
            
        for line in fh:
                                    
            json_msg = {}
            json_msg["fs_payload"] = json.loads(line)
            json_msg["json_file"] = fileName
            json_msg["line"] = flush+1

            msg_txt = json.dumps(json_msg)
            
            
            if flushcap == 1:
                # Post to MongoDB
                if config_params["DEST"] == 1:
                    x = mongodb_collection.insert_one(json_msg)
                                                            
            if config_params["ECHORECORDS"] == 1:
                pp_json(msg_txt, sort=True, indents=4)
                
            if config_params["ECHORECORDS"] == 1:
                if config_params["DEBUGLEVEL"] == 1:
                    logger.error("  msg_txt: {msg_txt}".format(
                        msg_txt=msg_txt
                    ))
                elif config_params["DEBUGLEVEL"] > 1:
                    logger.debug("  msg_txt: {msg_txt}".format(
                        msg_txt=msg_txt
                    ))

            if flushcap > 1:
                flush +=1 
                # Add Record to set to post
                mydocs.append(json_msg)
                if flush==flushcap:
                    # Post to MongoDB
                    if config_params["DEST"] == 1 or config_params["DEST"] == 3:
                        result = mongodb_collection.insert_many(mydocs)
                    
                    # Post to DocumentDB
                    if config_params["DEST"] == 2 or config_params["DEST"] == 3:
                        pass
                        # Multi Line

                    # Reset Flush Counter
                    if config_params["DEBUGLEVEL"] > 0:
                        logger.info("  Flushed    @ {flush}".format(flush=flush))

                    flush = 0
                    mydocs = []
                    

            recs += 1
            if recs == reccap:
                logger.info("    RecCap break          @ :{reccap}".format(reccap=recs))
                break 

        # end for
    # end Width
                
    # Current File completed 
    step0endtime = datetime.now()
    step0end = perf_counter()
    step0time = step0end - step0start
    rate = str( round(recs/step0time,2))
    
    logger.info("    Process Stats         St:{start},  Et:{end},   Rt:{runtime}, Recs:{recs} docs, Rate:{rate} docs/s, File:{file}".format(
        start=str(step0starttime.strftime("%Y-%m-%d %H:%M:%S.%f")),
        end=str(step0endtime.strftime("%Y-%m-%d %H:%M:%S.%f")),
        runtime=str(step0time),
        recs=recs,
        rate=rate,
        file=fileName
        ))
    
    return recs

#end fileProcessor

def hourProcess(hour_dir, mongodb_collection):

    step0starttime = datetime.now()
    step0start = perf_counter()
    
    recs = 0
    logger.info("  Hour Dir                  :{hour_dir}".format(
        hour_dir=hour_dir))
    
    files = []    
        
    logger.debug("  Recs for hour             :{rec} docs".format(rec=recs))
    
    for file in sorted(os.listdir(hour_dir)):   
        if file.endswith(".gz"):
            files.append(hour_dir + "/" + file)
            
    for file in files:
        fileRecs = fileProcessor(file, mongodb_collection)
        
        recs += fileRecs 
        logger.debug("  totalRecs hour            :{rec} docs".format(rec=recs))

    step0endtime = datetime.now()
    step0end = perf_counter()
    step0time = step0end - step0start  
    rate = str( round(recs/step0time,2))
    
    logger.info("")
    logger.info("  Hour Process Stats     St:{start},  Et:{end},   Rt:{runtime}, Recs:{recs} docs, Rate:{rate} docs/s".format(
        start=str(step0starttime.strftime("%Y-%m-%d %H:%M:%S.%f")),
        end=str(step0endtime.strftime("%Y-%m-%d %H:%M:%S.%f")),
        runtime=str(step0time),
        recs=recs,
        rate=rate
        ))
    
    logger.info("")
    
    return recs

#End dayProcessor

def dayProcess(day_dir, mongodb_collection):
   
    step0starttime = datetime.now()
    step0start = perf_counter()

    recs = 0

    logger.info("  Day Dir                   :{day_dir}".format(
        day_dir=day_dir))
        
    hours = sorted(os.listdir(day_dir))
    for hour in hours:
        if hour.startswith("hour="):
            fileRecs = hourProcess(day_dir + "/" + hour, mongodb_collection)

            recs += fileRecs    
            logger.debug("  totalRecs hour            :{rec} docs".format(rec=recs))
    
    step0endtime = datetime.now()
    step0end = perf_counter()
    step0time = step0end - step0start     
    rate = str( round(recs/step0time,2))

    logger.info("  Day Process Stats       St:{start},  Et:{end},   Rt:{runtime}, Recs:{recs} docs, Rate:{rate} docs/s".format(
        start=str(step0starttime.strftime("%Y-%m-%d %H:%M:%S.%f")),
        end=str(step0endtime.strftime("%Y-%m-%d %H:%M:%S.%f")),
        runtime=str(step0time),
        recs=recs,
        rate=rate
        ))
    
    logger.info("")
    
    return recs

#End dayProcessor

def monthProcess(month_dir, mongodb_collection):
    
    # Print the Numbers
    step0starttime = datetime.now()
    step0start = perf_counter()
        
    recs = 0
    
    logger.info("  Month Dir                :{month_dir}".format(
        month_dir=month_dir))
        
    days = sorted(os.listdir( month_dir ))
    for day in days:
        if day.startswith("day="):
            fileRecs = dayProcess(month_dir + "/" + day, mongodb_collection)

            recs += fileRecs    
            logger.debug("  totalRecs day              :{rec} docs".format(rec=recs))

    step0endtime = datetime.now()
    step0end = perf_counter()
    step0time = step0end - step0start     
    rate = str( round(recs/step0time,2))

    logger.info("  Month Process Stats St:{start},  Et:{end},   Rt:{runtime}, Recs:{recs} docs, Rate:{rate} docs/s".format(
        start=str(step0starttime.strftime("%Y-%m-%d %H:%M:%S.%f")),
        end=str(step0endtime.strftime("%Y-%m-%d %H:%M:%S.%f")),
        runtime=str(step0time),
        recs=recs,
        rate=rate
        ))
    
    logger.info("")
    
    return recs

#End dayProcessor

def run_loader():

    step0starttime = datetime.now()
    step0start = perf_counter()
    
    logger.info("***********************************************************")
    logger.info("* ")
    logger.info("* Python FeatureSpace ASyncOut Event Loader")
    logger.info("* ")
    logger.info("***********************************************************")
    logger.info("* General")
    logger.info("* EchoConfig                :"+ str(config_params["ECHOCONFIG"]))
    logger.info("* DebugLevel                :"+ str(config_params["DEBUGLEVEL"]))
    logger.info("* FlushCap                  :"+ str(config_params["FLUSHCAP"]))
    logger.info("* RecCap                    :"+ str(config_params["RECCAP"]))
    logger.info("* SourcePath                :"+ str(config_params["SOURCEPATHS"]))
    
    logger.info("***********************************************************")    
    logger.info("* MongoDB")
    logger.info("* MongoDB Root              :" + config_params["MONGO_ROOT"])
    logger.info("* MongoDB Username          :" + config_params["MONGO_USERNAME"])
    logger.info("* MongoDB Host              :" + config_params["MONGO_HOST"])
    logger.info("* MongoDB Port              :" + config_params["MONGO_PORT"])
    logger.info("* MongoDB Direct Connect    :" + config_params["MONGO_DIRECT"])
    logger.info("* MongoDB Datastore         :" + config_params["MONGO_DATASTORE"])
    logger.info("* MongoDB Collection        :" + config_params["MONGO_COLLECTION"])
    logger.info("* MongoDB Batch Size        :" + str(config_params["MONGO_BATCH_SIZE"]))
    
    logger.info("***********************************************************")     


    # Step 1 - Aquire DB Connections
    if config_params["DEST"] == 1:
        step1starttime = datetime.now()
        step1start = perf_counter()
        
        mongodb_collection = connect_to_mongodb()
        
        step1endtime = datetime.now()
        step1end = perf_counter()
        step1time = step1end - step1start     

    
    # Lets Go
    try:
        step2starttime = datetime.now()
        step2start = perf_counter()
        
        # Figure out where we are starting from.
        SourceIs = config_params["SOURCEPATHS"][0].split("/")
        LastIs = SourceIs[len(SourceIs)-1]
        LastTypeIs = LastIs.split("=")
        LastTag = LastTypeIs[0]
        
        if LastTag == "hour":
            hour = (config_params["SOURCEPATHS"])[0]
            logger.info("Load started from Hour :{hour}".format(hour=hour))
            
            hourProcess(hour, mongodb_collection)
            
        elif LastTag == "day":     
            day = (config_params["SOURCEPATHS"])[0]
            logger.info("Load started from Day  :{day}".format(day=day))
            
            dayProcess(day, mongodb_collection)

        elif LastTag == "month":
            month = (config_params["SOURCEPATHS"])[0]
            logger.info("Load started from Month:{month}".format(month=month))
            
            monthProcess(month, mongodb_collection)
            
        else:
            logger.info("Invalid Source point :{source}".format(
                source=config_params["SOURCEPATHS"][0]
            ))
            os._exit(0)      
  

        # All files complete
        step2endtime = datetime.now()
        step2end = perf_counter()
        step2time = step2end - step2start
        
    except Exception as e:
        logger.error("Generic error :{error}".format(error=e))
        raise e
    
    finally:
        # Print the Numbers
        step0endtime = datetime.now()
        step0end = perf_counter()
        step0time = step0end - step0start
        if config_params["DEST"] > 0:
            logger.info("DBConnection, St:{start} Et:{end} Rt:{runtime}".format(
                start=str(step1starttime.strftime("%Y-%m-%d %H:%M:%S.%f")),
                end=str(step1endtime.strftime("%Y-%m-%d %H:%M:%S.%f")),
                runtime=str(step1time)))
            
        logger.info("All Files, St:{start} Et:{end} Rt:{runtime}".format(
            start=str(step2starttime.strftime("%Y-%m-%d %H:%M:%S.%f")),
            end=str(step2endtime.strftime("%Y-%m-%d %H:%M:%S.%f")),
            runtime=str(step2time)))
            
        logger.info("End to End, St:{start} Et:{end} Rt:{runtime}".format(
            start=str(step0starttime.strftime("%Y-%m-%d %H:%M:%S.%f")),
            end=str(step0endtime.strftime("%Y-%m-%d %H:%M:%S.%f")),
            runtime=str(step0time)))
    
        
        fh.close()
        logger.debug("File handle closed")

    pass

#end main


# For local testing
if __name__ == '__main__':

    run_loader()
    
#end main