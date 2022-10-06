import asyncio
import pymongo
from random import randrange
from datetime import datetime, timedelta
from multiprocessing import Process
import logging
import os

root_start_date = datetime(1970, 8, 11, 0, 0, 0)
CONNECTION_STR = "mongodb://qualdoadmin:12345678@mongodb-service:27017"

# Max parallel process
MAX_PROCESS = 4

# List will be split into batches of given number. For asyncio processing
NUMBER_OF_BATCHES = 10

# Number of hour to add from given start time
NUMBER_OF_HOURS_TO_ADD = 24

# Do not increase the following constant to avoid overlap. 
# Have it always < 3600 since greater than 3600 will overlap with next datetime
NUMBER_OF_ENTRIES_PER_THREAD = 3500

initial_dates = [root_start_date]

LOG_PATH_CONST = "{}/log_for_{}.log"

# logging_location_parent = "/dbfs/log_check"
# logging_location_parent = "D:\Work\Codebase\check"
logging_location_parent = os.environ.get('HOME')


def get_log_dir_name():
    """_summary_

    Returns:
        _type_: _description_
    """
    logging_location = logging_location_parent + '/logs/'
    if not os.path.exists(logging_location):
        try:
            os.makedirs(logging_location)
        except Exception as e:
            print(e)
    return logging_location


async def add_entry_to_db(start_date, logger):
    """
    R
    """
    try:
        start_epoch = int(start_date.timestamp())
        random_value = randrange(10)
        random_value = start_epoch + random_value
        
        tenant_id = random_value % 15
        data_set_id = random_value % 10
        metric_id = random_value % 50
        for i in range(0, NUMBER_OF_ENTRIES_PER_THREAD):
            req_date_value = start_date + timedelta(seconds=i)
            epoch = start_epoch + i
            txt_name = f"Nandha_{epoch}"
            
            client = pymongo.MongoClient(CONNECTION_STR)
            qualdodb = client["qualdo"]
            sampleCol = qualdodb.get_collection("sample")

            metadata_id = epoch % 100
            
            if metadata_id in [2,4,6] or metadata_id > 50:
                metadata_id = None

            new_data = {
                "date": req_date_value,
                "id_dict": {"tenant_id": tenant_id, "data_set_id": data_set_id, 
                            "meta_data_id": metadata_id, "metric_id": metric_id, 
                            "symbol": "Custom"},
                "value": round((randrange(200) / 200), 3),
                "drift_value": round((randrange(400) / 400), 3),
                "epoch": epoch,
                "text": txt_name
            }

            cr = sampleCol.insert_one(new_data)
            logger.error(f"{start_date} - {i} - {datetime.utcnow()} - "
                        f"{cr.inserted_id} - {cr.acknowledged}")

    except Exception as e:
        logger.error(f"Exception {e}")

    return None


async def check_and_run_queries(dates, logger):
    """_summary_

    Args:
        dates (_type_): _description_
    """
    await asyncio.gather(*[add_entry_to_db(c_date, logger) for c_date in dates])


for idx in range(0, (NUMBER_OF_HOURS_TO_ADD - 1)):
    new_date = root_start_date + timedelta(hours=1)
    initial_dates.append(new_date)
    root_start_date = new_date


start_time2 = datetime.utcnow()
next_root_date = root_start_date + timedelta(hours=1)

date_list_initial = len(initial_dates)
print(f"Use this {next_root_date} as next start time\n\n")
TOTAL_PLANNED_ENTRIES = date_list_initial * NUMBER_OF_ENTRIES_PER_THREAD
print(f"Date list x Entries per date = {date_list_initial} x {NUMBER_OF_ENTRIES_PER_THREAD}\n")
print(f"Total planned entries: {TOTAL_PLANNED_ENTRIES}\n\n")

# print(f"Total date list: {initial_dates}\n\n")


# Split the long list into batches. Entries in each batch will be run
# inside asyncio process parallely
batches = [initial_dates[i:i + NUMBER_OF_BATCHES]
           for i in range(0, date_list_initial, NUMBER_OF_BATCHES)]

# Split them based on number of max multi process allowed.
# If max process is given as 4. At a time only 4 process will run parallely
# Remaining will run after previous 4 process gets completed
process_based_batches = [batches[i:i + MAX_PROCESS]
           for i in range(0, len(batches), MAX_PROCESS)]

# print(f"Process based Batches: {process_based_batches}\n\n")
print(f"Length of top level batch: {len(process_based_batches)}\n"
      f"Length of inner level batch: {len(batches)}\n")


def process_dates_in_a_process(date_list):
    p_id = os.getpid()
    log_base_path = get_log_dir_name()
    logger = logging.getLogger(f"log_for_{p_id}")
    _logfile = LOG_PATH_CONST.format(log_base_path, p_id)
    fh = logging.FileHandler(_logfile)
    fh.setLevel(logging.INFO)

    custom_format_str = "%(asctime)s - [%(filename)s - %(funcName)10s():%(lineno)s ] - %(levelname)s - %(message)s"
    formatter = logging.Formatter(custom_format_str, datefmt='%Y-%m-%d %H:%M:%S')

    fh.setFormatter(formatter)
    logger.addHandler(fh)
    asyncio.run(check_and_run_queries(date_list, logger))


if __name__ == '__main__':
    index = 0
    for small_batches in process_based_batches:
        process_list = []
        index = index + 1
        start_time_p = datetime.utcnow()
        for curr_dates in small_batches:
            p = Process(target=process_dates_in_a_process, args=(curr_dates,))
            p.start()
            process_list.append(p)

        for curr_process in process_list:
            curr_process.join()

        end_time_p = datetime.utcnow()
        time_taken_batch = (end_time_p - start_time_p).total_seconds()
        print(f"Time taken: {time_taken_batch} seconds. index: {index}")


    end_time2 = datetime.utcnow()
    time_taken2 = (end_time2 - start_time2).total_seconds()
    print(f"Time taken: {time_taken2} seconds")
    print(f"Total planned entries: {TOTAL_PLANNED_ENTRIES}")
