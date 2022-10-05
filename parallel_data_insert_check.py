import asyncio
import pymongo
from random import randrange
from datetime import datetime, timedelta
from multiprocessing import Process
import logging
import os

root_start_date = datetime(1970, 1, 3, 0, 0, 0)
CONNECTION_STR = "mongodb://qualdoadmin:12345678@mongodb-service:27017"


# How many elements each list should have
NUMBER_OF_BATCHES = 3
NUMBER_OF_START_DATES = 5
NUMBER_OF_ENTRIES_PER_THREAD = 2

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


for idx in range(0, (NUMBER_OF_START_DATES - 1)):
    new_date = root_start_date + timedelta(hours=1)
    initial_dates.append(new_date)
    root_start_date = new_date


start_time2 = datetime.utcnow()
next_root_date = root_start_date + timedelta(hours=1)
print(f"Use this {next_root_date} as next start time\n\n")
TOTAL_PLANNED_ENTRIES = len(initial_dates) * NUMBER_OF_ENTRIES_PER_THREAD
print(f"Total date list: {initial_dates}\n\n")
print(f"Total planned entries: {TOTAL_PLANNED_ENTRIES}\n\n")


# using list comprehension
batches = [initial_dates[i:i + NUMBER_OF_BATCHES]
           for i in range(0, len(initial_dates), NUMBER_OF_BATCHES)]
print(f"Batches: {batches}\n\n")


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
    process_list = []
    for curr_dates in batches:
        p = Process(target=process_dates_in_a_process, args=(curr_dates,))
        p.start()
        process_list.append(p)


    for curr_process in process_list:
        curr_process.join()


    end_time2 = datetime.utcnow()
    time_taken2 = (end_time2 - start_time2).total_seconds()
    print(f"Time taken: {time_taken2} seconds")
    print(f"Total planned entries: {TOTAL_PLANNED_ENTRIES}")
