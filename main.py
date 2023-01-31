# main script
import os
import time
from import_data import import_data
from dotenv import load_dotenv, find_dotenv
from get_API_access_token import get_access_token

# get start time for calculating elapsed time
start_time = time.time()

# get access_token
load_dotenv(find_dotenv())
client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")
api_token = get_access_token(client_id, client_secret)

# store access_token initialization time in environment variable
now = str(time.time())
os.environ["TIME_INIT_ACCESS"] = now

# initialize one more environment variable
os.environ["NUMBER_OF_CALLS"] = ""

# collect some info from user
# TODO: ask for the kind of request, import report, import CRF data or request survey status? (based on import_data.php)
# TODO: set path to server location
# TODO: get Castor Study ID from CSV file, RDP export?

input_file_path = "C:\\Users\\s.manger\\PycharmProjects\\CastorAPI\\input_files\\"
input_file = "Test_Study_-_Castor_API_export_20230111.csv"
study_id = "A7875A16-E56C-4F0B-BB92-5B18B4C72C46"
invalid_characters = bytes('\\/+,.-";:()[]{}!@#$%^&*|?<>', 'utf-8')

# call main function
import_data(study_id, api_token, input_file_path, input_file, invalid_characters)

# check elapsed time (in seconds)
end_time = time.time()
print("Total time elapsed (s): ", end_time - start_time)
