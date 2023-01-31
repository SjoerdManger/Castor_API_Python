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
input_file_path = "[USER_PATH]"
input_file = "[IMPORT_FILE].csv"
study_id = "[STUDY_ID]"
invalid_characters = bytes('\\/+,.-";:()[]{}!@#$%^&*|?<>', 'utf-8')

# call main function
import_data(study_id, api_token, input_file_path, input_file, invalid_characters)

# check elapsed time (in seconds)
end_time = time.time()
print("Total time elapsed (s): ", end_time - start_time)
