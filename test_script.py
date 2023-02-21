# test script:
# Does the import CSV match the final result in Castor EDC?

# requirements:
import io
import os
import json
import time
import pandas as pd  # version 1.5.3
from import_data import import_data
from api_call import perform_api_call
from dotenv import load_dotenv, find_dotenv
from get_API_access_token import get_access_token
from test_script_compare_rows import compare_rows
from test_script_compare_values import compare_values
from test_script_grid_fields import process_grid_fields
from test_script_option_groups import process_option_groups
from test_script_number_date_fields import process_number_date

# get privacy sensitive parameter values
load_dotenv(find_dotenv())
client_id = os.getenv("CLIENT_ID")  # Castor EDC Client ID as found in your database settings in Castor
client_secret = os.getenv("CLIENT_SECRET")  # Castor EDC Client Secret as found in your database settings in Castor
import_file_path = os.getenv("IMPORT_FILE_PATH")  # path without filename + extension
import_file_name = os.getenv("IMPORT_FILE_NAME")  # filename + extension
full_import_file_path = os.getenv("FULL_IMPORT_FILE_PATH")  # full path with filename + extension
full_export_file_path = os.getenv("FULL_EXPORT_FILE_PATH")  # full path with filename + extension
test_result_file_path = os.getenv("TEST_RESULT_FILE_PATH")  # full path with filename + extension
comparison_file_path = os.getenv("COMPARISON_FILE_PATH")  # full path with filename + extension
study_id = os.getenv("CASTOR_EDC_STUDY_ID")  # study ID as found in your database settings in Castor EDC
api_token = get_access_token(client_id, client_secret)


# store access_token initialization time in environment variable
now = str(time.time())
os.environ["TIME_INIT_ACCESS"] = now

# initialize one more environment variable
os.environ["NUMBER_OF_CALLS"] = ""

# basic parameter set
invalid_characters = bytes('\\/+,.-";:()[]{}!@#$%^&*|?<>', 'utf-8')
special_chars = '\\/+,.-";:()[]{}!@#$%^&*|?<>'

# -- -- 1. IMPORT data set -- --
perform_import = int(input("Perform import? 1/0 (Yes/No) "))
df_import = pd.read_csv(import_file_path + import_file_name, sep=";")
df_import = df_import.rename(columns={"Participant Id": "Participant ID"})
df_import = df_import.set_index("Participant ID", drop=True)
df_import = df_import.drop(['Participant Status', 'Participant Creation Date'], axis=1, errors='ignore')

if perform_import == 1:
    # import to Castor EDC
    import_data(study_id, api_token, import_file_path, import_file_name, invalid_characters)

# -- -- 2. EXPORT data set that was just imported -- --
# GET a full list of participants in Castor EDC
participant_list = json.loads(perform_api_call("participant?archived=0", study_id, api_token, "GET"))
page_count = participant_list["page_count"]
paged_participant_list = []
i = 1
while i < page_count + 1:
    paged_field_request = json.loads(
        perform_api_call("participant?archived=0&page=" + str(i), study_id, api_token, "GET"))

    paged_participant_list.append(paged_field_request["_embedded"]["participants"])
    i += 1

# get dataframe of participants + their abbreviation
participant_data = []
site_abbreviation = []
for page in paged_participant_list:
    for participant in page:
        site_abbreviation.append(participant["_embedded"]["site"]["abbreviation"])
        participant_data.append(participant["id"])

df_participant = pd.DataFrame({"Participant ID": participant_data, "Site Abbreviation": site_abbreviation})

# GET export from Castor EDC
api_url = "export/data?exclude_empty_surveys=true&exclude_empty_reports=true"
exported_data = perform_api_call(api_url, study_id, api_token, "GET", "", "export_data")

# process result such that archived records are filtered out
df_export = pd.read_csv(io.StringIO(exported_data), sep=";")
df_export = df_export[~df_export["Record ID"].str.contains("ARCHIVED", na=False)]
df_export = df_export[df_export["Form Type"].str.contains("Study", na=False)]
df_export.reset_index(drop=True, inplace=True)
df_export = df_export[["Record ID", "Field ID", "Value"]]
df_export = df_export.rename(columns={"Record ID": "Participant ID"})

# export the study structure to retrieve the field names, see notes at bottom*
exported_structure = perform_api_call("export/structure", study_id, api_token, "GET", "", "export_structure")
df_export_structure = pd.read_csv(io.StringIO(exported_structure), sep=";")
df_export_structure = df_export_structure[df_export_structure["Form Type"].str.contains("Study", na=False)]
df_export_structure = df_export_structure[["Field ID", "Field Variable Name"]]

# merge the three dataframes to match Field ID, Field Value, Field Variable Name and Site Abbreviation
df_merged = pd.merge(df_export_structure, df_export, on="Field ID", how='left')
df_merged = df_merged.pivot_table(values="Value", index="Participant ID", columns="Field Variable Name", aggfunc="sum", fill_value="#FILL_VALUE#", dropna=False)
df_merged = pd.merge(df_merged, df_participant, on="Participant ID", how='left')
df_merged = df_merged.replace("#FILL_VALUE#", "")
df_merged.set_index("Participant ID", inplace=True)

# -- -- 2a. process option group column names -- --
option_groups = json.loads(perform_api_call("field?page=1&include=optiongroup", study_id, api_token, "GET"))
page_count = option_groups["page_count"]
paged_option_groups_list = []
i = 1
while i < page_count + 1:
    paged_option_group_request = json.loads(
        perform_api_call("field?include=optiongroup&page=" + str(i), study_id, api_token, "GET"))

    paged_option_groups_list.append(paged_option_group_request["_embedded"]["fields"])
    i += 1

df_merged = process_option_groups(paged_option_groups_list, df_merged, special_chars)

# -- -- 2b. process grid column names -- --
df_merged = process_grid_fields(paged_option_groups_list, df_merged, special_chars)

# -- -- 2c. process the "number & date" field -- --
df_merged = process_number_date(paged_option_groups_list, df_merged)

# -- -- 3. COMPARE export with import -- --
df_merged.to_csv(full_export_file_path, sep=";")
df_import.to_csv(full_import_file_path, sep=";")

# prepare test results file
f = open(test_result_file_path, "w")
f.write("---- TEST RESULTS ----")
f.write("\n")
f.close()

# -- -- 3a. compare count of rows -- --
df_import, df_merged, perform_data_value_check = compare_rows(df_import, df_merged, test_result_file_path)

# -- -- 3b. compare on values -- --
if perform_data_value_check:
    compare_values(df_import, df_merged, test_result_file_path, comparison_file_path)

print(" --- DONE --- ")
print('Check "test_results.txt" for a test result summary, ' + test_result_file_path)

# *I chose for a structure export instead of the "fields" end-point, because it's easier to filter on the type of form the fields are used in
