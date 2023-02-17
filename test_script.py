# test script:
# Does the import CSV match the final result in Castor EDC?

# requirements:
import re
import io
import os
import json
import time
import pandas as pd  # version 1.5.3
from import_data import import_data
from api_call import perform_api_call
from dotenv import load_dotenv, find_dotenv
from get_API_access_token import get_access_token


def non_match_elements(list_a, list_b):
    non_match_list = []
    for list_item in list_a:
        if list_item not in list_b:
            non_match_list.append(list_item)
    return non_match_list


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

# process option group column names
option_groups = json.loads(perform_api_call("field?page=1&include=optiongroup", study_id, api_token, "GET"))
page_count = option_groups["page_count"]
paged_option_groups_list = []
i = 1
while i < page_count + 1:
    paged_option_group_request = json.loads(
        perform_api_call("field?include=optiongroup&page=" + str(i), study_id, api_token, "GET"))

    paged_option_groups_list.append(paged_option_group_request["_embedded"]["fields"])
    i += 1

option_groups = []
option_groups_variable_name = []
for page in paged_option_groups_list:
    for field in page:
        if field['option_group'] is not None and field['option_group'] not in option_groups and field['field_type'] == "checkbox":
            option_groups.append(field['option_group'])
            option_groups_variable_name.append(field['field_variable_name'])

ls_dict = []
special_chars = '\\/+,.-";:()[]{}!@#$%^&*|?<>'
my_dict = {}
for option in range(len(option_groups)):
    # make a new dict for each option
    my_dict = {"ID": option_groups[option]['id'], "Field Name": option_groups_variable_name[option], "Option Name": []}

    # store X amount of option names from available options
    for dict_item in option_groups[option]['options']:
        # check for special chars and replace them
        option_group_name = re.sub(r'[{}]+'.format(re.escape(special_chars)), '', dict_item['name'])
        option_group_name = option_group_name.replace(' ', '_')
        my_dict["Option Name"].append(option_groups_variable_name[option] + "#" + option_group_name)

    # append dict to list of dicts
    ls_dict.append(my_dict)

# loop through the list and add the new columns to df_merge
item_to_pop = []
for item in ls_dict:
    if item["Field Name"] in df_merged.columns:
        for option in item["Option Name"]:
            if item["Field Name"] not in df_merged.columns:  # this condition is only true after the "original" column has already been removed
                df_merged[option] = item_to_pop  # add new column to existing dataframe
            else:
                item_to_pop = df_merged[item["Field Name"]]  # store item to be removed in variable, in order to use for rest of variables in same option group
                df_merged[option] = df_merged.pop(item["Field Name"])  # remove column from dataframe

# right now all option groups store the same information, for example:
# current situation:             but it needs to be changed to:
# "field1#option1" = "1;3"       "field1#option1" = "1"
# "field1#option2" = "1;3"       "field1#option2" = ""
# "field1#option3" = "1;3"       "field1#option3" = "1"
counter = 1
counter_dict = {}
option_cols = [col for col in df_merged.columns if '#' in col]
if option_cols:
    current_prefix = option_cols[0].split("#")[0]
    for col in option_cols:
        prefix = col.split("#")[0]
        if prefix != current_prefix:
            current_prefix = prefix
            counter = 1
        counter_dict[col] = counter
        counter += 1

# split option group values in the different columns
for option, value in counter_dict.items():
    df_merged[option] = df_merged[option].apply(lambda x: 1 if str(value) in x.split(";") else "")

# process grid column names
grid_ls_dict = []
my_dict = {}
for page in paged_option_groups_list:
    for field in page:
        if field['field_type'] == 'grid' and field['field_summary_template'] not in grid_ls_dict:
            my_dict = {"Field Name": field['field_variable_name'], "Grid": field['field_summary_template']}
            grid_ls_dict.append(my_dict)

grid_json_options = []
for option in range(len(grid_ls_dict)):
    grid_json_options.append(json.loads(grid_ls_dict[option]["Grid"]))

grid_row_name_list = []
grid_column_name_list = []
grid_column_name = ""
grid_row_name = ""
for option in range(len(grid_json_options)):
    for row in grid_json_options[option]["rowNames"]:
        # check for special chars and replace them
        grid_row_name = re.sub(r'[{}]+'.format(re.escape(special_chars)), '', row)
        grid_row_name = grid_row_name.replace(' ', '_')
        grid_variable_name = grid_ls_dict[option]["Field Name"]
        grid_row_name_list.append(grid_variable_name + "_" + grid_row_name)
    for column in grid_json_options[option]["columnNames"]:
        # check for special chars and replace them
        grid_column_name = re.sub(r'[{}]+'.format(re.escape(special_chars)), '', column)
        grid_column_name = grid_column_name.replace(' ', '_')
        grid_column_name_list.append(grid_column_name)

# combine row names with column names:
grid_ls_dict_new_names = []
for i in grid_row_name_list:
    for j in grid_column_name_list:
        my_dict = {"Field Name": i.split('_Row')[0], "New Name": i + "_" + str(j)}
        grid_ls_dict_new_names.append(my_dict)

# loop through the list and add the new columns to df_merge
for item in grid_ls_dict:
    if item["Field Name"] in df_merged.columns:
        for field in grid_ls_dict_new_names:
            grid_field_name = field["Field Name"]
            grid_new_field_name = field["New Name"]
            if grid_field_name not in df_merged.columns:  # this condition is only true after the "original" column has already been removed
                df_merged[grid_new_field_name] = item_to_pop  # add new column to existing dataframe
            else:
                item_to_pop = df_merged[grid_field_name]  # store item to be removed in variable, in order to use for rest of variables in same option group ("if" statement)
                df_merged[grid_new_field_name] = df_merged.pop(grid_field_name)  # remove column from dataframe

# right now all grid groups store the same information, for example:
# '{"0":{"0":"row 1 column 1","1":"row 1 column 2"},"1":{"0":"row 2 column 1","1":"row 2 column 2"}}'
# update the columns such that the row/column specific values are stored
field_name = ""
previous_field_name = ""
incrementer = 0
for index, row in df_merged.iterrows():
    for grid_field in grid_ls_dict_new_names:
        field_name = grid_field["Field Name"]
        new_name = grid_field["New Name"]

        # get the digit after "Row", e.g. extract "1" from "Row_1_Column_2"
        row_column_name = new_name.split(field_name)[1]  # remove field_name prefix
        row_name = re.split("_Column_", row_column_name)[0]  # split by "_Column_" and retrieve everything before split
        first_digit_row_number = re.search(r"\d", row_name).start()  # search for first digit
        row_name_number = row_name[first_digit_row_number:]

        # get the digit after "Column", e.g. extract "2" from "Row_1_Column_2"
        column_name_number = re.split("_Column_", row_column_name)[1]  # split by "_Column_" and retrieve everything after split

        # update grid values in dataframe
        if new_name in df_merged.columns and row[new_name] != "":
            column_to_adjust = json.loads(df_merged[new_name][0])
            new_value = column_to_adjust[str(int(row_name_number) - 1)][str(int(column_name_number) - 1)]  # -1 because it's zero-based
            df_merged.at[index, new_name] = new_value

# process the "number & date" field
my_dict = {}
number_date_ls_dict = []
for page in paged_option_groups_list:
    for field in page:
        if field['field_type'] == 'numberdate' and field['field_id'] not in number_date_ls_dict:
            my_dict = {"Field Name": field['field_variable_name'], "New Name Date": field['field_variable_name'] + "_date", "New Name Number": field['field_variable_name'] + "_number"}
            number_date_ls_dict.append(my_dict)

# update number date columns in dataframe
new_names_ls_dict = []
for field in number_date_ls_dict:
    if field["Field Name"] in df_merged.columns:
        item_to_pop = df_merged[field["Field Name"]]
        df_merged.pop(field["Field Name"])
        df_merged[field["New Name Date"]] = item_to_pop  # store the values in one column, will be split in two next

# split option group values in the different columns
for field in number_date_ls_dict:
    df_merged[[field["New Name Number"], field["New Name Date"]]] = df_merged[field["New Name Date"]].str.split(';', expand=True)

# -- -- 4. COMPARE export with import -- --
df_merged.to_csv(full_export_file_path, sep=";")
df_import.to_csv(full_import_file_path, sep=";")

# prepare test results file
f = open(test_result_file_path, "w")
f.write("---- TEST RESULTS ----")
f.write("\n")
f.close()

# compare on simple count of rows
len_import = len(df_import)
len_export = len(df_merged)
perform_data_value_check = True

continue_compare = 0
f = open(test_result_file_path, "a")
if len_import != len_export:
    f.writelines(["Error: Data set size doesn't match: import size: ", str(len_import), ", export size: ", str(len_export)])
    f.write("\n")
    continue_compare = int(input("Rows do not match, continue and delete unmatched rows?? 1/0 (Yes/No) "))

    # remove non-matching rows from either dataframe, if "continue compare" = 1
    if continue_compare == 1:
        row_not_found = set(df_merged.index).symmetric_difference(df_import.index)
        if row_not_found.issubset(set(df_import.index)):
            df_import = df_import.drop(row_not_found, axis=0)
            f.writelines(["Continue compare, removed: ", str(row_not_found), ", from import file"])
            f.write("\n")
        elif row_not_found.issubset(set(df_merged.index)):
            df_merged = df_merged.drop(row_not_found, axis=0)
            f.writelines(["Continue compare, removed: ", str(row_not_found), ", from export file"])
            f.write("\n")
    else:
        f.write("End of script: Cannot continue comparison")
        perform_data_value_check = False
else:
    f.write("Count of rows successful: Data set sizes match!")
f.write("\n")
f.close()

# compare on values
continue_compare = 0
if perform_data_value_check:
    if len(df_merged.columns) != len(df_import):
        f = open(test_result_file_path, "a")
        f.write("Columns do not match: ")
        continue_compare = int(input("Columns do not match, continue and delete unmatched columns?? 1/0 (Yes/No) "))

    # remove non-matching columns from both dataframes, if "continue compare" = 1
    if continue_compare == 1:
        f = open(test_result_file_path, "a")
        f.write("Continue compare by deleting non-matched columns")
        f.write("\n")

        # drop non matches from df_merged and sort columns/rows
        non_match = non_match_elements(df_merged.columns, df_import.columns)
        string_non_match = ', '.join([str(item) for item in non_match])
        df_merged = df_merged.drop(non_match, axis=1, errors='ignore')
        df_merged = df_merged.sort_index()  # sort rows
        df_merged = df_merged.sort_index(axis=1)  # sort columns

        if len(string_non_match) != 0:
            f.writelines(["The following (export) columns where not found in the import: ", string_non_match])
            f.write("\n")

        # drop non matches from df_import and sort columns/rows
        non_match = non_match_elements(df_import.columns, df_merged.columns)
        string_non_match = ', '.join([str(item) for item in non_match])
        df_import = df_import.drop(non_match, axis=1, errors='ignore')
        df_import = df_import.sort_index()  # sort rows
        df_import = df_import.sort_index(axis=1)  # sort columns

        if len(string_non_match) != 0:
            f.writelines(["The following (import) columns where not found in the export file: ", string_non_match])
            f.write("\n")

        # actual check for differences
        diff = df_import.compare(df_merged, result_names=("import", "export"))
        diff.to_csv(comparison_file_path, sep=";")
        f.write("Check on differences made, see: " + test_result_file_path)
        f.write("\n")
        f.close()
    else:
        f.write("End of comparison")
        f.write("\n")
        f.close()

print(" --- DONE --- ")
print('Check "test_results.txt" for a test result summary, ' + test_result_file_path)

# *I chose for a structure export instead of the "fields" end-point, because it's easier to filter on the type of form the fields are used in
