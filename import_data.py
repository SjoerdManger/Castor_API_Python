import re
import os
import csv
import json
import time
import datetime
import pandas as pd
import requests as req
from api_call import perform_api_call
from dotenv import load_dotenv, find_dotenv
from get_API_access_token import get_access_token


def import_data(castor_study_id, access_token, input_file_path, input_file, invalid_characters=""):

    # open session
    session = req.Session()

    # get field list from Castor EDC
    field_request = json.loads(perform_api_call("field", castor_study_id, access_token, "GET", session=session))
    page_count = field_request["page_count"]

    # GET a full list of fields in Castor EDC
    paged_field_list = []
    i = 1
    while i < page_count + 1:
        paged_field_request = json.loads(
            perform_api_call("field?page=" + str(i), castor_study_id, access_token, "GET", session=session))

        paged_field_list.append(paged_field_request["_embedded"]["fields"])
        i += 1

    # GET a full list of option groups in Castor EDC
    option_group_request = json.loads(
        perform_api_call("field?include=optiongroup", castor_study_id, access_token, "GET", session=session))
    page_count = option_group_request["page_count"]

    paged_option_group_list = []
    i = 1
    while i < page_count + 1:
        option_group_request = json.loads(
            perform_api_call("field?include=optiongroup&page=" + str(i), castor_study_id, access_token, "GET", session=session))

        paged_option_group_list.append(option_group_request["_embedded"]["fields"])
        i += 1

    new_paged_option_group_list = []
    for upper_list in paged_option_group_list:
        for sublist in range(len(upper_list)):
            new_paged_option_group_list.append(upper_list[sublist])

    # process field array such that you have an array of field_ids and an array of field_variable_names
    data = []
    for page in paged_field_list:
        for field in page:
            if field["field_type"] == "checkbox":
                field_id = field["id"]
                field_name = field["field_variable_name"]
                option_array = [opr["option_group"]["options"] for opr in new_paged_option_group_list if opr["id"] == field_id]
                for oa in option_array:
                    for option in oa:
                        # Use regular expression to check for special characters and replace them
                        special_chars = '\\/+,.-";:()[]{}!@#$%^&*|?<>'
                        option_group_name = re.sub(r'[{}]+'.format(re.escape(special_chars)), '', option["name"])
                        option_group_name = option_group_name.replace(' ', '_')
                        new_variable_name = field_name + "#" + option_group_name
                        data.append([field_id, new_variable_name, field["field_type"]])
            else:
                data.append([field["id"], field["field_variable_name"], field["field_type"]])

    df = pd.DataFrame(data, columns=["field_id", "field_name", "field_type"])
    list_field_id = df["field_id"].tolist()
    list_field_name = df["field_name"].tolist()
    list_field_type = df["field_type"].tolist()

    # match CSV header with field names from Castor EDC
    with open(input_file_path + input_file, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=';')
        csv_rows = [list(row.values()) for row in csv_reader]

    csv_headers = csv_reader.fieldnames
    field_name_to_index = {x: i for i, x in enumerate(list_field_name)}
    matched_field_name = [field_name_to_index.get(x, "null") for x in csv_headers]

    # to get the field ID corresponding with the field name, just use the same index but,
    # use it in the field id list instead
    # i.e. print(list_field_id[2]) and print(list_field_name[2]) are pointing to the same variable

    # get list of institutes
    institute_list = json.loads(perform_api_call("site", castor_study_id, access_token, "GET", session=session))
    institute_list = institute_list["_embedded"]["sites"]

    list_institute_abbreviation = []
    for x in institute_list:
        if x['deleted'] is not True:
            list_institute_abbreviation.append(x["abbreviation"])

    # get list of all records in Castor EDC
    records_list = json.loads(perform_api_call("participant?archived=0", castor_study_id, access_token, "GET", session=session))
    records_list = records_list["_embedded"]["participants"]

    list_record_id = []
    for x in records_list:
        list_record_id.append(x["participant_id"])

    # check for the column containing the record ID
    record_id_keys = {"PARTICIPANT_ID", "PARTICIPANT ID", '"PARTICIPANT ID"', '"PARTICIPANT_ID"'}
    matched_record_id_key = next((i for i, x in enumerate(csv_headers) if x.upper() in record_id_keys), False)

    # search for option group in Castor EDC
    option_groups = json.loads(perform_api_call("field-optiongroup", castor_study_id, access_token, "GET", session=session))
    option_groups = option_groups["_embedded"]["fieldOptionGroups"]

    options_list_ids = []
    options_list_options = []
    for x in option_groups:
        options_list_ids.append(x["id"])
        options_list_options.append(x["options"])

    first_value_per_option_group = []
    for y in options_list_options:
        for option in y:
            if option["groupOrder"] == 0:
                first_value_per_option_group.append(option["value"])
                break

    # make list of archived participant IDs
    archived_participant_list = json.loads(perform_api_call("participant?archived=1&page=1", castor_study_id, access_token, "GET", session=session))

    paged_archived_participant_list = []
    i = 1
    while i < archived_participant_list["page_count"]:
        paged_field_request = json.loads(perform_api_call("participant?archived=1&page=" + str(i), castor_study_id, access_token, "GET", session=session))

        paged_archived_participant_list.append(paged_field_request["_embedded"]["participants"])
        i += 1

    archived_participant_list = []
    for page in paged_archived_participant_list:
        for participant in page:
            archived_participant_list.append(participant["id"])

    # start importing rows from CSV to Castor EDC
    for row in csv_rows:

        # check whether access token is still valid/has not expired
        time_now = str(time.time())
        elapsed_time = float(time_now) - float(os.environ["TIME_INIT_ACCESS"])

        if elapsed_time >= 15000:
            load_dotenv(find_dotenv())
            client_id = os.getenv("CLIENT_ID")
            client_secret = os.getenv("CLIENT_SECRET")

            # get new access token
            access_token = get_access_token(client_id, client_secret)

            # store global
            time_now = str(time.time())
            os.environ["TIME_INIT_ACCESS"] = time_now

        # process option groups
        option_group_new_value = []
        previous_field_type = ""
        for x in options_list_ids:
            first_value = int(first_value_per_option_group[options_list_ids.index(x)])
            increment = 0
            new_value = ""
            for y in matched_field_name:  # where y is the index pointing to the corresponding column in "row"

                if y != "null":
                    # "new_value" concatenation is complete once a "new" field type is introduced,
                    # add new_value to the list
                    if previous_field_type == "checkbox" and list_field_type[y] != "checkbox":
                        option_group_new_value.append(new_value)

                    if list_field_type[y] == "checkbox":

                        # update new array such that the first 1 is stored as 1, the second 1 as 2 and so on ...
                        # assuming the first option group value starts at 1. Zeroes are ignored.
                        # e.g.list (                     will become: list (
                        #              [0] = > 1                                [0] = > 1
                        #              [1] = > 0
                        #              [2] = > 1                                [1] = > 3
                        #          )                                       )

                        # if case is for all subsequent values of the same option group
                        if increment != 0 and int(row[matched_field_name.index(y)]) != 0:
                            new_value = new_value + ";" + str(first_value + increment)
                        elif new_value == '':  # else case is for first value in option group only
                            new_value = str(first_value)

                        increment += 1

                    previous_field_type = list_field_type[y]

        # update new values for option groups
        option_id = 0
        for y in matched_field_name:  # where y is the index pointing to the corresponding column in "row"

            if y != "null":
                # index of "option_group_new_value" must be incremented once a "new" field type is introduced
                if previous_field_type == "checkbox" and list_field_type[y] != "checkbox":
                    option_id += 1

                if list_field_type[y] == "checkbox":
                    row[matched_field_name.index(y)] = option_group_new_value[option_id]

        # match institute from current row with corresponding institute id from Castor EDC
        matched_institutes = list(set(list_institute_abbreviation) & set(row))

        institute_id = False
        for institute in institute_list:
            if institute["abbreviation"] in matched_institutes:
                institute_id = institute["site_id"]

        # store record ID in variables
        record_id = row[matched_record_id_key]
        record_id_raw = record_id

        # replace any invalid characters with "%" + the hexadecimal value of the invalid char
        test_string = bytes(record_id, "utf-8")
        for c in invalid_characters:
            if c in test_string:
                replacement = "%" + hex(c).split('x')[-1].upper()
                record_id = record_id.replace(chr(c), replacement)

        # check whether record_id exists in Castor EDC
        record_exists = False

        if record_id_raw != "":
            # check if current record already exists in Castor EDC
            record_exists = json.loads(perform_api_call("participant/" + record_id, castor_study_id, access_token, "GET", session=session))

            archived_record_to_find = "ARCHIVED-" + record_id_raw + "-[0-9].*]"
            r = re.compile(archived_record_to_find)
            found_in_list = list(filter(r.match, archived_participant_list))

            # update to boolean value
            if record_exists["status"] != 404:
                record_exists = True
            elif record_exists["status"] != "open" and len(found_in_list) != 0:
                print(record_id_raw, " is ARCHIVED, skip this row")
                continue
            else:
                record_exists = False

        # create new record if it doesn't exist by incrementing the previous numeric part by 1
        if not record_exists:
            # only when the record_id is not found as a column in the header, and is therefore set
            if record_id_raw == "":

                # get the latest record ID from Castor
                now = str(datetime.datetime.now())
                latest_record = max([x for x in records_list if x["created_on"]["date"] < now],
                                    key=lambda x: x["created_on"]["date"])
                latest_record_id = latest_record["record_id"]

                # find position of first digit
                digit_position = re.search(r'\d+', latest_record_id)
                digit_position_start = digit_position.start()
                first_non_zero = re.search(r'[1-9]', latest_record_id).start()

                new_record_id = 0
                if digit_position:
                    record_prefix = latest_record_id[0: digit_position_start]
                    record_id_sub = int(latest_record_id[digit_position_start:])
                    record_id_sub += 1

                    # pad string with zero's if there were any
                    if first_non_zero > digit_position_start:
                        amount_of_zeros = first_non_zero - digit_position_start
                        new_record_id = record_prefix + str(record_id_sub).zfill(amount_of_zeros + 1)
                    else:
                        new_record_id = record_prefix + str(record_id_sub)

                # update record_id so that the new ID is used
                record_id = new_record_id

            # create new record ID
            post_fields = {
                "participant_id": str(record_id_raw),
                "site_id": str(institute_id)
            }

            # create new record ID
            created_record_id = json.loads(perform_api_call("participant", castor_study_id, access_token, "POST", post_fields, session=session))

            # error handling record ID creation
            if created_record_id["status"] in [402, 404, 422]:
                print("Creation of record ID: " + record_id + " failed: " + created_record_id["detail"])
                break

        # loop through columns in row and update field values in Castor EDC
        json_data = []
        number_of_json_objects = 0
        field_types = {"date": "date", "datetime": "datetime", "numeric": "numeric"}

        for value, field_name, field_id in zip(row, matched_field_name, list_field_id):
            if value != "" and field_name != "null":
                datapoint_value = value
                datapoint_id = list_field_id[field_name]
                datapoint_type = field_types.get(field_name)

                if datapoint_type in ["date", "datetime"]:
                    datapoint_value = datetime.datetime.strptime(datapoint_value, "%d-%m-%Y").strftime("%d-%m-%Y")
                elif datapoint_type == "numeric":
                    datapoint_value = datapoint_value.replace(',', '.')

                json_data.append({'field_id': datapoint_id, 'field_value': datapoint_value})
                number_of_json_objects += 1

        append_to_json = {'common': {'change_reason': 'initial load', 'confirmed_changes': 'true'}, 'data': json_data}

        # check if there's any data to be imported, if not, continue
        if number_of_json_objects == 0:
            continue

        # POST data to Castor EDC
        category = "participant/" + record_id + "/data-points/study"
        result = json.loads(perform_api_call(category, castor_study_id, access_token, "POST", append_to_json, session=session))

        # extract the first element
        first_key = next(iter(result))
        print("Record ID:", record_id_raw, " processed with status: ", first_key)

    # close session
    session.close()
