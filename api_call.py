import os
import time
import json
import requests as req


def perform_api_call(category, study_id, access_token, get_or_post, post_fields="null", return_value="null", session=None):
    url = "https://data.castoredc.com/api/study/" + study_id + "/" + category

    if return_value == "export_data" or return_value == "export_structure":
        headers = {
            'accept': 'text/csv',
            "Authorization": "Bearer " + access_token
        }
    else:
        headers = {
            "accept": "application/hal+json",
            "Authorization": "Bearer " + access_token
        }

    # needed for the return values for option groups
    is_option_group_call = False

    # check if elapsed time is bigger than 9 minutes or amount of calls is approximating 600 calls
    number_of_calls = os.environ["NUMBER_OF_CALLS"]
    time_init_access = os.environ["TIME_INIT_ACCESS"]

    if number_of_calls != '':
        number_of_calls = int(number_of_calls) + 1
    else:
        number_of_calls = 1
    current_time = time.time()

    if current_time - float(time_init_access) > 540 or number_of_calls > 595:
        # sleep for the remainder of the 10 minutes.
        remainder = 600 - (current_time - float(time_init_access))
        print("sleeping for ", remainder, " seconds")
        time.sleep(remainder)

        # reset variables
        os.environ["TIME_INIT_ACCESS"] = str(time.time())
        number_of_calls = 0

    os.environ["NUMBER_OF_CALLS"] = str(number_of_calls)

    if session:
        if get_or_post == "GET":
            response = session.get(url, headers=headers)
        elif get_or_post == "POST":
            response = session.post(url, headers=headers, json=post_fields)
    else:
        if get_or_post == "GET":
            response = req.get(url, headers=headers)
        elif get_or_post == "POST":
            response = req.post(url, headers=headers, json=post_fields)

    if response.status_code in [200, 201]:
        # return value
        if return_value == "export_data" or return_value == "export_structure":
            returned_value = response.text
        else:
            returned_value = response.content
    else:
        returned_value = response.text

    return returned_value
