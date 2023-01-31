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
        new_list = []
        if return_value == "get_fields" or return_value == "get_field_option_group":
            # initialize page count, arrays and counter
            json_response = json.loads(response.text)
            page_count = json_response["page_count"]
            new_list.append(json_response)  # append first page

            if page_count > 1:
                for i in range(page_count - 1):
                    if return_value == "get_field_option_group":
                        is_option_group_call = True
                        api_url = "field?page=" + str(i + 2) + "&include=optiongroup"
                        new_list.append(json.loads(perform_api_call(api_url, study_id, access_token, "GET")))
                    else:
                        api_url = "field?page=" + str(
                            i + 2)  # append first page, followed by the second on the next line
                        new_list.append(json_response)  # append first page, followed by the second on the next line
                        new_list.append(json.loads(perform_api_call(api_url, study_id, access_token, "GET")))

        # return value
        if return_value == "export_data" or return_value == "export_structure":
            returned_value = response.text
        elif return_value == "get_fields" or return_value == "get_option_groups" or is_option_group_call:
            returned_value = new_list
        else:
            returned_value = response.content
    else:
        returned_value = response.text

    return returned_value
