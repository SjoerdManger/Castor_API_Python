

def process_number_date(paged_option_groups_list, df):

    number_date_ls_dict = []
    for page in paged_option_groups_list:
        for field in page:
            if field['field_type'] == 'numberdate' and field['field_id'] not in number_date_ls_dict:
                my_dict = {"Field Name": field['field_variable_name'], "New Name Date": field['field_variable_name'] + "_date", "New Name Number": field['field_variable_name'] + "_number"}
                number_date_ls_dict.append(my_dict)

    # update number date columns in dataframe
    new_names_ls_dict = []
    for field in number_date_ls_dict:
        if field["Field Name"] in df.columns:
            item_to_pop = df[field["Field Name"]]
            df.pop(field["Field Name"])
            df[field["New Name Date"]] = item_to_pop  # store the values in one column, will be split in two next

    # split option group values in the different columns
    for field in number_date_ls_dict:
        df[[field["New Name Number"], field["New Name Date"]]] = df[field["New Name Date"]].str.split(';', expand=True)

    return df
