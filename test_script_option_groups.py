import re


def process_option_groups(paged_option_groups_list, df, special_chars):
    option_groups = []
    option_groups_variable_name = []
    for page in paged_option_groups_list:
        for field in page:
            if field['option_group'] is not None and field['option_group'] not in option_groups and field['field_type'] == "checkbox":
                option_groups.append(field['option_group'])
                option_groups_variable_name.append(field['field_variable_name'])

    ls_dict = []
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
        if item["Field Name"] in df.columns:
            for option in item["Option Name"]:
                if item["Field Name"] not in df.columns:  # this condition is only true after the "original" column has already been removed
                    df[option] = item_to_pop  # add new column to existing dataframe
                else:
                    item_to_pop = df[item["Field Name"]]  # store item to be removed in variable, in order to use for rest of variables in same option group
                    df[option] = df.pop(item["Field Name"])  # remove column from dataframe

    # right now all option groups store the same information, for example:
    # current situation:             but it needs to be changed to:
    # "field1#option1" = "1;3"       "field1#option1" = "1"
    # "field1#option2" = "1;3"       "field1#option2" = ""
    # "field1#option3" = "1;3"       "field1#option3" = "1"
    counter = 1
    counter_dict = {}
    option_cols = [col for col in df.columns if '#' in col]
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
        df[option] = df[option].apply(lambda x: 1 if str(value) in x.split(";") else "")

    return df