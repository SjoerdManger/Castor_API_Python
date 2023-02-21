import re
import json


def process_grid_fields(paged_option_groups_list, df, special_chars):
    grid_ls_dict = []
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
        if item["Field Name"] in df.columns:
            for field in grid_ls_dict_new_names:
                grid_field_name = field["Field Name"]
                grid_new_field_name = field["New Name"]
                if grid_field_name not in df.columns:  # this condition is only true after the "original" column has already been removed
                    df[grid_new_field_name] = item_to_pop  # add new column to existing dataframe
                else:
                    item_to_pop = df[grid_field_name]  # store item to be removed in variable, in order to use for rest of variables in same option group ("if" statement)
                    df[grid_new_field_name] = df.pop(grid_field_name)  # remove column from dataframe

    # right now all grid groups store the same information, for example:
    # '{"0":{"0":"row 1 column 1","1":"row 1 column 2"},"1":{"0":"row 2 column 1","1":"row 2 column 2"}}'
    # update the columns such that the row/column specific values are stored
    field_name = ""
    previous_field_name = ""
    incrementer = 0
    for index, row in df.iterrows():
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
            if new_name in df.columns and row[new_name] != "":
                column_to_adjust = json.loads(df[new_name][0])
                new_value = column_to_adjust[str(int(row_name_number) - 1)][str(int(column_name_number) - 1)]  # -1 because it's zero-based
                df.at[index, new_name] = new_value

    return df
