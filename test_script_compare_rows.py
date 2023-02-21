

def compare_rows(df_import, df_export, test_result_file_path):
    len_import = len(df_import)
    len_export = len(df_export)
    perform_data_value_check = True

    f = open(test_result_file_path, "a")
    if len_import != len_export:
        f.writelines(
            ["Error: Data set size doesn't match: import size: ", str(len_import), ", export size: ", str(len_export)])
        f.write("\n")
        continue_compare = int(input("Rows do not match, continue and delete unmatched rows?? 1/0 (Yes/No) "))

        # remove non-matching rows from either dataframe, if "continue compare" = 1
        if continue_compare == 1:
            row_not_found = set(df_export.index).symmetric_difference(df_import.index)
            if row_not_found.issubset(set(df_import.index)):
                df_import = df_import.drop(row_not_found, axis=0)
                f.writelines(["Continue compare, removed: ", str(row_not_found), ", from import file"])
                f.write("\n")
            elif row_not_found.issubset(set(df_export.index)):
                df_export = df_export.drop(row_not_found, axis=0, self=True)
                f.writelines(["Continue compare, removed: ", str(row_not_found), ", from export file"])
                f.write("\n")
        else:
            f.write("End of script: Cannot continue comparison")
            perform_data_value_check = False
    else:
        f.write("Count of rows successful: Data set sizes match!")
    f.write("\n")
    f.close()

    return df_import, df_export, perform_data_value_check
