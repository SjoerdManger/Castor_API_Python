

def non_match_elements(list_a, list_b):
    non_match_list = []
    for list_item in list_a:
        if list_item not in list_b:
            non_match_list.append(list_item)
    return non_match_list


def compare_values(df_import, df_export, test_result_file_path, comparison_file_path):

    continue_compare = 0
    if len(df_export.columns) != len(df_import):
        f = open(test_result_file_path, "a")
        f.write("Columns do not match: ")
        continue_compare = int(input("Columns do not match, continue and delete unmatched columns?? 1/0 (Yes/No) "))

    # remove non-matching columns from both dataframes, if "continue compare" = 1
    if continue_compare == 1:
        f = open(test_result_file_path, "a")
        f.write("Continue compare by deleting non-matched columns")
        f.write("\n")

        # drop non matches from df_merged and sort columns/rows
        non_match = non_match_elements(df_export.columns, df_import.columns)
        string_non_match = ', '.join([str(item) for item in non_match])
        df_merged = df_export.drop(non_match, axis=1, errors='ignore')
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
