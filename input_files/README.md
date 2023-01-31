# README
This is a folder used to place your input files.
This tool requires the user to provide a CSV-file with ";" as a separator. 
There are other prerequisites for this CSV-file: 

1. Participant ID setting must be set to "Patient Study ID (free text)". You can adjust this by going to Settings -> Study -> Other -> Participant IDs       
2. When importing Reports, a separate column with Record ID needs to be included and sorted on this column.
3. Dates must be formatted in DD-MM-YYYY
4. In order for Option Groups to work properly, the Option Group name must match the option group as exported from Castor EDC. 
   E.g. an option group called "Gender" with option values "0: Female" and "1: Male", should be noted as the column names "Gender#Female" and "Gender#Male" respectively.
5. For checkboxes (Option Group) the absence of a value should be denoted as "" or " ". I.e. "0" counts as a valid value.

You can also review the [Castor Helpdesk](https://helpdesk.castoredc.com/export-and-import-data/import-study-data) for more general info about importing study data 
