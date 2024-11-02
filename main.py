# main file to read csv_file and import other files 
import pandas as pd
import csv
import normalizer
import re

# Reading the input csv file and the fds text file
try:
    table = pd.read_csv('referenceInputTable.csv')
    print(f"Provided sample input Table:\n{table}\n")
except FileNotFoundError:
    print("Error: 'referenceInputTable.csv' not found.")
    exit(1)
# Enter FDs as input by reading file
try:
    with open('fds.txt', 'r') as file:
        lines = [line.strip() for line in file]
except FileNotFoundError:
    print("Error: 'fds.txt' not found.")
    exit(1)
fds = {}
for line in lines:
    determinant, dependent = line.split(" -> ")
    # Splitting the determinant by comma to make it a list
    determinant = determinant.split(", ")
    fds[tuple(determinant)] = dependent.split(", ")
print(f"fds=\n{fds}\n")

# Helper function to check if any value in the series contains a comma
def contains_comma(series):
    return any(series.str.contains(','))

# Helper function  to parse the main data table for comma-separated values (input parser)
def inputparser(table):
    table = table.astype(str)  # Ensure all data is treated as strings
    
    # Use filter to find columns that contain commas
    columns_with_commas = list(filter(lambda col: contains_comma(table[col]), table.columns))
    
    # Use map to apply splitting and stripping on each column with commas
    for col in columns_with_commas:
        table[col] = table[col].apply(lambda x: list(map(str.strip, x.split(','))) if isinstance(x, str) else x)

    return table


# Enter mvds as input 
mvds = {}
while True:
    inp = input("Please enter Multi-Valued Dependencies using format X ->> A, B or X ->> Y. After done, type exit or finish: ")
    if inp.lower() in ["exit", "finish"]:
        break
    try:
        # Split the input into determinant and dependent parts
        determinant, dependent = inp.split(" ->> ")
        # Clean up the inputs by stripping extra spaces
        determinants_list = [d.strip() for d in determinant.split(',')]
        dependents_list = [d.strip() for d in dependent.split(',')]
        # Store in the dictionary
        mvds.setdefault(tuple(determinants_list), []).extend(dependents_list)  # Use setdefault for cleaner logic
    except ValueError:
        print("Invalid input format, enter input format as X ->> A, B or X ->> A.")

# Print user-entered MVDs in the specified format
print('\nmvds = {')
for determinant, dependents in mvds.items():
    dependents_str = ', '.join(f"'{dep}'" for dep in set(dependents))  # Use set to avoid duplicates
    determinant_str = ', '.join(f"'{d}'" for d in determinant)  # Create a string for determinants
    print(f"    ({determinant_str}): [{dependents_str}],")
print('}')

# Choose any normal form as input to normalize the input table if not in the provided user normal form
step = int(input("\nSelect the highest normal form that the table can achieve (1: 1NF, 2: 2NF, 3: 3NF, 4: BCNF, 5: 4NF, 6: 5NF): "))
current = int(input("\nDo you want to know the highest normal form of the input table? (1: Yes, 2: No): "))
highest_normal_form = 0  # Variable to keep track of the highest normal form achieved

# Enter primary or composite keys as input
pk = input("\nEnter primary keys (for composite keys, separate them with commas): ").split(', ')
# Convert list to tuple
pk = tuple(pk)

# To generate output schema with proper constraints, first let's determine SQL datatype
def determine_sql_datatype(dtype):
    # Determine the SQL data type based on the pandas data type.
    if pd.api.types.is_integer_dtype(dtype):
        return "INT"
    elif pd.api.types.is_float_dtype(dtype):
        return "FLOAT"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "DATETIME"
    elif pd.api.types.is_string_dtype(dtype):
        max_length = dtype.str.len().max()  # Get the maximum length of the string values
        return f"VARCHAR({max_length})" if max_length is not None else "VARCHAR(255)"
    else:
        # Default to VARCHAR(255) for non-numeric or unknown types
        return "VARCHAR(255)"
    
# To generate output queries for 1NF   
def generate_1nf_table_sql(pk, dataframe):
    # Assume `pk` is the primary key column; if not provided, take the first column as primary key.
    pk = list(dataframe.keys())[0]
    
    # Create table name by joining primary key column names with an underscore
    table_name = "_".join(pk)
    
    # Extract only the columns related to primary keys from dataframe
    dataframe = dataframe[pk]
    
    # Start forming the CREATE TABLE SQL query
    create_query = f"CREATE TABLE {table_name} (\n"
    
    # Add columns to the query, specifying the primary key(s) and appropriate SQL data types
    for column in dataframe.columns:
        dtype = determine_sql_datatype(dataframe[column])  # Use the dynamic determination function
        if column in pk:
            # Mark column as NOT NULL and PRIMARY KEY
            create_query += f"  {column} {dtype} NOT NULL PRIMARY KEY,\n"
        else:
            create_query += f"  {column} {dtype},\n"
    
    # Remove last comma and close the table statement
    create_query = create_query.rstrip(',\n') + "\n);"
    print(create_query)  # Output the generated SQL query for debugging or review

# Function to create SQL tables for each normalized relation based on given relations and functional dependencies
def create_tables_for_normalized_relations(relations, fds):
    for rel_name, relation in relations.items():
        # Assume `rel_name` contains the primary key(s) and convert to tuple if it's a single string
        pks = rel_name
        pks = (pks,) if isinstance(pks, str) else pks
        
        # Construct table name from primary keys
        table_name = "_".join(pks)
        
        # Begin the CREATE TABLE statement for the relation
        create_query = f"CREATE TABLE {table_name} (\n"
        
        # Loop through columns in the relation and add each to the CREATE TABLE statement
        for column in relation.columns:
            # Pass the actual column to determine_sql_datatype
            create_query += f"  {column} {determine_sql_datatype(relation[column])}"
            if column in pks:  # Add NOT NULL and PRIMARY KEY constraint if column is a primary key
                create_query += " NOT NULL PRIMARY KEY"
            create_query += ",\n"
        
        # Track foreign keys already added to avoid duplicates
        foreign_keys_added = set()
        
        # Loop through functional dependencies to create foreign key constraints where applicable
        for (determinants, dependents) in fds.items():
            for dep in dependents:
                # Only add foreign key if column is part of current relation and not a primary key
                if dep in relation.columns and dep not in pks:
                    # Search for the source table containing the foreign key column
                    for source_table, source_columns in relations.items():
                        if dep in source_columns.columns and source_table != rel_name:
                            # Add the foreign key if it has not already been added
                            if (dep, source_table) not in foreign_keys_added:
                                # Format source table name if its a tuple
                                source_table_name = "_".join(source_table) if isinstance(source_table, tuple) else source_table
                                create_query += f"  FOREIGN KEY ({dep}) REFERENCES {source_table_name}({dep}),\n"
                                foreign_keys_added.add((dep, source_table))
        
        # Remove trailing comma, newline, and finalize the CREATE TABLE statement
        create_query = create_query.rstrip(',\n') + "\n);"
        
        # Output the SQL query for the generated table (useful for debugging or review)
        print(create_query)

table = inputparser(table)

# Normalize to 1NF
if step >= 1:
    result = normalizer.transform_to_1NF(table, pk)  # Call the normalization function
    normalized_table_1 = result[0]  # Get the normalized table
    onenfcheck = result[1]  # Get the 1NF check result

    if onenfcheck:
        print("Given input table is already in 1NF.\n")
        highest_normal_form = max(highest_normal_form, 1)  # Update highest normal form

    if step == 1:
        print("Generating output queries for 1NF-->\n")
        generate_1nf_table_sql(pk, normalized_table_1)


# Normalize to 2NF
if step >= 2:
    # Normalize to 2NF
    normalized_result = normalizer.transform_to_2NF(normalized_table_1, pk, fds)
    normalized_table_2, twonfcheck = normalized_result

    if twonfcheck:
        print("Given input table is already in 2NF.\n")
        highest_normal_form = max(highest_normal_form, 2)  # Update highest normal form

    if step == 2:
        print("Generating output queries for 2NF-->\n")
        create_tables_for_normalized_relations(normalized_table_2, fds)


# Normalize to 3NF
if step >= 3:
    # Normalize to 3NF
    normalized_result = normalizer.transform_to_3NF(normalized_table_2, fds)
    normalized_table_3, threenfcheck = normalized_result

    if threenfcheck:
        print("Given input table is already in 3NF.\n")
        highest_normal_form = max(highest_normal_form, 3)  # Update highest normal form

    if step == 3:
        print("Generating output queries for 3NF-->\n")
        create_tables_for_normalized_relations(normalized_table_3, fds)


# Normalize to BCNF
if step >= 4:
    # Normalize to BCNF
    normalized_result_bcnf = normalizer.transform_to_BCNF(normalized_table_3, pk, fds)
    normalized_table_bcnf, bcnfcheck = normalized_result_bcnf

    if bcnfcheck:
        print("Given input table is already in BCNF.\n")
        highest_normal_form = max(highest_normal_form, 4)  # Update highest normal form

    if step == 4:
        print("Generating output queries for BCNF-->\n")
        create_tables_for_normalized_relations(normalized_table_bcnf, fds)
   
# Normalize to 4NF
if step >= 5:
    normalized_result = normalizer.transform_to_4NF(normalized_table_bcnf, mvds)
    normalized_table_4nf, fournfcheck = normalized_result

    if fournfcheck:
        print("Given input table is already in 4NF.\n")
        highest_normal_form = max(highest_normal_form, 5)  # Update highest normal form

    if step == 5:
        print("Generating output queries for 4NF-->\n")
        create_tables_for_normalized_relations(normalized_table_4nf, fds)

# Normalize to 5NF
if step >= 6:
    normalized_result = normalizer.transform_to_5NF(normalized_table_4nf, pk, fds)
    normalized_table_5nf, fivenfcheck = normalized_result

    if fivenfcheck:
        print("Given input table is already in 5NF.\n")
        highest_normal_form = max(highest_normal_form, 6)  # Update highest normal form

    if step == 6:
        print("Generating output queries for 5NF-->\n")
        create_tables_for_normalized_relations(normalized_table_5nf, fds)


# Output the highest normal form achieved if the user requested it
if current == 1:
    if highest_normal_form == 0:
        print("The input table is still not normalized into any specific normal form.")
    else:
        normal_forms = {
            1: "1NF",
            2: "2NF",
            3: "3NF",
            4: "BCNF",
            5: "4NF",
            6: "5NF"
        }
        print(f"The highest normal form achieved is: {normal_forms[highest_normal_form]}")

print("Normalization process completed.")
