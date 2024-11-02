import pandas as pd  # Import the Pandas library for data manipulation and analysis, using the alias 'pd'
from itertools import combinations  # Import the 'combinations' function from 'itertools' for generating combinations of elements from an iterable
import re  # Import the 're' module for working with regular expressions, useful for string searching and manipulation


############################1NF##########################################
def validate_1NF(relation):
    # Determines if a given DataFrame meets the requirements for First Normal Form (1NF).
    # Args:
    #     relation (DataFrame): The DataFrame representing the relation to validate.
    # Returns:
    #     bool: True if the relation is in 1NF, False otherwise.

    # Check if the DataFrame is empty
    if relation.empty:
        return False
    
    # Ensure each column has a single, consistent data type and no nested structures.
    for col in relation.columns:
        # If a column contains more than one unique type, it violates 1NF
        if relation[col].map(type).nunique() > 1:
            return False
        
        # Check if any item in the column is a nested structure (list, dict, or set)
        if relation[col].apply(lambda item: isinstance(item, (list, dict, set))).any():
            return False
            
    return True  # Return True if all checks pass, indicating the relation is in 1NF

def is_nested_collection(value):
    # Checks if a value is a collection type (list or set).
    # Args:
    #     value: The value to check.
    # Returns:
    #     bool: True if value is a list or set, False otherwise.
    
    return isinstance(value, (list, set))  # Return True if the value is of type list or set

def transform_to_1NF(relation, pk):
    # Transforms a relation to comply with First Normal Form (1NF) by flattening any nested structures.
    # Args:
    #     relation (DataFrame): The DataFrame representing the relation to normalize.
    #     pk: The primary key identifier for the relation.
    # Returns:
    #     tuple: A dictionary with the normalized relation and a boolean indicating if normalization was required.
    
    normalized_relations = {}  # Dictionary to hold normalized relations
    is_already_1NF = validate_1NF(relation)  # Check if the relation is already in 1NF

    if is_already_1NF:
        # If the relation is already in 1NF, store it and return
        normalized_relations[pk] = relation
        return normalized_relations, is_already_1NF
    else:
        # If the relation is not in 1NF, process each column to flatten nested structures
        for col in relation.columns:
            # Check if any item in the column is a nested collection
            if relation[col].apply(is_nested_collection).any():
                # Flatten the nested structures in the column using explode
                relation = relation.explode(col)  # Explode the column to separate nested elements into individual rows

        # Display the transformed relation
        print(f"The relation after transforming into 1NF:\n{relation}\n")
        normalized_relations[pk] = relation  # Store the normalized relation
        return normalized_relations, is_already_1NF  # Return the normalized relations and 1NF status

#########################2NF##############
def validate_2NF(pk, fds, rel):
    # Collect attributes that are not part of the primary key
    non_primary_attrs = [col for col in rel.columns if col not in pk]

    # Validate the relation against 2NF rules
    for lhs, rhs in fds.items():  # LHS: determinant, RHS: dependents
        # Check if the left-hand side is a proper subset of the primary key
        if set(lhs).issubset(pk) and set(lhs) != set(pk):
            # If any of the right-hand side attributes are non-primary,
            # it violates 2NF, so return False
            if any(attr in non_primary_attrs for attr in rhs):
                return False

    return True  # Return True if the relation satisfies 2NF rules

def transform_to_2NF(rel, pk, fds):
    # Focus on the primary key in the relation
    rel = rel[pk]  # Select only the primary key columns
    normalized_relations = {}  # Dictionary to hold normalized relations
    attributes_to_remove = []  # List to track attributes to remove from the original relation
    is_2NF = validate_2NF(pk, fds, rel)  # Validate if the relation is in 2NF

    if is_2NF:
        # If the relation is already in 2NF, store it and return
        normalized_relations[pk] = rel
        return normalized_relations, is_2NF
    else:
        print("Resulting relation after conversion to 2NF:\n")
        # Collect non-primary attributes for further processing
        non_primary_attrs = [col for col in rel.columns if col not in pk]

        # Iterate over each functional dependency
        for lhs, rhs in fds.items():  # LHS: determinant, RHS: dependents
            # Check if the LHS is a proper subset of the primary key
            if set(lhs).issubset(pk) and set(lhs) != set(pk):
                # If any attribute on the RHS is a non-primary attribute
                if any(attr in rhs for attr in non_primary_attrs):
                    # Create a new relation with the determinant and dependents
                    new_rel = rel[list(lhs) + rhs].drop_duplicates()  # Get unique rows
                    normalized_relations[tuple(lhs)] = new_rel  # Store the new relation

                    # Track attributes on the RHS that are not part of the LHS
                    for attr in rhs:
                        if attr not in lhs and attr not in attributes_to_remove:
                            attributes_to_remove.append(attr)  # Mark for removal

        # Remove the non-key attributes from the original relation
        rel.drop(columns=attributes_to_remove, inplace=True)
        normalized_relations[pk] = rel  # Store the modified original relation

        # Display the normalized relations
        for rel_key in normalized_relations:
            print(normalized_relations[rel_key])  # Print each normalized relation
            print('\n')

        return normalized_relations, is_2NF  # Return the normalized relations and 2NF status
############3NF#############
def has_partial_dependencies(relations_dict, fds):
    # Identify the primary key by extracting keys from functional dependencies
    pk = set(fds.keys())
    # Identify non-key attributes by collecting attributes from right-hand sides of functional dependencies
    non_key_attributes = {attr for attrs in fds.values() for attr in attrs}
    
    # Iterate over each relation in the relations dictionary
    for rel_name, rel in relations_dict.items():
        # Check each functional dependency
        for lhs, rhs in fds.items():
            # Check if the lhs is part of the relation, not a subset of the primary key,
            # and the rhs is a subset of non-key attributes
            if (set(lhs).issubset(rel.columns) and 
                not set(lhs).issubset(pk) and 
                set(rhs).issubset(non_key_attributes)):
                return True  # Return True if a partial dependency is found
    return False  # Return False if no partial dependencies exist

def validate_3NF(relations_dict, fds):
    # Validate if the relations are in 3NF by checking for partial dependencies
    return not has_partial_dependencies(relations_dict, fds)

def create_new_relations(relations_dict, lhs, rhs, rel):
    # Create new relations based on the left-hand side (lhs) and right-hand side (rhs) of a functional dependency
    combined_columns = list(set(lhs).union(rhs))  # Combine lhs and rhs attributes
    new_table1_cols = list(lhs) + rhs  # Columns for the first new table
    new_table2_cols = list(set(rel.columns) - set(rhs))  # Columns for the second new table
    
    # Create new tables with unique values and reset their index
    table1 = rel[new_table1_cols].drop_duplicates().reset_index(drop=True)
    table2 = rel[new_table2_cols].drop_duplicates().reset_index(drop=True)
    
    return table1, table2  # Return the two newly created tables

def transform_to_3NF(relations_dict, fds):
    modified_relations = {}  # Dictionary to hold modified relations
    # Check if relations are already in 3NF
    if validate_3NF(relations_dict, fds):
        return relations_dict, True  # Return current relations if they are valid in 3NF

    print("Relations after transforming into 3NF:\n")
    # Iterate through each relation to transform it into 3NF
    for rel_name, rel in relations_dict.items():
        for lhs, rhs in fds.items():
            # Check if lhs is part of the relation and rhs is not a subset of lhs
            if (set(lhs).issubset(rel.columns) and 
                not set(rhs).issubset(lhs)):
                # Create new relations based on the current functional dependency
                table1, table2 = create_new_relations(relations_dict, lhs, rhs, rel)
                
                modified_relations[tuple(lhs)] = table1  # Store the first new table
                modified_relations[rel_name] = table2  # Store the second modified table
                break  # Exit the inner loop after transforming one dependency
        else:
            modified_relations[rel_name] = rel  # Keep the original relation if no transformation was made

    # Print each modified relation
    for rel in modified_relations:
        print(modified_relations[rel])  # Display the modified relation
        print('\n')

    return modified_relations, False  # Return modified relations and indicate they are not in 3NF

#####################BCNF#########
def attribute_closure(attributes_set, functional_deps):
    # Determine the closure of a set of attributes based on provided functional dependencies.
    closure_result = set(attributes_set)  # Start with the initial set of attributes
    while True:
        previous_closure = closure_result.copy()  # Keep a copy of the previous state
        for lhs, rhs in functional_deps.items():
            # If the left-hand side is a subset of the current closure, add the right-hand side attributes
            if set(lhs).issubset(closure_result):
                closure_result.update(rhs)  # Expand the closure with new attributes
        
        # Break the loop if no new attributes were added
        if previous_closure == closure_result:
            break
            
    return closure_result  # Return the final closure

def validate_bcnf(relations_dict, pk, fds):
    # Validate if each relation satisfies the conditions for BCNF
    for relation_name, relation in relations_dict.items():
        all_columns = set(relation.columns)  # Get all columns of the relation
        # Check each functional dependency in the set of functional dependencies (fds)
        for lhs, rhs in fds.items():
            for dependent in rhs:
                # Ensure dependent is not part of the left-hand side (lhs)
                if dependent not in lhs:
                    # If the attribute closure of lhs does not cover all columns, BCNF is violated
                    if all_columns - attribute_closure(lhs, fds):
                        return False  # Return False to indicate BCNF violation
    return True  # Return True if all relations satisfy BCNF conditions


def transform_to_BCNF(relations_dict, pk, fds):
    updated_bcnf_relations = {}  # Dictionary to hold updated BCNF relations
    
    while True:  # Continue until no further transformations are needed
        is_bcnf_valid = validate_bcnf(relations_dict, pk, fds)  # Validate current relations
        
        if is_bcnf_valid:
            return relations_dict, is_bcnf_valid  # Return current relations if they are valid in BCNF

        print("Transformed relations into BCNF:\n")
        # Iterate over each relation for transformation
        for relation_name, relation in relations_dict.items():
            for lhs, rhs in fds.items():
                closure_result = attribute_closure(set(lhs), fds)  # Get the attribute closure of lhs
                # Check if closure does not cover all columns in the relation
                if not closure_result.issuperset(relation.columns):
                    combined_columns = list(lhs) + rhs  # Combine lhs and rhs for new relation
                    # Ensure the combined columns are part of the relation but not all columns
                    if set(combined_columns).issubset(relation.columns) and not set(combined_columns) == set(relation.columns):
                        # Create a new relation using lhs and rhs
                        new_relation = relation[combined_columns].drop_duplicates()
                        updated_bcnf_relations[tuple(lhs)] = new_relation  # Store the new relation
                        # Drop the dependent attributes from the original relation
                        relation = relation.drop(columns=rhs)

            # Store the modified original relation in the updated dictionary
            updated_bcnf_relations[relation_name] = relation

        # Update relations_dict to be the modified relations
        relations_dict = updated_bcnf_relations.copy()

        # Print each updated relation
        for updated_relation in updated_bcnf_relations:
            print(updated_bcnf_relations[updated_relation])  # Display the updated relation
            print('\n')

        return updated_bcnf_relations, is_bcnf_valid  # Return the final updated relations and BCNF validity


###############4NF##############
def validate_4NF(relations, mvds):
    # Validate if each relation satisfies the 4NF conditions based on multi-valued dependencies (MVDs)
    for relation_name, relation in relations.items():
        # Check each determinant and its associated dependents in the MVDs
        for determinant, dependents in mvds.items():
            for dependent in dependents:
                # Determine if the determinant is a tuple or a single attribute
                if isinstance(determinant, tuple):
                    determinant_cols = list(determinant)  # Convert tuple to list for processing
                else:
                    determinant_cols = [determinant]  # Wrap single attribute in a list

                # Check if all determinant columns and the dependent are present in the relation
                if all(col in relation.columns for col in determinant_cols + [dependent]):
                    # Group the relation by the determinant columns and aggregate the dependent
                    grouped = relation.groupby(determinant_cols)[dependent].apply(set).reset_index()
                    # If the number of groups is less than the number of rows in the relation, 4NF is violated
                    if len(grouped) < len(relation):
                        print(f"Multi-valued dependency violation: {determinant} ->> {dependent}")
                        return False  # Return False to indicate 4NF violation

    return True  # Return True if all relations satisfy 4NF conditions


def transform_to_4NF(relations, mvds):
    four_relations = {}  # Dictionary to hold relations transformed into 4NF
    fournfcheck = validate_4NF(relations, mvds)  # Validate the relations for 4NF

    if fournfcheck:
        return relations, fournfcheck  # Return original relations if they are already in 4NF
    else:
        print(f"The relation after transforming into 4NF.\n")
        # Iterate over each relation for transformation
        for relation_name, relation in relations.items():
            for determinant, dependents in mvds.items():
                for dependent in dependents:
                    # Determine if the determinant is a tuple or a single attribute
                    if isinstance(determinant, tuple):
                        determinant_cols = list(determinant)  # Convert tuple to list for processing
                    else:
                        determinant_cols = [determinant]  # Wrap single attribute in a list

                    # Check if all determinant columns and the dependent are present in the relation
                    if all(col in relation.columns for col in determinant_cols + [dependent]):
                        # Group the relation by the determinant columns and aggregate the dependent
                        grouped = relation.groupby(determinant_cols)[dependent].apply(set).reset_index()
                        # If a violation is found, create new relations based on the determinants
                        if len(grouped) < len(relation):
                            # Create first table with determinant columns and dependent
                            table_1 = relation[determinant_cols + [dependent]].drop_duplicates()
                            four_relations[tuple(determinant_cols)] = table_1  # Store the first table
                            # Create second table excluding the dependent and the determinant columns
                            table_2 = relation[determinant_cols + [col for col in relation.columns if col not in [dependent] + determinant_cols]].drop_duplicates()
                            four_relations[relation_name] = table_2  # Store the second table
                            break  # Exit the loop to avoid duplicate processing
                else:
                    continue  # Continue to the next dependent if no break occurred
                break  # Exit to the next relation if a break occurred
            else:
                four_relations[relation_name] = relation  # Store the original relation if no changes were made

    # If the number of transformed relations matches the original, return them
    if len(four_relations) == len(relations):
        return four_relations, False  # Return False to indicate no further transformation is needed
    else:
        return transform_to_4NF(four_relations, mvds)  # Recursive call to ensure all relations are in 4NF


##################5NF######################

# Group the relation by the specified determinant and count occurrences
def is_superkey(relation, determinant):
    #Determine if a given set of attr (determinant) is a superkey for the relation.
    # Group the relation by the determinant attr and count occurrences
    count_df = relation.groupby(list(determinant)).size().reset_index(name='count')
    # Check if all counts are 1 (indicating uniqueness)
    unique_count = count_df['count'] == 1
    return unique_count.all()  # Return True if all counts are 1, indicating a superkey
def validate_5NF(relations):
    candidate_keys_dict = {}  # Dictionary to store candidate keys for each relation

    # Iterate over each relation to get candidate keys from user input
    for relation_name, relation in relations.items():
        print(relation)  # Display the current relation for user reference
        user_input = input("Enter the candidate keys for above relation (e.g., (A, B), (C, D)): ")
        print('\n')
        
        # Extract candidate keys from the user input using regex
        tuples = re.findall(r'\((.*?)\)', user_input)
        candidate_keys = [tuple(map(str.strip, t.split(','))) for t in tuples]
        candidate_keys_dict[relation_name] = candidate_keys  # Store the candidate keys

    print(f'Candidate Keys for tables:')  # Print all candidate keys collected
    print(candidate_keys_dict)
    print('\n')

    # Validate each relation against 5NF conditions
    for relation_name, relation in relations.items():
        candidate_keys = candidate_keys_dict[relation_name]  # Get candidate keys for the current relation

        # Convert relation data to a list of tuples for easy processing
        data_tuples = [tuple(row) for row in relation.to_numpy()]

        # Function to project data based on specified attributes
        def project(data, attributes):
            return {tuple(row[attr] for attr in attributes) for row in data}

        # Function to check if the attributes form a superkey
        def superkey(attributes):
            for key in candidate_keys:
                if set(key).issubset(attributes):  # Check if the key is a subset of attributes
                    return True
            return False  # Return False if no superkey is found

        # Iterate over all combinations of attributes in the relation
        for i in range(1, len(relation.columns)):
            for attrs in combinations(relation.columns, i):
                if superkey(attrs):  # Skip combinations that are superkeys
                    continue

                # Project the data for the selected attributes and their complement
                projected_data = project(data_tuples, attrs)
                complement_attrs = set(relation.columns) - set(attrs)  # Attributes not included in the projection
                complement_data = project(data_tuples, complement_attrs)  # Project the complement attributes

                # Create a join of the projected data and complement data
                joined_data = {(row1 + row2) for row1 in projected_data for row2 in complement_data}
                # Check if the original data matches the joined data
                if set(data_tuples) != joined_data:
                    print("Failed 5NF check for attributes:", attrs)  # Print failing attributes
                    return False, candidate_keys_dict  # Return False if validation fails

    return True, candidate_keys_dict  # Return True if all relations are in 5NF


def decompose_into_5NF(relation_name, dataframe, candidate_keys):
    # Function to project dataframe based on given attributes
    def project(df, attributes):
        return df[list(attributes)].drop_duplicates().reset_index(drop=True)  # Drop duplicates and reset index

    # Function to check if the decomposition is lossless
    def is_lossless(df, df1, df2):
        common_columns = set(df1.columns) & set(df2.columns)  # Find common columns between tables
        if not common_columns:
            return False  # No common columns means lossless join is not possible
        joined_df = pd.merge(df1, df2, how='inner', on=list(common_columns))  # Perform inner join
        return df.equals(joined_df)  # Check if original dataframe equals the joined dataframe

    decomposed_rel = [dataframe]  # Start with the original dataframe in a list

    # Decompose the relation based on candidate keys
    for key in candidate_keys:
        new_tables = []  # List to hold new tables after decomposition
        for table in decomposed_rel:
            if set(key).issubset(set(table.columns)):  # Check if key is in current table columns
                table1 = project(table, key)  # Create the first table with key attributes
                remaining_columns = set(table.columns) - set(key)  # Find remaining columns
                table2 = project(table, remaining_columns | set(key))  # Create second table with remaining attributes

                # Check if the decomposition is lossless
                if is_lossless(table, table1, table2):
                    new_tables.extend([table1, table2])  # Add both tables if lossless
                else:
                    new_tables.append(table)  # Keep the original table if lossless join fails
            else:
                new_tables.append(table)  # Keep the original table if key is not present
        decomposed_rel = new_tables  # Update the list of decomposed relations

    return decomposed_rel  # Return the list of decomposed relations


def transform_to_5NF(relations, pk, fds):
    five_relations = {}  # Dictionary to hold transformed 5NF relations
    fivenfcheck, candidate_keys_dict = validate_5NF(relations)  # Validate for 5NF

    if fivenfcheck:
        return relations, fivenfcheck  # Return existing relations if they are in 5NF
    else:
        print(f"The relation after transforming into 5NF.\n")
        for relation_name, relation in relations.items():  # Iterate over relations for transformation
            candidate_keys = candidate_keys_dict[relation_name]  # Get candidate keys for the relation
            decomposed_relations = decompose_into_5NF(relation_name, relation, candidate_keys)  # Decompose into 5NF
            five_relations[relation_name] = decomposed_relations  # Store the decomposed relations

    return five_relations, fivenfcheck  # Return transformed relations and check status
