RDBMS Normalization Project
# Overview
This project implements a program that performs normalization of database relations up to the 5th Normal Form (5NF). It takes a database schema and functional dependencies as input, normalizes the relations based on these dependencies, and generates the necessary SQL queries to create the normalized database tables. Additionally, the program can identify the highest normal form achieved by the input tables.

#Objectives
- Normalize database relations based on user-defined functional dependencies.
- Produce SQL queries for creating normalized tables.
- Optionally determine the highest normal form of the input tables.

# Input Requirements
# Database Schema
The input must include:
- Table Name: Name of the table to be normalized.
- List of Columns: Attributes within the table.
- Key Constraints: Definitions of primary keys and any candidate keys for normalization.
- Multi-Valued Attributes: Identification of any attributes holding non-atomic data.
The program may process one table at a time for normalization.
# Data Instances
- Lower Normal Forms (1NF to BCNF): Data instances are not required for these normal forms; the focus is on schema structure and functional dependencies.
- Fourth Normal Form (4NF): Requires user-provided Multi-Valued Dependencies (MVDs) and data instances to validate these dependencies before decomposition.
- Fifth Normal Form (5NF): Detailed data instances are necessary to identify join dependencies effectively.
# Functional Dependencies
Functional dependencies should be expressed in the format:  X -> Y Where `X` and `Y` are lists of attributes from the schema.
# Normalization Target
Users can specify the highest normal form they wish to achieve (1NF, 2NF, 3NF, BCNF, 4NF, or 5NF). The program will ensure that the database meets the criteria for each preceding normal form in sequence.
# Input Format
Users have flexibility in choosing the input format and data representation that best suits their needs.
# Output
Upon successful normalization, the program will output:
- SQL Queries: Ready-to-execute SQL statements for creating normalized tables, including primary keys, foreign keys, and relevant constraints.
- Normalized Schema: A detailed representation of the normalized tables, including each table's name, attributes, and constraints.

This project aims to provide a comprehensive tool for database normalization, facilitating the understanding and application of relational database theory.
