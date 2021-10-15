import sys
import os
from dotenv import load_dotenv, find_dotenv
from dataPreProcessing.utils import FetchDataFromOpenSource, GetLookupDate, DatabaseAdmin

# Defining the connection route using environment variables:

load_dotenv(find_dotenv())
string_connection = str(os.getenv('url'))
table_to_update = str(os.getenv('table'))

# List of financial entities in Colombia.

entities = {"COMPANIAS DE SEGUROS GENERALES": 13, "SOCIEDADES FIDUCIARIAS": 5, "COMPANIAS DE SEGUROS DE VIDA": 14,
            "COMPANIAS DE FINAN COMER": 4, "ESTABLECIMIENTOS BANCARIOAS": 1, "INSTITUCIONES OFICIALES ESP": 22,
            "CORPORACIONES FINANCIERAS": 2, "ALMACENES DE DEPOSITO": 6, "SOCIEDADES DE CAPITALIZACION": 10,
            "SOCIEDADES COOPERATIVAS DE SEGUROS": 15, "SOC. ADM. FONDOS DE PENSIONES Y CES": 23,
            "ENT. ADM. PRIMA MEDIA": 25, "COOPERATIVAS DE CARACTER FINANCIERO": 32, "SEDEPES": 128}

entities_list = list(entities.values())
entities_list.sort()
# some entities aren't included in the dictionary, if updated required check SFC website.

root_file_directory = str(sys.path[1]) + "/csvFiles"

# The root_file_directory should be csvFiles if the data is going to be download as a csv type.
http_endpoint: str = "https://www.datos.gov.co/resource/nf5h-3jc8.csv?"
current_date = GetLookupDate().get_lookup_date()  # Create a first instance of the class.

FetchDataFromOpenSource(http_endpoint, current_date, root_file_directory, str(entities_list[0])
                        , "BANCO%20POPULAR").parsing_csv_data()

# Creating a new session in the database server:
new_session = DatabaseAdmin(string_connection, table_to_update, chunk_size=1000, file='financial_data.csv'
                            , filepath=root_file_directory)

# Connecting to the server:
new_session.connect_to_the_database()

# Load data:
new_session.load_data()

# Close connection
new_session.close_connection()
