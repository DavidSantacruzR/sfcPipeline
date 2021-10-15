# Here write the functions and classes to help in data pre-processing.
# No need for API auth as info is public and it's required approximately one request per month.
# Data is approximately on a two month lag.

from urllib import request as url_req
from sqlalchemy import create_engine
from datetime import date as dt
import os
import json
import pandas as pd
import calendar


class FetchDataFromOpenSource:

    def __init__(self, request_endpoint: str, fetching_date: str, active_directory: str, entity_type: str
                 , entity_name: str = "empty"):
        self.endpoint = request_endpoint
        self.searching_date = fetching_date
        self.directory = active_directory
        self.entity_id = entity_type
        self.name = entity_name

    def parsing_json_data(self):  # This function returns all the websites links in a list.
        if self.name == "empty":
            url = self.endpoint + "fecha_corte=" + self.searching_date + "&tipo_entidad=" + self.entity_id
        else:
            url = self.endpoint + "fecha_corte=" + self.searching_date + "&tipo_entidad=" + self.entity_id \
                  + "&nombre_entidad=" + self.name
        # this is the endpoint taken from the open database of the government of colombia.
        response = url_req.urlopen(url)
        data = json.loads(response.read())
        os.chdir(self.directory)
        outfile = open("updated_data.JSON", mode="w")
        return json.dump(data, outfile)

    def parsing_csv_data(self):
        if self.name == "empty":
            url = self.endpoint + "fecha_corte=" + self.searching_date + "&tipo_entidad=" + self.entity_id
        else:
            url = self.endpoint + "fecha_corte=" + self.searching_date + "&tipo_entidad=" + self.entity_id \
                  + "&nombre_entidad=" + self.name
        # this is the endpoint taken from the open database of the government of colombia.
        financial_data = pd.DataFrame(pd.read_csv(url))  # Pandas directly parse from urls
        return financial_data.to_csv(path_or_buf=self.directory + "/financial_data.csv", index=False)


class GetLookupDate:

    def __init__(self):
        self.year = dt.today().year
        self.month = dt.today().month - 3  # 3 month lag in the data.
        self.day = calendar.monthrange(self.year, self.month)[-1]

    def get_lookup_date(self):
        self.month = "0" + str(self.month) if self.month < 10 else self.month
        self.day = "0" + str(self.day) if self.day < 10 else self.day
        lookup_date = str(self.year) + "-" + str(self.month) + "-" + str(self.day) + "T00:00:00.000"
        return lookup_date


class DatabaseAdmin:
    def __init__(self, engine: str, table_name: str, chunk_size: int, file, filepath: str):
        self.engine = engine
        self.table = table_name
        self.chunk = chunk_size
        self.file = file
        self.filepath = filepath

    def connect_to_the_database(self):

        try:
            conn = create_engine(self.engine).connect()
            print('Successfully connected to', self.engine)  # Opens the connection to the database.
            conn.close()
            return conn  # Establish a connection to the database after testing.

        except Exception as error:
            print('It was not possible to connect to the server, revise:', error)

    def load_data(self):
        os.chdir(self.filepath)
        data_to_update = pd.DataFrame(pd.read_csv(self.file))
        data_to_update.to_sql(self.table, con=self.engine, if_exists='append', chunksize=self.chunk, index=False)

    def close_connection(self):
        pass  # Should close the session once called.


