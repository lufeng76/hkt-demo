from collections import OrderedDict
import argparse
from argparse import ArgumentParser
import os.path
import sys
from datetime import datetime
import re

import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

class DataIngestion:
    """A helper class which contains the logic to translate the file into
    a format BigQuery will accept."""

    def __init__(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        self.schema_str = ''
        # Here we read the output schema from a json file.  This is used to specify the types
        # of data we are writing to BigQuery.

        schema_file = 'custbase.schema'

        self.pos_dict = {}
        self.type_dict = {}
        name_list = []
        pos_list = []
        type_list = []
        SchemaFile = 'custbase.schema'
        with open(SchemaFile) as myfile:
            for line in myfile:
                values = line.split(',')
                col_name = values[0].strip()
                col_pos = values[1].strip()
                col_type = values[2].strip()
                #print(col_name+','+col_pos+','+col_type)
                name_list.append(col_name)
                pos_list.append(col_pos)
                type_list.append(col_type)
        
        self.pos_dict = dict(zip(name_list, pos_list))
        self.type_dict = dict(zip(name_list, type_list))  

        for item in name_list:
            nn = ''
            if self.type_dict[item] == 'char':
                nn = 'string'
            elif self.type_dict[item] == 'decimal':
                nn = 'numeric'
            else:
                nn = 'timestamp'   

            if self.schema_str == '':
                nn = ''
                if self.type_dict[item] == 'char':
                    nn = 'string'
                elif self.type_dict[item] == 'decimal':
                    nn = 'numeric'
                else:
                    nn = 'timestamp'
                self.schema_str = item + ':' + nn
            else:
                self.schema_str = self.schema_str + ',' +  item + ':' + nn

        print(self.schema_str)       
        #schema_file = os.path.join(dir_path, 'resources',
        #                           'usa_names_year_as_date.json')

        #with open(schema_file) \
        #        as f:
        #    data = f.read()
        #    # Wrapping the schema in fields is required for the BigQuery API.
        #    self.schema_str = '{"fields": ' + data + '}'

    def parse_method(self, string_input):
        """This method translates a single line of comma separated values to a
        dictionary which can be loaded into BigQuery.
        Args:
            string_input: A comma separated list of values in the form of
                state_abbreviation,gender,year,name,count_of_babies,dataset_created_date
                Example string_input: KS,F,1923,Dorothy,654,11/28/2016
        Returns:
            A dict mapping BigQuery column names as keys to the corresponding value
            parsed from string_input. In this example, the data is not transformed, and
            remains in the same format as the CSV.
            example output:
            {
                'state': 'KS',
                'gender': 'F',
                'year': '1923',
                'name': 'Dorothy',
                'number': '654',
                'created_date': '11/28/2016'
            }
         """
        # Strip out carriage return, newline and quote characters.
        row = {}
        #print(string_input)

        for item in self.pos_dict.items():
            col_name = item[0]
            start_pos = int(item[1].split(':')[0]) - 1
            end_pos = int(item[1].split(':')[1])
            value = string_input[start_pos:end_pos]
            col_type = self.type_dict[col_name]
            if col_type == 'date' and len(value.strip()) > 9:
                x = datetime.strptime(value.strip(), '%d/%m/%Y')
                row[col_name] = x.strftime('%Y-%m-%d')
            else:
                row[col_name] = value.strip()

        # print(row)
        return row

def run(argv=None):
    """The main function which creates the pipeline and runs it."""

    parser = argparse.ArgumentParser()

    # Here we add some specific command line arguments we expect.
    # Specifically we have the input file to read and the output table to write.
    # This is the final stage of the pipeline, where we define the destination
    # of the data. In this case we are writing to BigQuery.
    parser.add_argument(
        '--input',
        dest='input',
        required=False,
        help='Input file to read. This can be a local file or '
        'a file in a Google Storage Bucket.',
        # This example file contains a total of only 10 lines.
        # Useful for developing on a small set of data.
        default='vi_sample.txt')

    # This defaults to the lake dataset in your BigQuery project. You'll have
    # to create the lake dataset yourself using this command:
    # bq mk lake
    parser.add_argument('--output',
                        dest='output',
                        required=False,
                        help='Output BQ table to write results to.',
                        default='lake.vi_sample')

    # Parse arguments from the command line.
    known_args, pipeline_args = parser.parse_known_args(argv)

    # DataIngestion is a class we built in this script to hold the logic for
    # transforming the file into a BigQuery table.

    data_ingestion = DataIngestion()  

    # Initiate the pipeline using the pipeline arguments passed in from the
    # command line. This includes information such as the project ID and
    # where Dataflow should store temp files.
    p = beam.Pipeline(options=PipelineOptions(pipeline_args))

    (p
     # Read the file. This is the source of the pipeline. All further
     # processing starts with lines read from the file. We use the input
     # argument from the command line. We also skip the first line which is a
     # header row.
     | 'Read from a File' >> beam.io.ReadFromText(known_args.input,
                                                  skip_header_lines=1)
     # This stage of the pipeline translates from a CSV file single row
     # input as a string, to a dictionary object consumable by BigQuery.
     # It refers to a function we have written. This function will
     # be run in parallel on different workers using input from the
     # previous stage of the pipeline.
     | 'String To BigQuery Row' >>
     beam.Map(lambda s: data_ingestion.parse_method(s)) |
     'Write to BigQuery' >> beam.io.Write(
         beam.io.BigQuerySink(
             # The table name is a required argument for the BigQuery sink.
             # In this case we use the value passed in from the command line.
             known_args.output,
             # Here we use the simplest way of defining a schema:
             # fieldName:fieldType
             schema=data_ingestion.schema_str,
             # Creates the table in BigQuery if it does not yet exist.
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
             # Deletes all data in the BigQuery table before writing.
             write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)))
    p.run().wait_until_finish()    

def run_local(argv=None):
    """The main function which creates the pipeline and runs it."""

    #parser = argparse.ArgumentParser()

    # DataIngestion is a class we built in this script to hold the logic for
    # transforming the file into a BigQuery table.

    data_ingestion = DataIngestion()  
    
    InputFile = 'vi_sample.txt'
    with open(InputFile) as myfile:
        i = 0
        for line in myfile:
            if len(line) < 1000:
                continue
            data_ingestion.parse_method(line)
            i = i + 1
            if i > 0:
                break

if __name__ == '__main__':
    # logging.getLogger().setLevel(logging.INFO)
    run()

