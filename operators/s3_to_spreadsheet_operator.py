from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
import pandas as pd
from BoxPlugin.hooks.box_hook import BoxHook


class S3ToSpreadsheetOperator(BaseOperator):
    """
    S3 to Spreadsheet Operator
    :param input_s3_conn_id:            The input s3 connection id.
    :type input_s3_conn_id:             string
    :param input_s3_bucket:             The input s3 bucket.
    :type input_s3_bucket:              string
    :param input_s3_key:                The input s3 key. This can be formatted
                                        as:
                                            1) String - To be used with a
                                            single file with default 'Sheet1'
                                            naming convention.
                                            2) Dictionary - To be used with a
                                            single file with a distinct sheet
                                            name to be used in the final
                                            spreadsheet.
                                            (e.g. "{"key_name": "sheet_name"}")
                                            3) List of dictionaries - To be
                                            used with multiple files each
                                            going into a separate sheet.
                                              (e.g. "[
                                                  {"key_name1": "sheet_name1"},
                                                  {"key_name2": "sheet_name2"},
                                                  {"key_name3": "sheet_name3"},
                                              ])
    :type input_s3_key:                 string
    :param input_file_type:             The file type of the input file.
                                        (JSON/CSV)
    :type input_file_type:              string
    :type output_destination:           The output destination. Currently,
                                        accepts "S3" (default) or "Box".
    :param output_destination:          string
    :param output_conn_id:              The output connection id.
    :type output_conn_id:               string
    :param output_s3_bucket:            The output s3 bucket. Only used if
                                        output_destination set to "S3".
    :type output_s3_bucket:             string
    :param output_s3_key:               The output s3 key. Only used if
                                        output_destination set to "S3".
    :type output_s3_key:                string
    :param box_folder_id:               The relevant box folder id. This value
                                        can be found in the URL when inside the
                                        Box UI. By default, this value will be
                                        set to 0 (i.e. home directory.)
    :type box_folder_id:                string
    :param box_file_name:               The file name in Box. This cannot be
                                        the same as any file already in the
                                        destination Box folder.
    :type box_file_name:                string
    :param output_format:               The output file format. Currently, only
                                        accepts "excel".
    :type output_format:                string
    :param output_payload:              The output payload, a self-defined
                                        dictionary of dataframe parameters to
                                        pass into output functions.
    :type output_payload:               string
    :param filters:                     Key-Value pairs that filters the pandas
                                        dataframe prior to creating the Excel
                                        file.
    :type filters:                      dictionary
    :param append_fields:               Key-Value pairs that get appended to
                                        the pandas dataframe prior to creating
                                        the Excel file.
    :type append_fields:                dictionary
    """

    template_fields = ['input_s3_key',
                       'output_s3_key',
                       'output_payload',
                       'filters',
                       'append_fields',
                       'output_box_file_name']

    @apply_defaults
    def __init__(self,
                 input_s3_conn_id,
                 input_s3_bucket,
                 input_s3_key,
                 input_file_type,
                 output_destination='S3',
                 output_conn_id=None,
                 output_s3_bucket=None,
                 output_s3_key=None,
                 output_box_folder_id='0',
                 output_box_file_name=None,
                 output_format=None,
                 output_payload=None,
                 filters=None,
                 append_fields=None,
                 *args,
                 **kwargs):
        super(S3ToSpreadsheetOperator, self).__init__(*args, **kwargs)
        self.input_s3_conn_id = input_s3_conn_id
        self.input_s3_bucket = input_s3_bucket
        self.input_s3_key = input_s3_key
        self.input_file_type = input_file_type
        self.output_destination = output_destination
        self.output_conn_id = output_conn_id
        self.output_s3_bucket = output_s3_bucket
        self.output_s3_key = output_s3_key
        self.output_box_folder_id = output_box_folder_id
        self.output_box_file_name = output_box_file_name
        self.output_format = output_format
        self.output_payload = output_payload
        self.filters = filters
        self.append_fields = append_fields

        if self.input_file_type.lower() not in ('json', 'csv'):
            raise Exception('Unsupported input file type.')

        if self.output_format.lower() not in ('excel'):
            raise Exception('Unsupported output file format.')

        if self.output_destination.lower() not in ('s3', 'box'):
            raise Exception('Unsupported output destination.')

    def execute(self, context):
        input_s3 = S3Hook(s3_conn_id=self.input_s3_conn_id)
        if self.output_format == 'excel':
            w = pd.ExcelWriter('temp.xlsx')
            print(self.input_s3_key)
            print(type(self.input_s3_key))
            if isinstance(self.input_s3_key, list):
                for i in self.input_s3_key:
                    for k, v in i.items():
                        input_key = \
                         (input_s3.get_key(k,
                                           bucket_name=self.input_s3_bucket))
                        df = self.read_file(input_key)
                        df.to_excel(w, sheet_name=v, **self.output_payload)
            else:
                if isinstance(self.input_s3_key, dict):
                    for k, v in self.input_s3_key.items():
                        input_key = input_s3.get_key(k,
                                                     bucket_name=self.input_s3_bucket)
                        df = self.read_file(input_key)
                        df.to_excel(w, sheet_name=v, **self.output_payload)
                else:
                    print(self.input_s3_key)
                    input_key = input_s3.get_key(self.input_s3_key,
                                                 bucket_name=self.input_s3_bucket)
                    df = self.read_file(input_key)
                    print(df.head())
                    df.to_excel(w, **self.output_payload)
            input_s3.connection.close()
            w.save()
            if self.output_destination.lower() == 's3':
                output_s3 = S3Hook(s3_conn_id=self.output_conn_id)
                output_s3.load_file(
                    filename='temp.xlsx',
                    key=self.output_s3_key,
                    bucket_name=self.output_s3_bucket,
                    replace=True)
                output_s3.connection.close()
            elif self.output_destination.lower() == 'box':
                box_hook = BoxHook(self.output_conn_id)
                print(self.output_box_file_name)
                box_hook.upload_file(folder_id=self.output_box_folder_id,
                                     file_path='temp.xlsx',
                                     file_name=self.output_box_file_name)

    def read_file(self, input_key):
        if self.input_file_type.lower() == 'json':
            print('INPUT KEY')
            print(input_key)
            df = pd.read_json(input_key
                              .get_contents_as_string(encoding='utf-8'),
                              orient='records')
        elif self.input_file_type.lower() == 'csv':
            df = pd.read_csv(input_key, low_memory=False)
        # Apply a mapping function to escape invalid characters
        df = (df.applymap(lambda x: x.encode('unicode_escape')
                                     .decode('utf-8')
                          if isinstance(x, str) else x))
        if self.filters:
            for i in self.filters:
                if i in df.columns.values.tolist():
                    df = df[df[i] == self.filters[i]]
        # Append on any user-defined fields if they exist
        if self.append_fields:
            for i in self.append_fields:
                df[i] = self.append_fields[i]
        return df
