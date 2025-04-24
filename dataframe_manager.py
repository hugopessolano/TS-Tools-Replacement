import pandas as pd
from schemas.request_schemas import *
import os
from schemas.endpoint_map import Endpoint, EndpointMap, build_endpoint_map
from dataframe_utils import reformat_payload
import json
from log_db import stdout_logger, db_logger, db_only_logger
import uuid

class DataframeManager:
    def __init__(self, csv_name:str|None=None, df:pd.DataFrame|None=None):
        self._id:uuid.UUID = uuid.uuid4()
        self._csv_name:str = csv_name
        self._script_dir:str = os.path.dirname(os.path.realpath(__file__))
        self._file_path:str = os.path.join(self._script_dir, 'tmp', self._csv_name) if csv_name else None
        self._export_file_path:str = os.path.join(self._script_dir, 'export', f"export_{self._csv_name if csv_name else 'file'}")
        self._current_df: pd.DataFrame = self.read_csv() if csv_name else df
        
        if self._current_df is None:
            stdout_logger.critical("DataframeManager requires either a CSV file name or a DataFrame to be initialized.")
            raise ValueError("DataframeManager requires either a CSV file name or a DataFrame to be initialized.")
        else:
            stdout_logger.debug(f"DataframeManager initialized with DataFrame size: {self._current_df.shape}")
    
    @property
    def id(self):
        return self._id
    @property
    def csv_name(self):
        return self._csv_name
    @property
    def current_df(self):
        return self._current_df

    @current_df.setter
    def current_df(self, df: pd.DataFrame):
        if not isinstance(df, pd.DataFrame):
            stdout_logger.error("dataframe must be a pandas DataFrame")
            raise ValueError("current_df must be a pandas DataFrame")
        self._current_df = df

    def read_csv(self):
        """Reads a CSV file and returns a DataFrame."""
        try:
            stdout_logger.debug(f"Reading CSV file from: {self._file_path}")
            df = pd.read_csv(self._file_path)
            stdout_logger.info(f"Successfully read CSV from: {self._file_path}")
            return df
        except FileNotFoundError:
            stdout_logger.error(f"CSV File not found at: {self._file_path}")
            raise
        except Exception as e:
            stdout_logger.error(f"Error at reading CSV: {e}")
            raise

    def row_to_request(self, row: pd.Series, request_type:type[Request], endpoint:Endpoint) -> Request:
        """Converts a DataFrame row to a request object dynamically."""
        try:
            stdout_logger.debug(f"Converting row {row.name} to request of type {request_type.__name__} for endpoint '{endpoint.path}'...")
            row_copy = row.copy()
            request_content = {
                'endpoint': endpoint,
                'positional_url_arguments': list(map(str,row_copy.to_list()[:endpoint.positional_arguments_count])) if endpoint.positional_arguments_count > 0 else None
                            }
        
            if 'params' in row_copy.index:
                stdout_logger.debug(f"Row {row.name}: Found 'params' column. Processing...")
                if 'params' in request_type.model_fields:
                    params_data = row_copy.pop('params')
                    # Podrías necesitar parsear si es un string JSON, etc.
                    if isinstance(params_data, str):
                        try:
                            params_data = json.loads(params_data)
                        except json.JSONDecodeError:
                            db_logger.bind(operation_id=self.id).warning(f"Row {row_copy.name}: Could not parse 'params' column as JSON: {params_data}")
                            params_data = {} # o None, o manejar el error
                    request_content['params'] = params_data
                else:
                    db_logger.bind(operation_id=self.id).warning(f"Row {row_copy.name}: 'params' column not found in {request_type.__name__} model. Ignoring irrelevant data.")
                    row_copy.pop('params')
                
            if 'payload' in request_type.model_fields:
                stdout_logger.debug(f"Row {row.name}: Processing payload data...")
                payload = {}
                payload_arguments = row_copy[endpoint.positional_arguments_count:].to_dict()
                if endpoint.validation_model:
                    stdout_logger.debug(f"Row {row.name}: Found validation model. Processing validations")
                    keys_to_reformat = [key for key in tuple(payload_arguments.keys()) if key in endpoint.validation_model.model_fields]
                    if len(keys_to_reformat) > 0:
                        stdout_logger.debug(f"Row {row.name}: Found keys to reformat: {keys_to_reformat}")
                        payload = reformat_payload(payload_arguments, keys_to_reformat, endpoint.validation_model)
                
                request_content['payload'] = payload            
            
            request = request_type(**request_content)
            return request
        except Exception as e:
            db_logger.bind(operation_id=self.id).error(f"Error converting row {row.name} to request: {e}", exc_info=True)
            return None 
        
    def parse_requests_into_dataframe(self, request_type:type[Request], endpoint:Endpoint) -> pd.DataFrame:
        """
        Parses each row into a Request object using row_to_request and adds it
        as a new 'request' column to the internal DataFrame.
        """
        if self._current_df.empty:
            stdout_logger.warning("DataFrame is empty. No requests to parse.")
            self._current_df['request'] = None
            return self._current_df

        stdout_logger.info(f"Parsing {len(self._current_df)} rows into {request_type.__name__} objects for endpoint '{endpoint.path}'...")
        self._current_df['request'] = self._current_df.apply(
            self.row_to_request,
            axis=1,
            args=(request_type, endpoint) 
        )
        successful_requests = self._current_df['request'].notna().sum()
        
        if successful_requests == 0:
            stdout_logger.warning("No requests were successfully parsed. Check the input data.")
            raise ValueError("No requests were successfully parsed. Check the input data.")
        
        stdout_logger.debug(f"Parsed {successful_requests} requests successfully.")
        failed_requests = self._current_df['request'].isna().sum()
        db_logger.bind(operation_id=self.id,).warning(f"Failed to parse {failed_requests} requests.") if failed_requests > 0 else None
        
        if failed_requests > 0:
            stdout_logger.warning(f"Check logs for errors related to the {failed_requests} failed request conversions.")
            try:
                failed_mask = self._current_df['request'].isna()
                failed_rows_df = self._current_df.loc[failed_mask, self._current_df.columns != 'request']
                failed_rows_dict_by_index = failed_rows_df.to_dict(orient='index')
                failed_data_log = json.dumps(failed_rows_dict_by_index, indent=2, default=str)
                db_only_logger.bind(operation_id=self.id, data=failed_data_log).warning(f"Data of failed rows:")
            except Exception as log_e:
                db_logger.bind(operation_id=self.id).warning(f"Could not serialize failed rows data to JSON: {log_e}.")

        return self._current_df
            
    def write_csv(self, data:pd.DataFrame, index=False):
        """Writes a DataFrame to a CSV file."""
        try:
            data.to_csv(self._file_path, index=index)
            print(f"Data successfully written to {self._file_path}")
        except Exception as e:
            print(f"Error writing to CSV file: {e}")

if __name__ == '__main__':
    csv_manager = DataframeManager('requests_data.csv')
    endpoint_map:EndpointMap = build_endpoint_map()
    endpoint:Endpoint = endpoint_map.products.single
    csv_manager.parse_requests_into_dataframe(PutRequest, endpoint)
    print('ok')