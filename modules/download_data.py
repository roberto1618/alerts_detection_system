"""
This module has the necessary functions to download data from *GA* and *BQ*. You can configure metrics
for both sources.
"""

from config.alerts_table_styles.extra_styles import ExtraStylesBQ
from config.alerts_table_styles.extra_styles import ExtraStylesPred
from config.alerts_table_styles.extra_styles import ExtraStylesEvaluation
from apiclient.discovery import build
from google.oauth2.service_account import Credentials
from google.cloud import bigquery
import pandas as pd
import time
from google.api_core.exceptions import NotFound
from google.api_core.exceptions import BadRequest
import logging

class DownloadData():
    """This class has the necessary methods to download the data to make the analysis."""

    def __init__(self, token_path):
        """
        Init function
        
        :param token_path: Path of the token for API connection.
        :type token_path: string
        """
        
        self.token_path = token_path
    
    def get_service_GA(self):
        """
        Function to get the service connection to GA.
        """

        # Set the scopes
        SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
        
        credentials = Credentials.from_service_account_file(
            self.token_path, scopes = SCOPES
        )

        # Initialize GA service to download data
        service = build(serviceName = 'analyticsreporting', version = 'v4', credentials = credentials)

        return service
    
    def res_to_df(self, res):
        """
        Function to convert the results from GA into a pandas DataFrame.

        :param res: A dictionary with the results from GA.
        :type res: dict
        """

        # Get the dictionary report with the data
        report = res['reports'][0]
        # Gather the dimensions and metris and assign them to the headers of the table
        dimensions = report['columnHeader']['dimensions']
        metrics = [m['name'] for m in report['columnHeader']['metricHeader']['metricHeaderEntries']]
        headers = [*dimensions, *metrics]
        
        # Gather the data and convert them to the rows of the table
        data_rows = report['data']['rows']
        data = []
        for row in data_rows:
            data.append([*row['dimensions'], *row['metrics'][0]['values']])
        
        return pd.DataFrame(data = data, columns = headers)

    def get_data_GA(self, view_id, start_date, end_date, metrics_input, dimensions_input, segments_input = [], filters_input = ''):
        """
        Function to get the final data from GA.

        :param view_id: The ID of the GA view.
        :type view_id: string
        :param start_date: The start date from which the data has to be downloaded, in the format YYYY-MM-DD.
        :type start_date: string
        :param end_date: Same but for the end date.
        :type end_date: string
        :param metrics_input: The metrics to be downloaded.
        :type metrics: list
        :param dimensions_input: The dimensions that have to appear in the table (date, city...).
        :type dimensions_input: list
        :param segments_input: The segments to be applied to the metrics.
        :type segments_input: list
        :param filters_input: The filter to be applied to the metrics.
        :type filters_input: string
        """

        # Initialize the service
        service = self.get_service_GA()

        # Download the raw data as a dictionary
        response = service.reports().batchGet(
                    body={
                        'reportRequests': [{
                            'viewId': view_id,
                            'dateRanges': [{'startDate': start_date, 'endDate': end_date}],
                            'metrics': [{'expression': 'ga:' + m} for m in metrics_input],
                            'dimensions': [{'name': 'ga:' + d} for d in dimensions_input],
                            'segments': [{'segmentId': s} for s in segments_input],
                            'dimensionFilterClauses': filters_input
                        }]
                    }).execute()

        # Convert data to Pandas DataFrame
        data = self.res_to_df(response)

        # The date is in format YYYYMMDD, it must be in format YYYY-MM-DD
        data['ga:date'] = data['ga:date'].apply(lambda x: x[:4] + '-' + x[4:6] + '-' + x[6:])
        
        # Data can be downloaded as an string while they are a number. They have to be
        # converted into float or integer, depending on the type o number
        for col in ['ga:' + m for m in metrics_input]:
            if data[col].dtype == 'object':
                if data[col].str.contains('\.').any():
                    data[col] = data[col].astype(float)
                else:
                    data[col] = data[col].astype('Int64')

        #data = data.drop(columns = 'ga:segment', axis = 1)

        return data
    
    def logging_bq(self, location = 'EU'):
        """
        Function to log into BQ.

        :param location: The location where the queries are executed.
        :type location: string
        """

        # Set the BQ client
        try:
            client = bigquery.Client.from_service_account_json(self.token_path, location = location)
        except FileNotFoundError:
            client = bigquery.Client(location = location)
        
        return client

    
    def get_data_BQ(self, sql_query_input):
        """
        Function to download data from BQ.

        :param sql_query_input: The sql query to download the specific metric.
        :type sql_query_input: string
        """
        
        # Set the BQ client
        client = self.logging_bq()

        # Download data
        data = client.query(sql_query_input).to_dataframe()

        return data
    
    def upload_historical_alerts(self, project_id, new_hist, env):
        """
        Function to upload historical alerts to BQ.

        :param project_id: Project Id where the data will be saved.
        :type project_id: string
        :param new_hist: The table with the new alerts.
        :type new_hist: pandas DataFrame
        :param env: The environment you are executing the System.
        :type env: string
        """

        # Set the BQ client
        client = self.logging_bq()

        # Set the name of the dataset and table with historical alerts
        dataset_id = 'alerts_historical_dataset_' + env
        table_id = 'alerts_historical_table_' + env

        extra_styles_inst = ExtraStylesBQ()
        extra_styles_methods = [name_method for name_method in dir(extra_styles_inst) if callable(getattr(extra_styles_inst, name_method)) and not '__' in name_method]
        if len(extra_styles_methods) > 1:
                extra_styles_methods.remove('do_nothing')
        for name_method in extra_styles_methods:
                method = getattr(extra_styles_inst, name_method)
                new_hist, schema = method(new_hist)

        # Create the datasets and table refs
        dataset_ref = client.dataset(dataset_id, project = project_id)
        table_ref = dataset_ref.table(table_id)

        # Create the dataset if it does not exist yet
        dataset = bigquery.Dataset(dataset_ref)
        client.create_dataset(dataset, exists_ok = True)

        # Create the table if it does not exist yet
        table = bigquery.Table(table_ref, schema = schema)
        client.create_table(table, exists_ok = True)

        # Convert the pandas DataFrame to dict in order to insert new rows
        rows_to_insert = new_hist.to_dict('records')
        # The rows insertion can fail the first time because there is not enough time to create the table.
        # Then we use a while loop and a try-except clause
        while True:
            try:
                client.insert_rows(table, rows_to_insert)
                break
            except NotFound:
                time.sleep(2)
            except BadRequest:
                break

    def upload_future_pred(self, project_id, future_table, env, date):
        """
        Function to upload historical alerts to BQ.

        :param project_id: Project Id where the data will be saved.
        :type project_id: string
        :param new_hist: The table with the new alerts.
        :type new_hist: pandas DataFrame
        :param env: The environment you are executing the System.
        :type env: string
        :param date: The start date of the predictions.
        :type date: timestamp
        """

        # Set the BQ client
        client = self.logging_bq(location = 'EU')

        # Set the name of the dataset and table with historical alerts
        dataset_id = 'future_pred_dataset_' + env
        table_id = 'future_pred_table_' + env

        # Set bq table name and temp table in order to upsert new predictions
        bq_table_name = project_id + '.' + dataset_id + '.' + table_id
        table_id_temp = 'temp_' + table_id
        bq_table_temp = project_id + '.' + dataset_id + '.' + table_id_temp

        # Add initial date predictions column
        future_table['Date_init'] = date.date()#.strftime('%Y-%m-%d')

        extra_styles_inst = ExtraStylesPred()
        extra_styles_methods = [name_method for name_method in dir(extra_styles_inst) if callable(getattr(extra_styles_inst, name_method)) and not '__' in name_method]
        if len(extra_styles_methods) > 1:
                extra_styles_methods.remove('do_nothing')
        for name_method in extra_styles_methods:
                method = getattr(extra_styles_inst, name_method)
                future_table, schema, query_upsert = method(future_table, bq_table_name, bq_table_temp)

        # Create the datasets and table refs
        dataset_ref = client.dataset(dataset_id, project = project_id)
        table_ref = dataset_ref.table(table_id)

        # Create the dataset if it does not exist yet
        dataset = bigquery.Dataset(dataset_ref)
        client.create_dataset(dataset, exists_ok = True)

        # Create the table if it does not exist yet
        table = bigquery.Table(table_ref, schema = schema)
        client.create_table(table, exists_ok = True)

        # # Consult BQ table and load data in a dataframe
        # bq_table_name = project_id + '.' + dataset_id + '.' + table_id
        # sql = f"SELECT * FROM `{bq_table_name}`"
        # future_table_bq = client.query(sql).to_dataframe()

        # # If there are no rows in the BQ table, then upload a new table and do not do the merge
        # if len(future_table_bq) == 0:
        #      future_table_upload = future_table.copy()
        # else:
        #     future_table_upload = pd.merge(future_table_bq, future_table, on = ['fecha', 'variable', 'producto', 'dispositivo'], how = 'outer')

        # name_prediction_column = 'prediccion_' + date.strftime('%Y_%m_%d')
        # # If the prediction has been executed more than once in the same day, the column is replaced
        # if name_prediction_column in future_table_upload.columns:
        #     future_table_upload = future_table_upload.drop(name_prediction_column, axis = 1)

        # future_table_upload = future_table_upload.rename(columns = {'prediccion': name_prediction_column})


        # Load data in a BQ table
        # job = client.load_table_from_dataframe(future_table, bq_table_name, job_config = bigquery.LoadJobConfig(write_disposition = 'WRITE_APPEND'))
        # job.result()

        # We append new data by doing an upsert because we can execute the same prediction in the same day

        job_config = bigquery.LoadJobConfig(
            write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema = schema,
        )

        job = client.load_table_from_dataframe(future_table, bq_table_temp, job_config = job_config)
        job.result()

        job = client.query(query_upsert)
        job.result()

        client.delete_table(bq_table_temp, not_found_ok = True)
    
    def upload_evaluation_table(self, project_id, env, date):
        """
        Function to upload evaluation metrics for the predictions.

        :param project_id: Project Id where the data will be saved.
        :type project_id: string
        :param env: The environment you are executing the System.
        :type env: string
        :param date: The last date where the evaluation is calculated.
        :type date: timestamp
        """

        # Set the BQ client
        client = self.logging_bq(location = 'EU')

        # Set the dataset id, table id with evaluation metrics and table id with the predictions
        dataset_id = 'future_pred_dataset_' + env
        table_id_ev = 'pred_evaluation_table_' + env
        table_id = 'future_pred_table_' + env

        # Prediction evaluation
        evaluation_table = project_id + '.' + dataset_id + '.' + table_id_ev
        pred_table_name = project_id + '.' + dataset_id + '.' + table_id

        # Set the styles of evaluation metrics table
        extra_styles_inst = ExtraStylesEvaluation()
        extra_styles_methods = [name_method for name_method in dir(extra_styles_inst) if callable(getattr(extra_styles_inst, name_method)) and not '__' in name_method]
        if len(extra_styles_methods) > 1:
                extra_styles_methods.remove('do_nothing')
        for name_method in extra_styles_methods:
                method = getattr(extra_styles_inst, name_method)
                query_evaluation = method(pred_table_name = pred_table_name, date = date.strftime('%Y_%m_%d'))

        # Upload evaluation metrics table
        try:
            job_config = bigquery.QueryJobConfig(destination = evaluation_table, write_disposition = 'WRITE_TRUNCATE')
            job = client.query(query_evaluation, job_config = job_config)
            job.result()
        except NotFound:
            logging.info('Table with predictions not found. Evaluation metrics will not be calculated.')
            pass
