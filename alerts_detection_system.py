"""
This module is the main script that contains the Alert Detection System's skeleton.
It calls every single module from the folder ``modules``. Therefore, this is the script that
has to be executed by a *shell* script in order to analyze data and send the alerts.

These are the steps this script does:

1. Load all configuration you set up in the ``config`` folder (metrics and general config).
2. Download data from *GA* and/or *BQ* and save the metrics as a *DataFrame*.
3. Create models for every metric with Machine Learning algorithms in order to understand the stationarity and seasonality.
4. Predict what should be your yesterday's data value and compare it with the actual one in order to detect the alerts.
5. Gather the alerts information in a table and send them by email.
"""

## Import libraries
from modules.download_data import DownloadData
from modules.alert_detector import AlertDetector
from modules.preprocess import Preprocess
from modules.email_generator import EmailGenerator
from modules.secret_manager import get_secret
import json
import yaml
import argparse
from datetime import datetime, timedelta
import pandas as pd
import logging
import os
from tqdm import tqdm

## Get the arguments from the terminal command line execution
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = 'Arguments to execute the script',
                                    formatter_class = argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('-e', '--env', dest = 'env', help = 'Set the environment', default = 'dev')
    parser.add_argument('-p', '--plot', dest = 'plot', help = 'Plot time series', default = 'False')
    parser.add_argument('-d', '--past-days', dest = 'past_days', type = int, help = 'Days to subtract from today (1 means executing System for yesterday)', default = 1)
    parser.add_argument('-f', '--future-pred', dest = 'future_pred', help = 'True if the future prediction is sent with the alerts', default = 'False')
    parser.add_argument('-a', '--force-alert', dest = 'force_alert', help = 'True if you want to force an alert in order to receive an alert email', default = 'False')
    parser.add_argument('-v', '--ev-pred', dest = 'ev_pred', help = 'True if you want to evaluate the predictions', default = 'False')
    parser.add_argument('-m', '--send-email', dest = 'send_email', help = 'True if you want to send an email with the alerts', default = 'True')
    args = parser.parse_args()

    config = vars(args)

    env = config['env'].upper()

    ## Configure the logger
    # Create logs folder if it does not exist
    folder = 'logs'

    if not os.path.exists(folder):
        os.mkdir(folder)
        print("Folder created: ", folder)

    now = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
    logging.basicConfig(filename = './logs/' + now + '_' + env + '.log', filemode = 'w', format = '%(asctime)s - %(levelname)s - %(message)s', level = logging.INFO)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # Agrega el StreamHandler al logger
    logger = logging.getLogger()
    logger.addHandler(console_handler)

    logging.info('EXECUTING ALERT DETECTION SYSTEM IN A ' + env + ' ENVIRONMENT')

    ## Read the config files. This config contains the necessary parameters
    ## like the credentials to read data, how to build the model, etc.
    config_file = open('./config/general_config_' + env + '.json')
    config_file = json.load(config_file)

    ## Read the corresponding parameters from config_file
    # metrics = open('./config/metrics_' + env + '.json')
    # metrics = json.load(metrics)

    with open('./config/metrics_' + env + '.yml', 'r') as f:
        metrics = yaml.safe_load(f)

    # Read google project name
    google_project = config_file['googleProject']['project']

    # Set the token paths to download data
    token_path = './credentials/' + config_file['googleProject']['tokenFile']
    project_id = config_file['googleProject']['project']

    # Other parameters
    n_hist_data = config_file['model']['nHistData']
    past_days = config['past_days']
    yesterday = datetime.today() - timedelta(days = past_days)
    year_ago = yesterday - timedelta(days = n_hist_data)

    ## Download data to make the analysis
    # Create an dataframe with dates, we will do a join for every metric we download on the key "date"
    data = pd.DataFrame({'date': [(yesterday - timedelta(days = x)).strftime('%Y-%m-%d') for x in range(n_hist_data)]})

    # Final dataframe with the customized modelling parameters for every metric
    model_params = pd.DataFrame()

    # Download the metrics. It iterates over the metrics info the user has configured. It uses different functions depending
    # on the source of the metric
    downloader = DownloadData(token_path)
    for _, metric_info in tqdm(metrics.items()):
        logging.info('Downloading metric: ' + str(metric_info))
        kpi_names_method = metric_info['kpiNamesMethod']
        if metric_info['source'] == 'GA':
            # Download the metric
            raw_metric = downloader.get_data_GA(view_id = metric_info['viewID'],
                                metrics_input = [metric_info['metric']],
                                dimensions_input = ['date'],
                                #segments_input = [metric_info['segment']],
                                filters_input = [metric_info['filters']],
                                start_date = year_ago.strftime('%Y-%m-%d'),
                                end_date = yesterday.strftime('%Y-%m-%d'))
            
            # Rename the columns in order to have a more informative ones
            metric_rename = (metric_info['viewName'] + '{0}').format(*list(kpi_names_method.keys()))
            raw_metric = raw_metric.rename(columns = {'ga:date': 'date', 'ga:' + metric_info['metric']: metric_rename})

            # Merge the single metric table with the data with all the previous metrics. It will be accumulating
            data = data.merge(raw_metric, on = 'date', how = 'left')
            #print(data)

        if metric_info['source'] == 'BQ':
            # Download the metric
            sql_query_input = metric_info['sqlQuery'].format(*[f"{key}" for key in list(kpi_names_method.keys())], 
                                                            dateName = metric_info['dateName'],
                                                            start_date = year_ago.strftime(metric_info['dateFormat']),
                                                            end_date = yesterday.strftime(metric_info['dateFormat']))
            raw_metric = downloader.get_data_BQ(sql_query_input = sql_query_input)
            #print(raw_metric)
            raw_metric = raw_metric.rename(columns = {metric_info['dateName']: 'date'})

            # Convert the date into str type
            raw_metric['date'] = raw_metric['date'].astype(str)

            # The date is in format YYYYMMDD, it must be in format YYYY-MM-DD
            raw_metric['date'] = raw_metric['date'].apply(lambda x: x[:4] + '-' + x[4:6] + '-' + x[6:])

            # If the data has duplicated dates, the program raise an error
            are_duplicated = raw_metric['date'].duplicated().any()
            try:
                assert not are_duplicated, 'Your query ' + metric_info['sqlQuery'] + ( ' returns duplicated dates. You have to '
                                                                                        'rewrite it in order to obtain an only register per day.')
                data = data.merge(raw_metric, on = 'date', how = 'left')
            except AssertionError:
                logging.warning('Your query ' + metric_info['sqlQuery'] + ( ' returns duplicated dates. '
                                                                           'The metric will not be include in the analysis.'))
            
        # Create a table with model info for every metric
        model_params_aux = []
        for key in kpi_names_method.keys():
            row = {'kpi': key}
            for k, v in kpi_names_method[key].items():
                row[k] = v
            model_params_aux.append(row)
        
        model_params_aux = pd.DataFrame.from_records(model_params_aux)

        model_params = model_params.append(model_params_aux, ignore_index = True)

    print(model_params)
    # Sort by date
    data = data.sort_values(by = 'date', ascending = True)
    print(data)

    # Divide data into model data and yesterday's actual data
    real_value_table = data.iloc[-1:]
    data = data.iloc[:-1]

    ## Imput missing data
    logging.info('Imputing missing data...')
    preprocess = Preprocess()
    data = preprocess.remove_outliers(data)
    data = preprocess.fill_moving_avg(data)

    ## Make the analysis of the metrics and predict the alert
    plot = config['plot'] == 'True'
    alert_detector = AlertDetector(data = data, plot = plot, date = yesterday)

    logging.info('Detecting the alerts with a ML model...')
    future_pred = config['future_pred'] == 'True'
    ev_pred = config['ev_pred'] == 'True'
    limsup_alert = config_file['tableAlerts']['limSupAlert'] == 'True'
    alerts_table, future_table = alert_detector.get_alerts_table(real_value_table, model_params = model_params, future_pred = future_pred, limsup_alert = limsup_alert)

    # Force an alert
    force_alert = config['force_alert'] == 'True'
    if force_alert:
        alerts_table.iloc[0,1] = 1
        alerts_table.iloc[1,1] = 1
        alerts_table.iloc[2,1] = 1
        alerts_table.iloc[3,1] = 1
        alerts_table.iloc[4,1] = 1
    only_alerts_table = alerts_table.loc[alerts_table['Alert'] == 1,['Metric', 'Prediction', 'Real', 'Details']]
    pd.set_option('display.max_rows', None)
    pd.set_option('display.expand_frame_repr', False)
    logging.info(alerts_table)

    ## Save historical alerts in a BQ table
    save_historical = config_file['tableAlerts']['saveHistorical'] == 'True'
    if save_historical:
        logging.info('You chose to save alerts into historical BQ table. Saving them...')
        # We want only the alerts
        new_hist = only_alerts_table.copy()
        new_hist['Date'] = yesterday.strftime('%Y-%m-%d')
        downloader.upload_historical_alerts(project_id = project_id, new_hist = new_hist, env = env)
    
    if future_pred:
        logging.info('You chose to calculate predictions. Uploading to BQ...')
        downloader.upload_future_pred(project_id = project_id, future_table = future_table, env = env, date = yesterday)
    
    if ev_pred:
        logging.info('You chose to evaluate the current predictions. Uploading metrics to BQ...')
        downloader.upload_evaluation_table(project_id = project_id, env = env, date = yesterday)

    ## Send email with alerts information
    send_email = config['send_email'] == 'True'

    # Depending of the sending method, different functions are executed
    if send_email:
        logging.info('You chose to send an email with the alerts. Sending it...')
        # Config some email parameters
        env = env.lower()
        # Get the email info from the Secret Manager of Google Cloud
        email_from = get_secret(google_project, 'email_user_' + env, token_path)
        emails_to = get_secret(google_project, 'email_to_' + env, token_path)
        password = get_secret(google_project, 'email_password_' + env, token_path)
        smtp_server = get_secret(google_project, 'email_smtp_server_' + env, token_path)
        port = get_secret(google_project, 'email_smtp_port_' + env, token_path)
        alerts_table_title = config_file['mail']['alertsTableTitle']
        # Create the email corpus and send the email
        email_generator = EmailGenerator(email_from = email_from, emails_to = emails_to, password = password, smtp_server = smtp_server, port = port)
        msg = email_generator.create_corpus(only_alerts_table = only_alerts_table, date = yesterday, alerts_table_title = alerts_table_title, future_table = future_table)
        email_generator.send_email(msg = msg)
