"""
This library uses a time series model in order to predict the alerts. Specifically, it uses a Prophet model and
it is prepared to model daily data. Principally, we can summarize the process as follows:

- First, each metric that is introduced in the config is modeled (the stationarity and the seasonality) until the day before yesterday.
- After that, yesterday's data is predicted along with its confidence interval.
- Yesterday's actual data is compared with the confidence interval. If it is below or above any limit, then an alert is detected.
"""

from prophet import Prophet
from config.alerts_table_styles.extra_styles import ExtraStylesPreAlerts
import pandas as pd
import matplotlib.pyplot as plt
import logging
import datetime
from tqdm import tqdm

import statsmodels.api as sm
from pmdarima.arima import auto_arima

class AlertDetector():
    """
    This class has the corresponding methods to get the final table with the alerts summary
    for the metrics downloaded from *GA*/*BQ*.
    """

    def __init__(self, data, plot, date):
        """
        Init function.

        :param data: The data with the metrics.
        :type data: Pandas DataFrame
        :param plot: Set to True to plot a time series Prophet graph. It will show a graph for each metric.
        :type plot: boolean
        :param date: The date when the alerts System is evaluating the alert.
        :type date: timestamp
        """

        self.data = data
        self.plot = plot
        self.date = date

    def get_prediction_PROPHET(self, metric, confidence_interval = 95, seasonality_mode = 'multiplicative', change_prior = 0.5):
        """
        Function to get the predictions table given a single metric.
        It uses a time series model based on Prophet library.

        :param metric: The metric to analyze.
        :type metric: string
        :param confidence_interval: The confidence level to build the confidence interval. An integer between 1-99.
        :type confidence_interval: integer
        :param seasonality_model: The type of seasonality. It can be 'additive' or 'multiplicative'.
        :type seasonality_model: string
        :param change_prior: The changepoint_prior_scale parameter. Higher values return more sensibility in the changes of the time series.
        :type change_prior: float
        """
        
        # Data must have columns renamed as this for Prophet model
        data = self.data.rename(columns = {'date': 'ds', metric: 'y'})[['ds', 'y']]

        # Build model, this can be parametrized to fit better to the metrics
        model = Prophet(interval_width = confidence_interval/100,
                        seasonality_mode = seasonality_mode,
                        seasonality_prior_scale = 10.0,
                        #weekly_seasonality = True,
                        changepoint_prior_scale = change_prior)
        
        # model.add_seasonality(name = 'yearly', period = 365.25, fourier_order = 3)
        # model.add_seasonality(name = 'monthly', period = 30.5, fourier_order = 3)
        model.add_seasonality(name = 'weekly', period = 7, fourier_order = 3)

        # Train the model
        model.fit(data)

        # We want the prediction for the rest of the month
        remaining_days, _ = self.calculate_days_remaining_month()

        table_for_predictions = model.make_future_dataframe(periods = remaining_days)

        # Make the predictions for yesterday
        pred_table = model.predict(table_for_predictions)

        #pred_table.to_csv('./tables_test/pred_table_' + metric + '.csv', decimal = ',')
                          
        # Plot time series
        if self.plot:
            fig = model.plot(pred_table)
            plt.show()

        return pred_table
    
    def get_prediction_ARIMA(self, metric):
        """
        Function to get the predictions table given a single metric.
        It uses a time series model based on SARIMAX model.

        :param metric: The metric to analyze.
        :type metric: string
        """
        
        # Rename columns
        data = self.data.rename(columns = {'date': 'ds', metric: 'y'})[['ds', 'y']]

        # Change ds to datetime datatype
        data['ds'] = pd.to_datetime(data['ds'])

        # The date has to be in the index
        data.set_index('ds', inplace = True)

        # Create the model with the corresponding hyperparams
        model = auto_arima(data,
                           seasonal = True,
                           m = 7,
                           trend = 'ct')

        # Fit the model
        model.fit(data)

        # We want the prediction for the rest of the month
        remaining_days, _ = self.calculate_days_remaining_month()

        # Get the prediction and confidence intervals
        forecast, conf_int = model.predict(n_periods = remaining_days, return_conf_int = True)

        # Create the final pred table with the predictions and confidence intervals
        pred_table = pd.DataFrame({'ds': forecast.index, 'yhat': forecast, 'yhat_lower': conf_int[:, 0], 'yhat_upper': conf_int[:, 1]})

        # Plot time series
        if self.plot:
            plt.plot(data, label = 'Real data')
            plt.plot(model.predict_in_sample(), label = 'Model Adjust')
            plt.plot(forecast, label = 'Prediction')
            plt.legend()
            plt.show()

        return pred_table

    def get_alerts_table(self, real_value_table, model_params, future_pred = False, limsup_alert = False):
        """
        Function to extract the info from the predictions table and create a new
        table with the summary of the alerts for every single metric. When the
        actual yesterday's data is below or above the limits of the confidence interval,
        an alert will exist.

        :param real_value_table: The yesterday's real value table.
        :type real_value_table: Pandas DataFrame
        :param model_params: Table with all model params for every metric.
        :type model_params: Pandas DataFrame
        :param future_pred: True if future predictions must be calculated.
        :type future_pred: boolean
        :param limsup_alert: True if alerts above limsup must be detected.
        :type limsup_alert: boolean
        """

        # Save the metrics in a list
        metrics = self.data.drop('date', axis = 1).columns

        # Create an empty DataFrame to concatenate the alerts of every metric
        alerts_table = pd.DataFrame()
        # Create the table with the future preds
        _, last_day_current_month = self.calculate_days_remaining_month()
        # Create range of dates for the future preds
        delta = datetime.timedelta(days = 1)
        init_date = self.date.date()
        dates_list = []
        while init_date <= last_day_current_month:
            dates_list.append(init_date)
            init_date += delta

        #future_table = pd.DataFrame({'date': [x.strftime('%Y-%m-%d') for x in dates_list]})
        future_table = pd.DataFrame(columns = ['Date', 'Metric', 'Prediction'])

        # We build the model and make predictions until the day before yesterday
        
        for m in tqdm(metrics):
            logging.info('Detecting the alerts for ' + m)
            # The column has to have at least two values without nulls
            are_complete_null = self.data[m].isnull().all()
            assert not are_complete_null, 'Error: Your metric ' + m + (' have all values as null.')
            # Save the real data for yesterday
            real = real_value_table[m].iloc[0]
            method = model_params.loc[model_params['kpi'] == m, 'method'].values[0]
            # Evaluate the alerts detection method
            if method == 'prophet' or method == 'arima':
                # Get the predictions for the metric
                if method == 'prophet':
                    confidence_interval = model_params.loc[model_params['kpi'] == m, 'confInt'].values[0]
                    seasonality_mode = model_params.loc[model_params['kpi'] == m, 'seasonMode'].values[0]
                    change_prior = model_params.loc[model_params['kpi'] == m, 'changePrior'].values[0]

                    pred_table = self.get_prediction_PROPHET(m,
                                                             confidence_interval = confidence_interval,
                                                             seasonality_mode = seasonality_mode,
                                                             change_prior = change_prior)
                elif method == 'arima':
                    pred_table = self.get_prediction_ARIMA(m)
                # The last prediction is the yesterday prediction. It is saved in order to compared with real data
                pred = pred_table.loc[pred_table['ds'] == self.date.strftime('%Y-%m-%d'), 'yhat'].values[0]

                # Save the lower and upper limits of confidence interval in order to make the decision (alert or not alert)
                liminf = pred_table.loc[pred_table['ds'] == self.date.strftime('%Y-%m-%d'), 'yhat_lower'].values[0]
                limsup = pred_table.loc[pred_table['ds'] == self.date.strftime('%Y-%m-%d'), 'yhat_upper'].values[0]
                
                # If the variable has __related__ in the method, then apply new treatment to the predictions
                is_related = model_params.loc[model_params['kpi'] == m, 'isRelated'].values[0] == 'True'
                if is_related:
                    extra_styles_inst = ExtraStylesPreAlerts()
                    extra_styles_methods = [name_method for name_method in dir(extra_styles_inst) if callable(getattr(extra_styles_inst, name_method)) and not '__' in name_method]
                    if len(extra_styles_methods) > 1:
                        extra_styles_methods.remove('do_nothing')
                    for name_method in extra_styles_methods:
                        method = getattr(extra_styles_inst, name_method)
                        pred, liminf, limsup = method(alerts_table, m, pred, liminf, limsup)
                
                # We cannot have negative predictions
                pred = 0.0 if pred < 0 else pred

                # We cannot have negative lims and if pred is 0, then liminf also must be zero
                liminf = 0.0 if liminf < 0 or pred == 0 else liminf
                limsup = 0.0 if limsup < 0 else limsup
                
                # Save if it is an alert or not and save the details
                if pd.isna(real):
                    details = 'Missing data.**'
                    is_alert = 1
                    # Convert the real value to -1.0 so that the remove_decimals function can be applied without error
                    real = -1.0
                else:
                    if real < liminf:
                        is_alert = 1
                        details = 'Decreasing tendency.*'
                    elif real > limsup and limsup_alert:
                        is_alert = 1
                        details = 'Increasing tendency.*'
                    else:
                        is_alert = 0
                        details = 'No alert.'

                # Round pred if is an integer variable
                if isinstance(self.data[m].iloc[-1], int):
                    real = int(real)
                    pred = int(round(pred))
                    pred_table['yhat'] = pred_table['yhat'].round().astype(int)
                else:
                    real = round(real, 3)
                    pred = round(pred, 3)
                    print(pred)
            elif method == 'constraint':
                pred = 'No'
                liminf = '-'
                limsup = '-'
                if pd.isna(real):
                    details = 'Missing data.**'
                    is_alert = 1
                    # Convert the real value to -1.0 so that the remove_decimals function can be applied without error
                    real = -1.0
                else:
                    is_alert = real
                    real = 'Yes' if real == 1 else 'No'
                    if is_alert:
                        details = 'Constraint violated.***'
                    else:
                        details = 'No alert.'

            # If we do not want to send an alert for this kpi, then set is_alert to two
            if model_params.loc[model_params['kpi'] == m, 'sendAlert'].values[0] == 'False' and real != -1.0:
                is_alert = 2

            # Remove config strings from metrics
            #m = m.replace('__model__', '').replace('__constraint__', '').replace('prophet__', '').replace('arima__', '').replace('related__', '').replace('_', ' ')
            m = m.replace('_', ' ')

            # Create the alert info row for that metric and append it to the general alerts DataFrame
            single_alert_info = pd.DataFrame({'Metric': [m],
                                              'Alert': [is_alert],
                                              'Prediction': [pred],
                                              'Real': [real],
                                              'LimInf': [liminf],
                                              'LimSup': [limsup],
                                              'Details': [details]})
            alerts_table = pd.concat([alerts_table, single_alert_info], ignore_index = True)
            
            ## Concat predictions to future_table
            pred_table['ds'] = pd.to_datetime(pred_table['ds']).dt.date
            # We want only rows from yesterday to the end of the month
            concat_table = pred_table.loc[pred_table['ds'] >= self.date.date(), ['ds', 'yhat']].reset_index(drop = True)
            # Every negative value must be zero
            concat_table.loc[concat_table['yhat'] < 0, 'yhat'] = 0
            # Rename the columns and concat tables
            concat_table = concat_table.rename(columns = {'ds': 'Date', 'yhat': 'Prediction'})
            concat_table['Metric'] = m

            future_table = future_table.append(concat_table, ignore_index = True)
        
        # Remove or keep the decimals depending on the metric type
        # In order to have different types of numbers in the same column, the columns must be converted to string
        alerts_table['Prediction'] = alerts_table['Prediction'].astype(str).apply(self.remove_decimals)
        alerts_table['Real'] = alerts_table['Real'].astype(str).apply(self.remove_decimals)

        # Every -1 is set as 'No data'
        alerts_table.loc[alerts_table['Real'] == '-1', 'Real'] = 'No data'

        if not future_pred:
            future_table = None

        return alerts_table, future_table

    def remove_decimals(self, number):
        """
        Function to remove decimals from metrics that should not have them.
        They must have been converted to string before.
        
        :param number: The number to remove the decimals.
        :type number: string
        """

        # Split the number in two parts: before and after the decimal point
        number_split = number.split('.')
        # Sum all decimals
        try:
            decimals_sum = sum(int(d) for d in number_split[1])
        except IndexError:
            return number
        # Check if sum of decimals is zero. If it is, then the number can be converted to integer
        if decimals_sum == 0:
            return number_split[0]
        else:
            return number
        
    def calculate_days_remaining_month(self):
        """
        This function calculates the remaining days until the end of the month that correspond to the date.
        """
        # Obtain first day of the next month
        first_day_next_month = datetime.date(self.date.year, self.date.month, 1) + datetime.timedelta(days = 31)

        # Obtain last day of current month
        last_day_current_month = first_day_next_month - datetime.timedelta(days = first_day_next_month.day)

        # Obtain remaining days until the end of the current month
        remaining_days = (last_day_current_month - self.date.date()).days + 1

        return remaining_days, last_day_current_month