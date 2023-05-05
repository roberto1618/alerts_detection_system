"""
Many times data is downloaded from *BQ* or *GA* with missing values. Then, it is important to apply
the proper techniques in order to fill the gaps. This module does that. The module will be updated
with more techniques as the project progresses.
"""

import numpy as np
import pandas as pd
import logging

class Preprocess():
    """
    This class is to impute missing data.
    """
    def fill_moving_avg(self, data):
        """
        Function to apply moving average to fill missing data.

        :param data: Data with the metrics.
        :type data: Pandas DataFrame
        """

        # Save the metrics in a list and discard date from missing data imputation
        metrics = data.drop('date', axis = 1).columns

        # Impute missing data for every column. Every column is a time series
        for m in metrics:
            # Implement the moving average
            data['moving_avg'] = data[m].rolling(window = 7, min_periods = 1).mean()
            # If the variable has integer values, then we round the moving average to integers
            data['moving_avg'] = data['moving_avg'].fillna(0)
            if data[m].dtypes == 'Int64':
                data['moving_avg'] = data['moving_avg'].round().astype(int)
            # If the last value is NA, is preferable not to fill it because the System would send a wrong real value
            # Then, we assign it to zero (equal to no data)
            #if pd.isna(data.iloc[-1, data.columns.get_loc(m)]):
            #    data.iloc[-1, data.columns.get_loc(m)] = 0
            data[m] = np.where(data[m].isna(), data['moving_avg'], data[m])
            # If you replace missing data, it could have integer values mixed with float. So all of them are converted to float
            #data[m] = data[m].astype(float)
            data = data.drop('moving_avg', axis = 1)

        return data
    
    def remove_outliers(self, data):
        """
        Function to remove outliers from the time series.

        :param data: Data with the metrics.
        :type data: Pandas DataFrame
        """
        # Save the metrics in a list and discard date from missing data imputation
        metrics = data.drop('date', axis = 1).columns

        for m in metrics:
            # Calculate mean and std for every metric
            mean = np.mean(data[m])
            std = np.std(data[m])

            # Set the limits to detect outliers
            lower_limit = mean - 3*std
            upper_limit = mean + 3*std
    
            # Every outlier data is converted to NA, except the last row which corresponds to yesterday's actual data
            condition = (data[m] < lower_limit) | (data[m] > upper_limit)
            #aux = data.iloc[:-1]
            #aux.loc[condition, m] = None
            #data = pd.concat([aux, data.iloc[-1:]])
            data.loc[condition, m] = None
        return data
