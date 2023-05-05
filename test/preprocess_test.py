from ..modules.preprocess import Preprocess
import unittest
import pandas as pd
import numpy as np

class TestPreprocess(unittest.TestCase):

    def test_fill_moving_avg(self):
        # Create sample data with missing values
        data = pd.DataFrame({
            'date': pd.date_range('2022-01-01', '2022-01-10'),
            'metric1': [1, 2, np.nan, 4, 5, np.nan, np.nan, 8, 9, 10],
            'metric2': [11, 12, 13, np.nan, 15, np.nan, np.nan, np.nan, 19, 20]
        })
        
        # Create expected output
        expected_output = pd.DataFrame({
            'date': pd.date_range('2022-01-01', '2022-01-10'),
            'metric1': [1, 2, 2, 4, 5, 4, 4, 8, 9, 10],
            'metric2': [11, 12, 13, 13, 15, 15, 15, 15, 19, 20]
        })
        
        # Create Preprocess object and fill missing values using moving average
        preprocess = Preprocess()
        output = preprocess.fill_moving_avg(data)
        
        # Check that output matches expected output
        self.assertTrue(expected_output.equals(output))


if __name__ == '__main__':
    unittest.main()