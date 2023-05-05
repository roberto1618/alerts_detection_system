from google.cloud import bigquery

class ExtraStylesPreAlerts:
    """
    This class is used for recalculating the predictions and confidence intervals for specific metrics.
    """
    
    def do_nothing(self, alerts_table, metric, pred, liminf, limsup):
        """
        Function to literally do nothing to the prediction and confidence intervals.

        :param alerts_table: The alerts info table that you have calculated with alerts and non-alerts.
        :type alerts_table: Pandas DataFrame
        :param metric: The metric prediction and confidence intervals you want to modify.
        :type metric: string
        :param pred: The original prediction of the metric.
        :type pred: float, int
        :param liminf: The original liminf of the metric's prediction.
        :type liminf: float
        :param liminf: The original limsup of the metric's prediction.
        :type liminf: float
        """

        return pred, liminf, limsup

class ExtraStylesPreAlerts:
    
    def do_nothing(self, alerts_table, metric, pred, liminf, limsup):
        """
        Function to literally do nothing to the prediction and confidence intervals.

        :param alerts_table: The alerts info table that you have calculated with alerts and non-alerts.
        :type alerts_table: Pandas DataFrame
        :param metric: The metric prediction and confidence intervals you want to modify.
        :type metric: string
        :param pred: The original prediction of the metric.
        :type pred: float, int
        :param liminf: The original liminf of the metric's prediction.
        :type liminf: float
        :param liminf: The original limsup of the metric's prediction.
        :type liminf: float
        """

        return pred, liminf, limsup

class ExtraStylesAlerts:
    """
    This class edits the style of the alerts table that is going to appear in the email.
    You can add all the methods you want with a single parameter "table". A new html table will be returned.
    If you do not add any extra module, then the "do_nothing" module will be executed and the table will be uploaded in the original style.
    """

    def do_nothing(self, table):
        """
        Function to literally do nothing to the style and schema of the table.

        :param table: The table to apply the style.
        :type table: Pandas DataFrame
        """
        
        table_html = table.to_html(index = False)
        return table_html

class ExtraStylesBQ:
    """
    This class edits the style of the BQ alerts table that is going to be uploaded.
    You can add all the methods you want with a single parameter "table". A new table with the schema you have created will returned.
    If you do not add any extra module, then the "do_nothing" module will be executed and the table will be uploaded in the original style.
    """
    def do_nothing(self, table):
        """
        Function to literally do nothing to the style and schema of the table.

        :param table: The table to apply the style.
        :type table: Pandas DataFrame
        """

        schema = [
            bigquery.SchemaField('Metric', 'STRING'),
            bigquery.SchemaField('Prediction', 'FLOAT'),
            bigquery.SchemaField('Real', 'STRING'),
            bigquery.SchemaField('Details', 'STRING'),
            bigquery.SchemaField('Date', 'DATE')
        ]

        return table, schema
    
class ExtraStylesPred:
    """
    This class edits the style of the predictions table.
    You can add all the methods you want with a single parameter "table". A new table with the schema you have created will returned.
    If you do not add any extra module, then the "do_nothing" module will be executed and the table will be uploaded in the original style.
    """

    def do_nothing(self, table):
        """
        Function to literally do nothing to the style and schema of the table.

        :param table: The table to apply the style.
        :type table: Pandas DataFrame
        """
        
        schema = [
            bigquery.SchemaField('Date', 'DATE'),
            bigquery.SchemaField('Metric', 'STRING'),
            bigquery.SchemaField('Prediction', 'FLOAT')
        ]

        return table, schema
    
class ExtraStylesEvaluation:
    """
    This class edits the style of the predictions table.
    You can add all the methods you want. A custom query for the evaluation metrics table will be created.
    If you do not add any extra module, then the "do_nothing" module will be executed and the table will be uploaded in the original style.
    """

    def do_nothing(self, pred_table_name, date, real_table_name = None):
        """
        Function to literally do nothing to the query of evaluation metrics table.

        :param pred_table_name: Table name of BQ table with the predictions.
        :type pred_table_name: string
        :param date: The last date where the evaluation is calculated.
        :type date: timestamp
        :param real_table_name: Table name of BQ table with real data.
        :type real_table_name: string
        """

        query_evaluation = f"""
            WITH max_dates AS (
                SELECT
                    MAX(Date_init) AS max_future_pred_date,
                    (SELECT MAX(PARSE_DATE('%Y%m%d', fecha)) FROM `{real_table_name}`) AS max_real_table_date
                FROM
                    `{pred_table_name}`
                ),
                joined_data AS (
                SELECT
                    t1.Date,
                    t1.Metric,
                    t1.Prediction, 
                    t2.real_value
                FROM
                    `{pred_table_name}` t1
                JOIN
                    {real_table_name} t2
                ON
                    t1.Date = t2.Date AND
                    t1.Metric = t2.Metric
                WHERE
                    fecha_ini = (
                        SELECT
                            MAX(Date_init) AS max_date_init
                        FROM
                            `{pred_table_name}`
                    )
                ),
                abs_error AS (
                SELECT
                    Metric,
                    CASE
                        WHEN real_value = 0 THEN NULL
                        ELSE ABS(Prediction - real_value) / real_value * 100 
                    END AS error
                FROM
                    joined_data
                )
            SELECT
                Metric,
                AVG(error) AS MAPE
            FROM
                abs_error
            GROUP BY
                Metric;
        """
        return query_evaluation