import datetime
import statistics
from datetime import datetime

import pandas as pd
import scipy.stats as stats


class Scorer:
    def __init__(self):
        pass

    def get_diagnosis_dataframe(self):
        cols = [
            'stage_id',
            'attempt',
            'stage_duration_seconds',
            'input_records',
            'input_size',
            'output_records',
            'output_size',
            'shuffle_write_size',
            'shuffle_write_records',
            'stage_description',
            'stage_status',
            'associated_job_id',
            'associated_sql_id',
            'stage_link',
            'job_link',
            'sql_link',
            'gap_score',
            'outlier_score',
            'is_skewed',
            'is_spilled'
        ]
        return pd.DataFrame(columns=cols, dtype=object)

    def get_notebook_mapping_dataframe(self):
        cols = [
            'associated_job_id',
            'notebook_dir',
            'notebook_url',
            'job_description',
            'command_text'
        ]
        return pd.DataFrame(columns=cols, dtype=object)

    def get_summary_dataframe(self):
        cols = [
            'stage_id',
            'stage_link',
            'sql_link',
            'sql_steps',
            'stage_description',
            'issues_summary',
            'severity_score'
        ]
        return pd.DataFrame(columns=cols, dtype=object)

    def get_date_candidates(self, input_date):
        """
        Only accepts timestamp unix timestamp in seconds
        """
        final_date_format = '%Y-%m-%d'
        original_date = datetime.datetime.fromisoformat(input_date.replace("GMT", ""))
        date_plus_one_day = original_date + datetime.timedelta(days=1)
        return [original_date.strftime(final_date_format), date_plus_one_day.strftime(final_date_format)]

    def get_min_max_gap(self, values, denominator=60 * 60):
        """
        Params:
          values (list) : List of values to determine gap (stage duration or memory used)
        Returns:
          gap score (float) :
          Higher value signifies bigger skew / imbalance.
          Considers both the absolute and relative discrepencies between the max and the min. Can also be thought of as the gap difference in minutes, but normalized

          ### Comparing same relative discrepency but different absolute discrepency ###
          i.e. (max) 1 hour and (min) 30 mins would give: (1 - (30/120)) * ((120-30)/60)  = 1.13  << Gives higher score because gap is 60 minutes, even if ratio of min : max is 1:4
          i.e. (max) 40 minute and (min) 10 minute would give: (1 - (10/40)) * ((40-10)/60) = 0.38 << absolute gap is only 30 minutes, even if ratio of min : max is 1:4

          ### Comparing different relative discrepency but same absolute discrepency ###
          i.e. (max) 10 minute and (min) 9 minute would give : (1 - (9/10)) * (10-9)/60 = 0.0016  << Higher score because ratio is 1:9
          i.e. (max) 100 minute and (min) 99 minute would give : (1 - (99/100)) * (100-99)/60 = 0.00016 << Lower score because ratio is 1:99
        """
        if max(values) < 120:
            return 0
        med_val = statistics.median(values)
        max_val = max(values)
        return (1 - (med_val / max_val)) * ((max_val - med_val) / denominator)

    def detect_zeroes(self, values):
        """
        If zeroes exists, returns true
        """
        return 0 in values

    def detect_outlier_ratio(self, values, sd_threshold=1.5):
        """
        Returns true if ratio of outliers (defined by exceeding 1.5 s.d. from the mean) exceeds 5% and more than 2 minutes. Else return 0
        """
        if max(values) < 120:
            return 0
        z_scores = stats.zscore(values)
        z_score_flagged = len([[z, t] for z, t in zip(z_scores, values) if abs(z) > sd_threshold])
        outliers_ratio = z_score_flagged / len(values)
        return outliers_ratio

    def detect_skew(self, task_df):
        """
        Runs series of tests. To add on more tests for skew.
        """
        if task_df is None or task_df.shape[0] == 0:
            return {'raw_scores': [0, 0], 'decision_scores': [False, False]}

        values = task_df['executorRunTime'].values / 1000
        gap_score = self.get_min_max_gap(values)
        outlier_score = self.detect_outlier_ratio(values)

        has_gap = gap_score > 0.2
        has_outliers = outlier_score > 0.2
        return {'raw_scores': [gap_score, outlier_score], 'decision_scores': [has_gap, has_outliers]}

    def detect_spill(self, df):
        if df is None or df.shape[0] == 0:
            return False
        if df[(df['memoryBytesSpilled'] > 0) | (df['diskBytesSpilled'] > 0)].shape[0] > 0:
            return True
        return False
