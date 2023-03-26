import configparser
import itertools
import json
import re
import re as regex
from ast import literal_eval

import pandas as pd
import requests as re
from tqdm import tqdm

from SparkMonkey.scorer.scorer import Scorer
from SparkMonkey.utils.utils import Utils


class SparkMonkey:
    def __init__(self, databricks_host_url):
        self.utils = Utils()
        self.scorer = Scorer()
        self.config = configparser.ConfigParser()
        self.config.read('config/config.cfg')
        print(self.config.sections())
        """
        AUTHENTICATION
        """
        self.token = self.utils.dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
        self.token_header = {'Authorization': f'Bearer {self.token}'}

        """
        API PARAMETERS
        """
        self.cluster_id = self.utils.spark.conf.get("spark.databricks.clusterUsageTags.clusterId")
        self.spark_context_id = self.utils.spark.conf.get("spark.databricks.sparkContextId")
        self.databricks_url = databricks_host_url
        self.org_id = self.utils.spark.conf.get("spark.databricks.clusterUsageTags.orgId")
        self.ui_port = self.utils.spark.sql("set spark.ui.port").collect()[0].value
        self.host_url = self.generate_host_url()

        """
        SPARK UI ENDPOINTS
        """
        endpoint_config = dict(self.config.items('spark_ui_endpoints'))
        self.all_application_endpoint = self.host_url + endpoint_config['all_application_endpoint']
        self.all_executors_endpoint = self.host_url + endpoint_config['all_executors_endpoint']
        self.all_jobs_endpoint = self.host_url + endpoint_config['all_jobs_endpoint']
        self.jobs_details_endpoint = self.host_url + endpoint_config['jobs_details_endpoint']
        self.all_stages_endpoint = self.host_url + endpoint_config['all_stages_endpoint']
        self.stage_attempts_endpoint = self.host_url + endpoint_config['stage_attempts_endpoint']
        self.stage_details_endpoint = self.host_url + endpoint_config['stage_details_endpoint']
        self.task_summary_endpoint = self.host_url + endpoint_config['task_summary_endpoint']
        self.task_list_endpoint = self.host_url + endpoint_config['task_list_endpoint']
        self.sql_details_endpoint = self.host_url + endpoint_config['sql_details_endpoint']
        self.sql_single_details_endpoint = self.host_url + endpoint_config['sql_single_details_endpoint']

        """
        SCHEMAS
        """
        schema_config = dict(self.config.items('response_schema'))
        self.job_df_columns = list(schema_config['job_df_columns'].split(","))
        self.stage_df_columns = list(schema_config['stage_df_columns'].split(","))
        self.stage_details_df_columns = list(schema_config['stage_details_df_columns'].split(","))
        self.task_list_df_columns = list(schema_config['task_list_df_columns'].split(","))
        self.sql_list_columns = list(schema_config['sql_list_columns'].split(","))

        """
        INITIALIZE VARIABLES
        """
        self.diagnosis_df = None
        self.notebook_df = None
        self.app_id = self.get_application_id()
        self.sql_details_map = {}
        self.display_imported_message()

    @staticmethod
    def display_imported_message():
        print(""" You have imported the Spark M🙊nkey module

      To get started: spark_monkey = SparkMonkey(host_url='adb-12345678.9.azuredatabricks.net')
      To start analysis on cluster: spark_monkey.diagnose_cluster()
      To visualize potential issues: spark_monkey.display_summary()
    """)

    def generate_host_url(self):
        host_url = f"https://{self.databricks_url}/driver-proxy-api/o/0/{self.cluster_id}/{self.ui_port}/api/v1/"
        return host_url

    def get_application_id(self):
        response = re.get(self.all_application_endpoint, headers=self.token_header)
        if response.status_code == 200:
            return response.json()[0]['id']

    def get_executors(self):
        response = re.get(self.all_executors_endpoint.format(app_id=self.app_id), headers=self.token_header)
        return response

    def retrieve_diagnostics(self):
        return self.diagnosis_df

    def display_summary(self):
        summary_df = self.retrieve_summary_df()
        if isinstance(summary_df, type(None)):
            print("No issues detected")
            return
        self.display_formatted_summary(summary_df)

    def display_formatted_summary(self, summary_df):
        """
        display in html , formatted output for end user
        """
        self.utils.displayHTML(f"""
    <head>
      {self.utils.retrieve_style_html()}
      {self.utils.retrieve_imports_html()}
    </head>
    <body>
      {self.utils.retrieve_table_html(summary_df)}
    </body>
    """)

    def retrieve_all_jobs(self):
        """
        Retrieves all jobs in the spark application
        """
        response = re.get(self.all_jobs_endpoint.format(app_id=self.app_id), headers=self.token_header)
        if response.status_code == 200:
            job_list = response.json()
            return pd.DataFrame(job_list, columns=self.job_df_columns)

    def retrieve_all_stages(self):
        """
        Retrieves all stages in the spark application
        """
        response = re.get(self.all_stages_endpoint.format(app_id=self.app_id, with_summaries=with_summaries),
                          headers=self.token_header)
        return response

    def retrieve_all_stage_details(self, stage_id, with_summaries=True, raw=False):
        """
        Retrieves a list of attempts for a given stage ID
        """
        response = re.get(
            self.stage_attempts_endpoint.format(app_id=self.app_id, stage_id=stage_id, with_summaries=with_summaries),
            headers=self.token_header)
        if raw:
            return response
        if response.status_code == 200:
            stage_detail_list = response.json()
            return pd.DataFrame(stage_detail_list, columns=self.stage_details_df_columns)

    def retrieve_job_details(self, job_id):
        response = re.get(self.jobs_details_endpoint.format(app_id=self.app_id, job_id=job_id),
                          headers=self.token_header)
        if response.status_code == 200:
            job_detail_list = response.json()
            return job_detail_list

    def retrieve_job_tasks(self, job_id):
        response = self.retrieve_job_details(job_id)
        return response['stageIds']

    def retrieve_task_list(self, stage_id, attempt_id=0):
        """
        Retrieves a specific stage and attempt
        """
        response = re.get(self.task_list_endpoint.format(app_id=self.app_id, stage_id=stage_id, attempt_id=attempt_id),
                          headers=self.token_header)
        if response.status_code == 200:
            try:
                task_detail_list = response.json()
                task_detail_df = pd.DataFrame(task_detail_list, columns=self.task_list_df_columns)
                task_metrics_df = pd.json_normalize(task_detail_df['taskMetrics'].dropna())
                return pd.concat([task_detail_df, task_metrics_df], axis=1)
            except:
                pass

    def retrieve_all_sql(self):
        """
        List all SQL Queries
        """
        sql_list = []
        batch_size = 200
        max_num_jobs = 2000
        for offset in range(0, max_num_jobs, batch_size):
            response = re.get(self.sql_details_endpoint.format(app_id=self.app_id, offset=offset, length=batch_size),
                              headers=self.token_header)
            if response.status_code == 200:
                try:
                    sql_list += response.json()
                except:
                    sql_list += json.loads(response.text + ']')
            else:
                break
        return pd.DataFrame(sql_list, columns=self.sql_list_columns)

    def retrieve_sql_details(self, query_id):
        """
        Retrieves a single SQL Query's Details
        """
        response = re.get(self.sql_single_details_endpoint.format(app_id=self.app_id, execution_id=query_id),
                          headers=self.token_header)
        if response.status_code == 200:
            return response.json()
        return

    def get_associated_sql(self, job_id):
        sql_df = self.retrieve_all_sql()
        sql_df['successJobIds'] = sql_df['successJobIds'].apply(lambda x: literal_eval(str(x)))
        sql_filtered = sql_df[sql_df['successJobIds'].apply(lambda x: job_id in list(x))]
        if sql_filtered.shape[0] > 0:
            return sql_filtered

    def extract_job_variables(self, pd_row):
        """
        Get the pandas row and returns required variables
        """
        return pd_row['jobId'], pd_row['stageIds'], pd_row['submissionTime'], regex.sub("\\s+", " ", str(
            pd_row['description']).replace("...", ""))

    def extract_associated_sql_id(self, job_id):
        """
        Extract sql ID based on the related job
        """
        sql_obj = self.get_associated_sql(job_id)
        if isinstance(sql_obj, type(None)):
            return None
        return sql_obj['id'].iloc[0]

    def extract_stage_variables(self, stage_id):
        """
        Extract required variables based on stage_id
        """
        stage_details = self.retrieve_all_stage_details(stage_id)
        if isinstance(stage_details, type(None)):
            return None
        ## Get the latest run and corresponding attempt ID ##
        stage_details = stage_details.iloc[-1]
        attempt_id = stage_details['attemptId'].item()
        stage_run_time = stage_details['executorRunTime'].item() / 1000
        if stage_details is None or stage_details['status'] != "COMPLETE":
            return
        return [attempt_id, stage_run_time, self.retrieve_task_list(stage_id, attempt_id),
                self.extract_stage_details(stage_details)]

    def retrieve_all_sql_details(self):
        """
        Goes through all 200 sql queries in history server, and maps it to class attribute
        """

        all_sql = self.retrieve_all_sql()['id'].tolist()
        return {sql_id: self.retrieve_sql_details(sql_id) for sql_id in all_sql}

    def retrieve_score_matrix(self, task_df):
        """
        Retrieves a dictionary with different scored heuristics i.e. skew score, spill score
        """
        score_matrix = self.scorer.detect_skew(task_df)
        score_matrix['is_skewed'] = sum(score_matrix['decision_scores']) >= 1
        score_matrix['is_spilled'] = self.scorer.detect_spill(task_df)
        return score_matrix

    def retrieve_spark_urls(self, job_id, stage_id, attempt_id, sql_id):
        """
        Description:
          Retrieve Stage, Job and SQL links to the Spark UI.
        Params:
          job_id (int):  Job ID that corresponds to spark history server
          stage_id (int): Stage ID that corresponds to the Job
          attempt_id (int): Attempt ID of the corresponding Stage
          sql_id (int): SQL ID of the spark history server
        """
        stage_link, job_link, sql_link = None, None, None
        if stage_id:
            stage_link = f"https://{self.databricks_url}/sparkui/{self.cluster_id}/driver-{self.spark_context_id}/stages/stage/?id={stage_id}&attempt={attempt_id}&o={self.org_id}"
        if job_id:
            job_link = f"https://{self.databricks_url}/sparkui/{self.cluster_id}/driver-{self.spark_context_id}/jobs/job/?id={job_id}&o={self.org_id}"
        if sql_id:
            sql_link = self.retrieve_sql_url(sql_id)
        return stage_link, job_link, sql_link

    def retrieve_sql_url(self, sql_id):
        return f"https://{self.databricks_url}/sparkui/{self.cluster_id}/driver-{self.spark_context_id}/SQL/execution/?id={sql_id}&o={self.org_id}"

    def retrieve_table_row_output(self, sql_id, table_name):
        """
        Returns number of rows in the table as mentioned in spark sql
        """
        for node_obj in self.sql_details_map[sql_id]['nodes']:
            if table_name in node_obj['nodeName'].split("Scan parquet")[-1].strip():
                num_rows_extracted = [int(float(m['value'].replace(",", ""))) for m in node_obj['metrics'] if
                                      m['name'] == "number of output rows"]
                if len(num_rows_extracted) > 0:
                    return num_rows_extracted[0]
        return 0

    def extract_filter_columns_from_description(self, sql_step, sql_id):
        """
        Retrieves the contents of pushed and partition filters and evaluate into python list
        Does not include subqueries, CTEs
        """
        extracted_table_name = self.extract_table_name_from_description(sql_step)
        if len(extracted_table_name) == 0 or 'Subquery' in extracted_table_name:
            return ['placeholder_to_ensure_subsequent_step_will_run'], [
                'placeholder_to_ensure_subsequent_step_will_run'], 0

        row_output = self.retrieve_table_row_output(sql_id, extracted_table_name)
        if row_output < 1000000:
            return ['placeholder_to_ensure_subsequent_step_will_run'], [
                'placeholder_to_ensure_subsequent_step_will_run'], 0

        partition_filters, pushed_filters = [], []
        if 'PartitionFilters' in sql_step:
            partition_filters = sql_step.split("PartitionFilters: ", 1)[1].split("\n")[0]
            partition_filters = [partition_filters.split("[", 1)[1].rsplit("]", 1)[0]]
        if 'PushedFilters' in sql_step:
            pushed_filters = sql_step.split("PushedFilters: ", 1)[1].split("\n")[0]
            pushed_filters = [pushed_filters.split("[", 1)[1].rsplit("]", 1)[0]]
        return partition_filters, pushed_filters, row_output

    def extract_table_name_from_description(self, sql_step):
        """
        Extracts table name speicfied in the sql description. Only works if 'Scan parquet' exists in the name
        """
        return sql_step.split("\n")[0].split("Scan parquet")[-1].strip()

    def retrieve_issues_summary(self, duration_in_seconds, is_skewed, is_spilled):
        """
        Summarizes the issues and recommends fixes
        """
        msg_body = f'This stage took {round(duration_in_seconds / 60, 2)} minutes in total' + '\n'
        if is_skewed:
            msg_body += '<br><font color="red">Skew</font> was detected in this stage. Seek help <u><a href="https://adb-5382132230645028.8.azuredatabricks.net/?o=5382132230645028#notebook/552660663360652">here</a></u>' + '\n'
        if is_spilled:
            msg_body += '<br><font color="red">Spill</font> was detected in this stage. Seek help <u><a href="https://adb-5382132230645028.8.azuredatabricks.net/?o=5382132230645028#notebook/552660663360652">here</a></u> ' + '\n'
        return msg_body

    def sql_contains_table_select(self, sql_steps):
        """
        Checks if sql contains a select from an actual table (instead of just reading delta log or something)
        """
        for sql_step in sql_steps.split("\n\n")[1:]:
            # If its scanning a proper table #
            if 'Scan parquet' in sql_step and len(sql_step.split("Scan parquet", 1)[1].split("\n", 1)[0].strip()) > 5:
                return True
        return False

    def extract_all_sql_steps_lst(self, sql_steps):
        """
        Returns a list of sql steps, processed to remove brackets
        """
        if not self.sql_contains_table_select(sql_steps):
            return
        return self.utils.remove_brackets_from_header([sql_step.strip() for sql_step in sql_steps.split("\n\n")[1:]])

    def generate_overlaps(self, query_tracker):
        """
        Given a dictionary of sql_id: [list of sql steps], return a [list of sql IDs, list of common steps between them]
        """
        overlap_map = []

        for L in range(len(query_tracker.keys()) + 1, 1, -1):
            combi_list = itertools.combinations(query_tracker.keys(), L)
            for combi in combi_list:
                set_list = [set(query_tracker[sql_id]) for sql_id in combi]
                sql_id_list = [i for i in list(combi)]
                exist_condition = [1 for sql_id_set in overlap_map if len(set(sql_id_list) - set(sql_id_set[0])) == 0]
                if len(exist_condition) > 0:
                    # If the current sql ids are already a subset in overlap_map, then we skip
                    continue
                if len(set_list) == 0:
                    continue
                overlapping_steps = [i for i in list(set.intersection(*set_list)) if len(i) > 0]
                if len(overlapping_steps) > 5:
                    overlap_map.append([sql_id_list, overlapping_steps])

        return overlap_map

    def query_contains_jobs(self, sql_id):
        """
        Searches the SQL ID to check if it contains spark jobs
        """
        if len(self.sql_details_map[sql_id].get("successJobIds", [])) > 0:
            return True
        return False

    def retrieve_repeated_queries(self):
        """
        Scans the sql queries and tries to find common, repeated queries.
        """
        repeated_queries_df = self.scorer.get_summary_dataframe()
        stage_id, stage_link = None, None
        severity_score = "🙈"
        issues_summary = '<br><font color="red">Repeated query transformations</font> were detected among these stages: {stage_list}. Consider using `.cache` to speed up processing. SQL Link is only showing one sample link'

        query_tracker = {}
        for sql_id, sql_obj in self.sql_details_map.items():
            result = self.extract_all_sql_steps_lst(sql_obj['planDescription'])
            if result and self.query_contains_jobs(sql_id):
                query_tracker[sql_id] = result

        if len(query_tracker) > 20:
            ### SKIP IF TOO MANY QUERIES ###
            return repeated_queries_df
        overlapping_queries = self.generate_overlaps(query_tracker)

        # GET SHORTEST SQL PLAN AND INSERT INTO DF FOR EVERY QUERY
        for sql_id_candidates, sql_set_candidates in overlapping_queries:
            shortest_sql_steps, shortest_sql_id = self.utils.retrieve_shortest_list(query_tracker, sql_id_candidates)
            sql_steps = self.utils.preprocess_steps(
                [sql_step for sql_step in shortest_sql_steps if sql_step in sql_set_candidates])
            sql_link = self.retrieve_sql_url(shortest_sql_id)
            stage_description = self.sql_details_map[shortest_sql_id]['description']
            repeated_queries_df.loc[len(repeated_queries_df)] = [stage_id, stage_link, sql_link, sql_steps,
                                                                 stage_description,
                                                                 issues_summary.format(stage_list=sql_id_candidates),
                                                                 severity_score]

        return repeated_queries_df

    def retrieve_summary_df(self):
        """
        Display Top N problamatic spark stages and relevant details
        TODO: Transform into a weighted severity_score
        """

        ##### SCANNING FROM SKEW/SPILL ######
        issues_heuristic_df = self.diagnosis_df[
            (self.diagnosis_df['is_skewed'] > 0) | (self.diagnosis_df['is_spilled'] > 0)]
        issues_heuristic_df['sql_steps'] = None
        issues_heuristic_df['issues_summary'] = None
        issues_heuristic_df['severity_score'] = None

        if issues_heuristic_df.shape[0] > 0:
            issues_heuristic_df['sql_steps'] = issues_heuristic_df.apply(
                lambda x: self.retrieve_sql_steps(x['associated_sql_id'], x['stage_id']), axis=1)
            issues_heuristic_df['issues_summary'] = issues_heuristic_df.apply(
                lambda x: self.retrieve_issues_summary(x['stage_duration_seconds'], x['is_skewed'], x['is_spilled']),
                axis=1)
            issues_heuristic_df['severity_score'] = "🙈🙈🙈"  # SIMPLE RULE FOR NOW. TO MAKE IT WEIGHTED

        issues_heuristic_df = issues_heuristic_df[
            ['stage_id', 'stage_link', 'sql_link', 'sql_steps', 'stage_description', 'issues_summary',
             'severity_score']]
        ###########################################

        ##### SCANNING FROM SQL - UDFs ############
        ##### TODO: PACKAGE IN ANTOHER METHOD #####
        issues_sql_df = self.scorer.get_summary_dataframe()
        stage_id, stage_link = None, None
        issues_summary = '<br><font color="red">UDFs</font> were detected in this stage. Avoid UDFs where possible. Seek help <u><a href="https://adb-5382132230645028.8.azuredatabricks.net/?o=5382132230645028#notebook/552660663360652">here</a></u>'
        severity_score = "🙈🙈"

        all_sql = self.sql_details_map.keys()
        for sql_id in all_sql:
            sql_link = self.retrieve_sql_url(sql_id)
            sql_steps = self.retrieve_sql_steps_udf(sql_id)
            stage_description = self.sql_details_map[sql_id]['description']
            if sql_steps != "":
                issues_sql_df.loc[len(issues_sql_df)] = [stage_id, stage_link, sql_link, sql_steps, stage_description,
                                                         issues_summary, severity_score]
        #############################################

        ##### SCANNING FROM SQL - FILTERS ############
        ##### TODO: PACKAGE IN ANTOHER METHOD ########
        stage_id, stage_link = None, None
        severity_score = "🙈🙈"

        for sql_id, sql_obj in self.sql_details_map.items():
            sql_link = self.retrieve_sql_url(sql_id)

            if 'AtomicCreateTableAsSelect' in self.sql_details_map[sql_id]['planDescription']:
                continue

            for sql_step in self.sql_details_map[sql_id]['planDescription'].split("\n\n")[1:]:
                if 'Scan parquet' in sql_step:
                    partition_filters, pushed_filters, row_output = self.extract_filter_columns_from_description(
                        sql_step, sql_id)
                    extracted_table = self.extract_table_name_from_description(sql_step)
                    stage_description = self.sql_details_map[sql_id]['description']
                    filter_message = ""
                    if len(partition_filters) == 0:
                        filter_message += "Partition filters"
                    if len(pushed_filters) == 0:
                        if len(filter_message) == 0:
                            filter_message = "Pushed filters"
                        else:
                            filter_message += " & Pushed filters"
                    if len(filter_message) > 0:
                        issues_summary = f'<br><font color="red">No {filter_message}</font> were applied when reading <font color="red">{row_output}</font> rows from table {extracted_table}. Filter only the data you need, wherever possible.'
                        issues_sql_df.loc[len(issues_sql_df)] = [stage_id, stage_link, sql_link, sql_steps,
                                                                 stage_description, issues_summary, severity_score]
        ###########################################
        ##### SCANNING FROM SQL - REPEATED QUERIES ############
        #     if spark.conf.get("spark.databricks.clusterSource") == "JOB":
        repeated_queries_df = self.retrieve_repeated_queries()
        #     else:
        #       print("Skipped repeated query check because its running on interactive cluster, where same cells get executed multiple times")
        ###########################################
        issues_detected_df = pd.concat([issues_heuristic_df, issues_sql_df, repeated_queries_df])
        return issues_detected_df

    def extract_stage_details(self, stage_details):
        """
        Returns variables required in stage_details dataframe
        """
        return [
            stage_details['executorRunTime'] / 1000,
            stage_details['inputRecords'],
            stage_details['inputBytes'],
            stage_details['outputRecords'],
            stage_details['outputBytes'],
            stage_details['shuffleWriteBytes'],
            stage_details['shuffleWriteRecords'],
            stage_details['description'],
            stage_details['status']
        ]

    def retrieve_wscg_stage_map(self, sql_id):
        """
        Asusmption is that 1 codegen ID -> 1 stage ID
        """
        wscg_to_stage = {}
        stage_metrics_map = {}
        for node_obj in self.sql_details_map[sql_id]['nodes']:
            for metric_obj in node_obj['metrics']:
                if 'stageId' in metric_obj['value'] and 'stage ' in metric_obj['value']:
                    stage_id = self.utils.retrieve_stage_id(metric_obj['value'])
                    stage_metrics_map[int(float(stage_id))] = stage_metrics_map.get(int(float(stage_id)), []) + [
                        node_obj]

            if 'WholeStageCodegen' not in node_obj['nodeName'] or 'stageId' not in node_obj['metrics'][0]['value']:
                continue
            wscg_id = node_obj['nodeName'].replace("WholeStageCodegen (", "").replace(")", "")

            #### Mapping for stage to codegen id ####
            wscg_to_stage[wscg_id] = wscg_to_stage.get(wscg_id, []) + [int(float(stage_id))]

        return wscg_to_stage, stage_metrics_map

    def retrieve_stage_sql_step_map(self, sql_id):
        """
        Description:
          Algorithm is to go through each SQL nodes, and for each node we retrieve corresponding codegen ID -> stage ID
          Then parse the query plan to retrieve the sql steps based on the codegen ID
        """
        wscg_to_stage, stage_metrics_map = self.retrieve_wscg_stage_map(sql_id)
        stage_to_sql_step_map = {}
        for wscg, stage_id in wscg_to_stage.items():
            stage_id = stage_id[0]
            for sql_step in self.sql_details_map[sql_id]['planDescription'].split("\n\n")[1:]:
                if sql_step.split("[codegen id : ")[-1].split("]")[0] == wscg:
                    stage_to_sql_step_map[stage_id] = stage_to_sql_step_map.get(stage_id, []) + [sql_step]
        return stage_to_sql_step_map, stage_metrics_map

    def retrieve_sql_steps_udf(self, sql_id):
        """
        returns sql steps but regardless of prescence of stage IDs
        """
        sql_dets = self.sql_details_map[sql_id]
        if not sql_dets:
            return ""
        step_lst = [sql_step for sql_step in sql_dets['planDescription'].split("\n\n")[1:] if
                    "BatchEvalPython" in sql_step]
        node_lst = [node_obj for node_obj in sql_dets['nodes'] if node_obj['nodeName'] == "BatchEvalPython"]
        return self.utils.preprocess_steps(step_lst, node_lst)

    def retrieve_sql_steps(self, sql_id, stage_id):
        """
        returns sql steps as visualized in spark ui, but removing hashtags and brackets where applicable
        """
        if sql_id is None:
            return
        stage_to_sql_step, stage_metrics_map = self.retrieve_stage_sql_step_map(sql_id)
        if stage_id not in stage_to_sql_step:
            return ""
        raw_steps = stage_to_sql_step[stage_id]
        stage_metrics = stage_metrics_map[stage_id]
        return self.utils.preprocess_steps(raw_steps, stage_metrics)

    def recommend_shuffle_partition(self, stage_id):
        """
        Calculates the a suggested number of shuffle partitions based on the stage's total tasks size divided by 200 mb. This is meant for experimentation and not in production pipelines.
        Params:
          stage_id (int): stage_id of the bottleneck stage
        """

        stage_obj = self.retrieve_all_stage_details(stage_id, raw=True)
        total_num_tasks = stage_obj.json()[0]['numCompleteTasks']
        #     median_task_size = stage_obj.json()[0]['taskMetricsDistributions']['inputMetrics']['bytesRead'][2]
        total_task_size = stage_obj.json()[0]['inputBytes'] * 0.000001
        rec_partitions = round(total_task_size // 200)

        print(f'Try setting the following: spark.conf.set("spark.sql.shuffle.partitions", {rec_partitions})')
        return rec_partitions

    def diagnose_cluster(self):
        """
        Logic for retrieving the stage details and returning a dataframe with the aggregated metrics and skew/spill detection scores
        """

        self.sql_details_map = self.retrieve_all_sql_details()
        self.diagnosis_df = self.scorer.get_diagnosis_dataframe()

        ### ITERATING THORUGH JOBS ###
        all_jobs_df = self.retrieve_all_jobs()
        for ind, row in tqdm(all_jobs_df.iterrows(), total=all_jobs_df.shape[0], desc='Diagnosing'):
            job_id, stage_ids, job_submission_time, search_txt = self.extract_job_variables(row)
            sql_id = self.extract_associated_sql_id(job_id)

            ### ITERATING THROUGH STAGES ###
            for stage_id in stage_ids:
                stage_var = self.extract_stage_variables(stage_id)
                if not stage_var:
                    continue
                attempt_id, stage_run_time, task_df, stage_detail_variables = stage_var
                score_matrix = self.retrieve_score_matrix(task_df)
                spark_history_urls = self.retrieve_spark_urls(job_id, stage_id, attempt_id, sql_id)
                try:
                    self.diagnosis_df.loc[len(self.diagnosis_df)] = [stage_id, attempt_id, *stage_detail_variables,
                                                                     job_id, sql_id, *spark_history_urls,
                                                                     *score_matrix['raw_scores'],
                                                                     score_matrix['is_skewed'],
                                                                     score_matrix['is_spilled']]
                except:
                    pass
