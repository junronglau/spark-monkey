import os
import re as regex
import sys
from pkgutil import get_loader

import IPython as ip
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession


class Utils:
    def __init__(self):
        self.spark = self._get_spark()
        self.dbutils = self.get_dbutils()
        self.displayHTML = self.get_displayHTML()

    @staticmethod
    def _get_dbutils(spark: SparkSession):
        """
            get databricks utils
            Args:
                spark (spark session): instantiated spark session
            Returns:
                temp dir (str): temp dir
        """
        try:
            if "dbutils" not in globals():
                utils = DBUtils(spark)
                return utils
            else:
                return globals().get("dbutils")
        except ImportError:
            return None

    @staticmethod
    def _get_displayHTML():

        if "displayHTML" not in globals():
            return ip.get_ipython().user_ns.get("displayHTML")
        else:
            return globals().get("displayHTML")

    @staticmethod
    def _get_spark():
        spark = SparkSession.builder.getOrCreate()
        return spark

    def get_dbutils(self):
        return self._get_dbutils(self.spark)

    def get_data_smart(package, resource, as_string=False):
        """Rewrite of pkgutil.get_data() that actually lets the user determine if data should
        be returned read into memory (aka as_string=True) or just return the file path.
        """

        loader = get_loader(package)
        if loader is None or not hasattr(loader, 'get_data'):
            return None
        mod = sys.modules.get(package) or loader.load_module(package)
        if mod is None or not hasattr(mod, '__file__'):
            return None

        # Modify the resource name to be compatible with the loader.get_data
        # signature - an os.path format "filename" starting with the dirname of
        # the package's __file__
        parts = resource.split('/')
        parts.insert(0, os.path.dirname(mod.__file__))
        resource_name = os.path.join(*parts)
        if as_string:
            return loader.get_data(resource_name)
        else:
            return resource_name

    def get_displayHTML(self):
        return self._get_displayHTML()

    def retrieve_stage_id(self, txt):
        return txt.split("stage ")[-1].split(":")[0]

    def retrieve_shortest_list(self, dict_map, list_of_keys):
        """
        returns the shortest object in the dictionary, takes in a list of dictionary keys
        """
        shortest_length = float("inf")
        shortest_key = None
        for dict_key in list_of_keys:
            if len(dict_map[dict_key]) < shortest_length:
                shortest_length = len(dict_map[dict_key])
                shortest_key = dict_key
        return dict_map[shortest_key], shortest_key

    def remove_brackets_from_header(self, list_of_steps):
        remove_sq_brackets = r'\[.*?\]'
        remove_rd_brackets = r'\(.*?\)'
        remove_hashtags = r'\#.*?(?=\s+|,+|\])'
        processed_steps = []
        for ind, text in enumerate(list_of_steps):
            header = text.split("\n")[0]
            body = text.split("\n")[1:]
            header = regex.sub(remove_sq_brackets, '', header)
            header = regex.sub(remove_rd_brackets, '', header)
            header = header.strip()
            body = [regex.sub(remove_hashtags, '', body_txt) for body_txt in body]
            processed_text = [header] + body
            final_text = '\n'.join(processed_text)
            processed_steps += [final_text]

        return processed_steps

    def preprocess_steps(self, list_of_steps, stage_metrics=None):
        """
        removes the unnecessary brackets, hashtags
        """
        ### DEFINE REGEX PATTERNS ###
        remove_sq_brackets = r'\[.*?\]'
        remove_rd_brackets = r'\(.*?\)'
        remove_hashtags = r'\#.*?(?=\s+|,+|\])'
        #############################

        final_text = ""
        for ind, text in enumerate(list_of_steps):
            header = text.split("\n")[0]
            body = text.split("\n")[1:]

            header = regex.sub(remove_sq_brackets, '', header)
            header = regex.sub(remove_rd_brackets, '', header)
            header = header.strip()
            if stage_metrics:
                header = f'[Step {ind + 1}]' + header + '<br><font color="red">' + self.lookup_metrics(header,
                                                                                                       stage_metrics) + '</font>'

            body = [regex.sub(remove_hashtags, '', body_txt) for body_txt in body]
            processed_text = [header] + body
            final_text += '\n'.join(processed_text) + '\n\n'

        return final_text.rstrip()

    def lookup_metrics(self, node_name, stage_metrics):
        """
        im getting desperate at this point so code is messy
        """
        result_text = ""
        for stage_obj in stage_metrics:
            if node_name in stage_obj['nodeName']:
                for metric_obj in stage_obj['metrics']:
                    if self.is_valid_metric(metric_obj['name']) and metric_obj[
                        'value'] not in result_text:  ## TEMP SOLN TO PREVENT DUPS
                        result_text += metric_obj['name'] + ":" + metric_obj['value'] + "\n"
        return result_text.rstrip()

    def is_valid_metric(self, metric_name):
        list_of_valid_metric_names = ['time', 'output', 'memory']
        for name in list_of_valid_metric_names:
            if name in metric_name:
                return True
        return False

    def generate_collapsible_button(self, txt, txt_ind):
        """
        For every line, generate a button for header and collapsible body
        """
        if not txt:
            return ""
        body = '\n\n'.join(
            ['<font color="blue">' + section.split("\n")[0] + '</font>\n' + section.split("\n", 1)[-1] for section in
             txt.split("\n\n")[:10]])
        continuation_body = ""
        remaining_steps = len(txt.split("\n\n")) - 10
        if remaining_steps > 0:
            continuation_body = f"\n[+{remaining_steps} more steps]"
        return f"""
    <button style="font-size: 10px" type="button" class="btn btn-danger" data-toggle="collapse" data-target="#body_{txt_ind}">Expand steps</button>
    <div id="body_{txt_ind}" class="collapse">
      {body}
      {continuation_body}
    </div>
    """

    def generate_table_rows(self, summary_df):
        template = """
    <tr>
      <td style="white-space: pre-line;font-size: 15px">{stage_description} <br> {sql_steps}</td>
      <td style="font-size: 15px">{issues_summary}</td>
      <td align="center style="font-size: 25px"><div class="textWithEmoji" style="font-size:20px;">{severity_score}</div></td>
       <td style="width: 240px;font-size: 13px" align="center">
        {stage_btn}{sql_btn}
      </td>
      </tr>
    """
        table_row_html = ""
        for ind, row in summary_df.iterrows():

            sql_btn, stage_btn = '', ''
            if row['sql_link']:
                sql_btn = f"""
          <a style="font-size: 10px" class="btn btn-danger"
          href="{row['sql_link']}" 
          target="_blank">Spark SQL Link</a>
        """
            if row['stage_link']:
                stage_btn = f"""
          <a style="font-size: 10px" class="btn btn-danger"
          href="{row['stage_link']}"
          target="_blank">Spark Stage Link</a>
        """

            table_row_html += template.format(
                stage_description=row['stage_description'],
                sql_steps=self.generate_collapsible_button(row['sql_steps'], ind),
                issues_summary=row['issues_summary'],
                severity_score=row['severity_score'],
                stage_btn=stage_btn,
                sql_btn=sql_btn
            )
        return table_row_html

    def retrieve_style_html(self):
        style_html = """
    <style>

    .btn {
    border-width: 5px;
    }

    .darken-grey-text {
        color: #2E2E2E;
    }
    .input-group.md-form.form-sm.form-2 input {
        border: 1px solid #bdbdbd;
        border-top-left-radius: 0.25rem;
        border-bottom-left-radius: 0.25rem;
    }
    .input-group.md-form.form-sm.form-2 input.purple-border {
        border: 1px solid #9e9e9e;
    }
    .input-group.md-form.form-sm.form-2 input[type=text]:focus:not([readonly]).purple-border {
        border: 1px solid #ba68c8;
        box-shadow: none;
    }
    .form-2 .input-group-addon {
        border: 1px solid #ba68c8;
    }
    .danger-text {
        color: #ff3547; 
    }  
    .success-text {
        color: #00C851; 
    }
    .table-bordered.red-border, .table-bordered.red-border th, .table-bordered.red-border td {
        border: 1px solid #ff3547!important;
    }        
    .table.table-bordered th {
        text-align: center;
    }

    div.textWithEmoji {
      font-size: medium;
    }
    td {
      vertical-align: middle;
    }
    </style>
    """
        return style_html

    def retrieve_imports_html(self):
        imports_html = """
        <!-- Font Awesome -->
        <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.1/css/bootstrap.min.css">
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.css">
        <!-- Material Design Bootstrap -->
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/mdbootstrap/4.4.3/css/mdb.min.css">
        <!-- Collapsibles -->
        <meta name="viewport" content="width=device-width, initial-scale=1">

        <script src="https://ajax.googleapis.com/ajax/libs/jquery/3.6.0/jquery.min.js"></script>
        <script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.4.1/js/bootstrap.min.js"></script>
    """
        return imports_html

    def retrieve_table_html(self, summary_df):
        table_html = f"""
    <table class="table table-hover">
      <thead class="mdb-color lighten-0">
        <tr class="text-blue">
          <th style="font-size: 15px;color:white">Details</th>
          <th style="font-size: 15px;color:white">Summary</th>
          <th style="font-size: 15px;color:white">Severity Score</th>
          <th style="font-size: 15px;color:white">Links</th>
        </tr>
      </thead>

      <tbody>
        {self.generate_table_rows(summary_df)}
      </tbody>
    </table>
    """
        return table_html
