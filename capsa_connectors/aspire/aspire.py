import requests

import pandas as pd

from google.cloud import bigquery
import google.auth
import pandas_gbq

from tables import TABLE_CONFIGS
from utils import (
    get_data,
    get_child_table,
)

AUTH_URL = 'https://cloud-api.youraspire.com/Authorization'
BASE_URL = 'https://cloud-api.youraspire.com'
DEFAULT_START_DATE = '2015-01-01'


def retrieve_bearer_token(client_key, client_secret):
	# Request payload
	payload = {
		'ClientId': client_key,
		'Secret': client_secret
	}
	# Headers for the request
	headers = {
		'Content-Type': 'application/json'
	}
	# Make the request to get the access token
	token_response = requests.post(AUTH_URL, json=payload, headers=headers)
	# Check if the request was successful
	if token_response.status_code == 200:
		bearer_token = token_response.json().get('Token')
	else:
		print(f"Error: {token_response.status_code}")
		print(token_response.text)
	return bearer_token


def establish_gbq_connection(project_id):
    # Setting up credentials
    credentials, _ = google.auth.default()
    credentials = google.auth.credentials.with_scopes_if_required(credentials, bigquery.Client.SCOPE)
    authed_http = google.auth.transport.requests.AuthorizedSession(credentials)
    # Update the in-memory credentials cache.
    pandas_gbq.context.credentials = credentials
    pandas_gbq.context.project = project_id


class AspireTable(object):
	def __init__(
        self, 
		table_name, 
		date_column, 
		id_column, 
		full_refresh=None, 
		refresh_date=None, 
		bearer_token=None, 
		project_id=None, 
		dataset_id=None
    ):
		self.table_name = table_name
		self.data_feed_name = table_name.replace("aspire_", "")
		self.date_column = date_column
		self.id_column = id_column
		self.full_refresh = full_refresh
		self.refresh_date = refresh_date or DEFAULT_START_DATE
		self.bearer_token = bearer_token
		self.table_configs = self._load_table_configs()
		self.project_id = project_id
		self.dataset_id = dataset_id
		self._get_data()

	def _get_max_date(self, df, date_column):
		dates = pd.to_datetime(df[date_column], format="mixed")
		return dates.max().strftime("%Y-%m-%d")
	
	def _load_gbq_table(self, table_name):
		return pandas_gbq.read_gbq(f"SELECT * FROM `{self.project_id}.{self.dataset_id}.{table_name}`", project_id=self.project_id, location="US")
	
	def _write_gbq_table(self, df, table_name):
		pandas_gbq.to_gbq(df, f"{self.dataset_id}.{table_name}", project_id=self.project_id, if_exists="replace")
		print(f"Updated {table_name} with {len(df)} rows")
	
	def _upsert(self, df, additional_df=None, id_column=None):
		if additional_df is None:
			return df
		df = df[~df[id_column].isin(additional_df[id_column])].copy()
		df = pd.concat([df, additional_df])
		return df
	
	def _format_columns(self, df, table_configs=None):
		if not table_configs:
			return df
		# Drop columns.
		drop_columns = table_configs.get("drop_columns")
		if drop_columns:
			df = df.drop(columns=drop_columns, errors='ignore')
		# Convert columns to bool.
		to_bool_columns = table_configs.get("to_bool_columns")
		if to_bool_columns:
			for col in to_bool_columns:
				df[col] = df[col].astype(bool)
		return df

	def _handle_table_configs(self, df, table_configs=None):
		if not table_configs:
			return df, {}, {}
		# Handle child tables.
		child_table_dfs = {}
		child_table_parent_ids = {}
		child_tables = table_configs.get("child_tables")
		if child_tables:
			for child_table in child_tables:
				child_table_config = TABLE_CONFIGS.get(child_table, {})
				child_table_df = get_child_table(df, child_table, table_configs["id_column"])
				# Format columns.
				child_table_df = self._format_columns(child_table_df, child_table_config)
				child_table_dfs[child_table] = child_table_df
				child_table_parent_ids[child_table] = table_configs["id_column"]
				df = df.drop(columns=[child_table], errors='ignore')
				if child_table_config.get("child_tables"):
					child_table_df, grandchild_table_dfs, grandchild_table_parent_ids = self._handle_table_configs(child_table_df, child_table_config)
					child_table_dfs[child_table] = child_table_df
					child_table_dfs.update(grandchild_table_dfs)
					child_table_parent_ids.update(grandchild_table_parent_ids)
		# Format columns.
		df = self._format_columns(df, table_configs)
		return df, child_table_dfs, child_table_parent_ids
	
	def _handle_write_child_tables(self, child_table_dfs, child_table_parent_ids, is_additional_data=False):
		for child_table, child_df in child_table_dfs.items():
			child_table_fullname = f"aspire_{child_table}"
			if is_additional_data:
				child_df = self._upsert(self._load_gbq_table(child_table_fullname), child_df, child_table_parent_ids[child_table])
			self._write_gbq_table(child_df, child_table_fullname)

	def _get_data(self):
		if self.full_refresh:
			self.data = get_data(BASE_URL, self.data_feed_name, self.bearer_token, self.date_column, self.refresh_date)
			self.df = pd.DataFrame(self.data)
			# Handle table configs on df.
			self.df, child_table_dfs, child_table_parent_ids = self._handle_table_configs(self.df, self.table_configs)
			self._write_gbq_table(self.df, self.table_name)
			self._handle_write_child_tables(child_table_dfs, child_table_parent_ids, is_additional_data=False)
		elif not self.date_column:
			self.df = self._load_gbq_table(self.table_name)
			print(f"Loaded {self.table_name} from file with {len(self.df)} rows")
		else:
			df = self._load_gbq_table(self.table_name)
			print(f"Loaded {self.table_name} from file with {len(df)} rows")
			max_date = self._get_max_date(df, self.date_column)
			print(f"Max date is {max_date}")
			additional_data = get_data(BASE_URL, self.data_feed_name, self.bearer_token, self.date_column, max_date)
			additional_df = pd.DataFrame(additional_data)
			print(additional_df.columns)
			# Handle table configs on additional_df.
			additional_df, child_table_dfs, child_table_parent_ids = self._handle_table_configs(additional_df, self.table_configs)
			if child_table_dfs:
				self._handle_write_child_tables(child_table_dfs, child_table_parent_ids, is_additional_data=True)
			print(f"Loaded {len(additional_df)} additional rows")
			self.df = self._upsert(df, additional_df, self.id_column)
			self._write_gbq_table(self.df, self.table_name)

	def _load_table_configs(self):
		table_configs = TABLE_CONFIGS.get(self.data_feed_name)
		if not table_configs:
			return
		return table_configs

