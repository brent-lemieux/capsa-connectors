import requests
import pandas as pd


def get_data(base_url, table, bearer_token, date_column=None, start_date=None, id_filter=None):
	url = f"{base_url}/{table}"
	print(url)
	all_rows = []
	page_number = 1
	limit = 1000
	while True:
		params = {
			'$pageNumber': page_number,
			'$limit': limit
		}
		if date_column and start_date:
			id_filter_str = ""
			if id_filter:
				id_filter_str = f" and {id_filter['key']} eq {id_filter['value']}"
			params['$filter'] = f"{date_column} ge {start_date}{id_filter_str}"
			params['$orderby'] = f"{date_column}"
		headers = {
			'Authorization': f'Bearer {bearer_token}',
			'Content-Type': 'application/json'
		}
		response = requests.get(url, headers=headers, params=params)
		if response.status_code == 200:
			data = response.json()
			if not data:
				break
			all_rows.extend(data)
			print(data[-1][date_column])
			page_number += 1
		else:
			raise Exception(f"Error: {response.status_code} - {response.text}")
	return all_rows	


def get_child_table(df, child_table, parent_id):
	child_rows = []
	for pid, row in df[[parent_id, child_table]].values:
		for arr in row:
			if len(arr) == 0:
				continue
			child_rows.append({
				parent_id: pid,
				**arr
			})
	cdf = pd.DataFrame(child_rows)
	return cdf