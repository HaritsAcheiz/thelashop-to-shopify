from httpx import AsyncClient, Client
from selectolax.parser import HTMLParser
from dataclasses import dataclass
import os
import asyncio
import duckdb
import json
import logging
import re
from html import escape
import math
import pandas as pd
import csv
from urllib.parse import urlparse, parse_qs, urlunparse

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


@dataclass
class FTScraper:
	base_url: str = 'https://thelashop.com'
	user_agent: str = 'Mozilla/5.0 (X11; Linux x86_64)'

	def get_price(self, wholesaleprice):
		float_wholesaleprice = float(wholesaleprice)
		if (wholesaleprice is None) or (float_wholesaleprice == 0) or (wholesaleprice == '0.00'):
			result = "0.00"
		else:
			result = float_wholesaleprice - round(float_wholesaleprice * 5 / 100, 2)

		return f"{result:.2f}"

	def get_fullscale_image_url(self, url):
		parsed_url = urlparse(url)
		query_params = parse_qs(parsed_url.query)
		query_params.pop('width', None)

		new_query = '&'.join(
			f"{k}={v[0]}" for k, v in query_params.items()
		)

		result = urlunparse((
			parsed_url.scheme,
			parsed_url.netloc,
			parsed_url.path,
			parsed_url.params,
			new_query,
			parsed_url.fragment
		))

		return result

	def clean_html(self, html_content):
		# 1. Remove non-standard attributes that Shopify may not recognize
		cleaned_html = re.sub(r'\sdata-[\w-]+="[^"]*"', '', html_content)

		# 2. Encode special characters like apostrophes, quotes, etc.
		cleaned_html = escape(cleaned_html)

		# 3. Decode standard HTML entities back to their original form (e.g., <, >)
		cleaned_html = cleaned_html.replace('&lt;', '<').replace('&gt;', '>').replace('&amp;', '&')

		# 4. Remove excessive whitespace between tags
		cleaned_html = re.sub(r'>\s+<', '><', cleaned_html)

		# 5. Ensure spaces between inline elements where necessary
		cleaned_html = re.sub(r'(<span[^>]*>)\s*(<)', r'\1 <', cleaned_html)

		# 6. Remove excess spaces and newlines in text nodes
		cleaned_html = re.sub(r'\s*\n\s*', '', cleaned_html)

		return cleaned_html

	# def auto_correct_json(self, json_string):
	# 	print(json_string)

	# 	try:
	# 		product_data_str = json_string + r']}'
	# 		product_data = json.loads(product_data_str)
	# 	except Exception:
	# 		product_data_str = json_string + r'}]}'
	# 		product_data = json.loads(product_data_str)
	# 	finally:
	# 		print(product_data_str)
	# 		return product_data

	# def debug_explode_columns(self, df, columns_to_explode):
	#     """
	#     Debug which rows have mismatched lengths when trying to explode multiple columns.

	#     Args:
	#         df: pandas DataFrame
	#         columns_to_explode: list of column names to check

	#     Returns:
	#         DataFrame containing problematic rows and their list lengths
	#     """
	#     # Create a dict to store lengths of lists in each column
	#     lengths = {}

	#     # Calculate length of each list in each column
	#     for col in columns_to_explode:
	#         lengths[f'{col}_length'] = df[col].apply(lambda x: len(x) if isinstance(x, (list, tuple)) else 1)

	#     # Create a DataFrame with the lengths
	#     length_df = pd.DataFrame(lengths)

	#     # Find rows where lengths don't match
	#     first_col_length = length_df.iloc[:, 0]
	#     mismatched_mask = False

	#     for col in length_df.columns[1:]:
	#         mismatched_mask |= (length_df[col] != first_col_length)

	#     # Get problematic rows
	#     problem_rows = df[mismatched_mask].copy()

	#     # Add length columns to the output
	#     for col, length in lengths.items():
	#         problem_rows[col] = length[mismatched_mask]

	#     return problem_rows

	def get_product_count(self, url):
		headers = {
			'user-agent': self.user_agent
		}

		with Client(headers=headers, follow_redirects=True) as client:
			response = client.get(url)
			response.raise_for_status()

		tree = HTMLParser(response.text)
		product_count = int(tree.css_first('div.pagination >span:nth-last-child(2) > a').text(strip=True))

		return product_count

	async def fetch(self, aclient, url, limit):
		logger.info(f'Fetching {url}...')
		async with limit:
			response = await aclient.get(url, follow_redirects=True)
			if limit.locked():
				await asyncio.sleep(1)
				response.raise_for_status()
		logger.info(f'Fetching {url}...Completed!')

		return url, response.text

	async def fetch_all(self, urls):
		tasks = []
		headers = {
			'user-agent': self.user_agent
		}
		limit = asyncio.Semaphore(10)
		async with AsyncClient(headers=headers, timeout=120, proxy='http://p.webshare.io:9999') as aclient:
			for url in urls:
				task = asyncio.create_task(self.fetch(aclient, url=url, limit=limit))
				tasks.append(task)
			htmls = await asyncio.gather(*tasks)

		return htmls

	def insert_to_db(self, htmls, database_name, table_name):
		logger.info('Inserting data to database...')
		# if os.path.exists(database_name):
		# 	os.remove(database_name)

		conn = duckdb.connect(database_name)
		curr = conn.cursor()

		try:
			curr.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (url TEXT, html BLOB)")

			htmls = [(url, bytes(html, 'utf-8') if not isinstance(html, bytes) else html) for url, html in htmls]
			curr.executemany(f"INSERT INTO {table_name} (url, html) VALUES (?, ?)", htmls)
			conn.commit()

		finally:
			curr.close()
			conn.close()
			logger.info('Data inserted!')

	def extract_product_data(self, script_content):
		pattern = r'var\s+seo_html\s*=\s*(\{.*?\})(?=\s*;|\s*\n\s*fetch)'
		match = re.search(pattern, script_content, re.DOTALL)

		if not match:
			return "No product data found in the script."

		# Extract the JSON string
		json_str = match.group(1)

		# try:
		cleaned_json_1 = re.sub(r'([{,])\s*(\w+):', r'\1"\2":', json_str)
		product_data = self.clean_json_string(cleaned_json_1)
		print('====================================================================')
		print(product_data)

		extracted_data = {
			"Product Name": product_data.get("name", ""),
			"Description": re.sub(r'\s+', ' ', product_data.get("description", "")),
			"SKU": product_data.get("sku", ""),
			"MPN": product_data.get("mpn", ""),
			"Color": product_data.get("color", ""),
			"Product ID": product_data.get("productID", ""),
			"Brand": product_data.get("brand", {}).get("name", ""),
			"Price": product_data.get("offers", {}).get("price", ""),
			"Currency": product_data.get("offers", {}).get("priceCurrency", ""),
			"Availability": "In Stock" if "InStock" in product_data.get("offers", {}).get("availability", "") else "Out of Stock",
			"URL": product_data.get("url", ""),
		}

		# Extract detailed specifications if available
		specifications = {}
		description = product_data.get("description", "")

		# Look for product specs (format: Key: Value)
		spec_pattern = r'(\w+[ \w]+?):\s*([\w./]+?[^,]*)'
		specs = re.findall(spec_pattern, description)
		for key, value in specs:
			if "Material" in key or "Color" in key or "Dimension" in key or "Weight" in key:
				specifications[key.strip()] = value.strip()

		if specifications:
			extracted_data["Specifications"] = specifications

		return extracted_data

		# except json.JSONDecodeError as e:
		# 	print(f"Error parsing JSON: {e}")
		# 	print(cleaned_json)
		# except Exception as e:
		# 	print(f"Error: {e}")
		# 	print(cleaned_json)

	def clean_json_string(self, json_str):

		def replace_inside_quotes(match):
			return match.group(0).replace("'", "~!~")

		clean_str = re.sub(r'"description"\s*:\s*".*?",\s*"sku"', '"sku"', json_str, flags=re.DOTALL)
		clean_str = re.sub(r'"(?:\\.|[^"\\])*"', replace_inside_quotes, clean_str)
		clean_str = re.sub(r"'([^']*?)'", r'"\1"', clean_str)
		clean_str = clean_str.replace('`', '"')
		clean_str = re.sub(r',(\s*[\]}])', r'\1', clean_str)
		clean_str = clean_str.replace("~!~", "'")

		try:
			parsed_json = json.loads(clean_str)
		except Exception:
			print(clean_str)

		return parsed_json

	def extract_product_data_variant(self, script_content):
		pattern = r'window\.dataLayer\.push\(\s*({.*?})\s*\);'
		matches = re.findall(pattern, script_content, re.DOTALL)

		if not matches[1]:
			return "No product data found in the script."

		product_data = []

		try:
			raw_json = matches[1]
			data = self.clean_json_string(raw_json)

			# Check if this is the view_item event with product data
			if data.get("event_name") == "view_item" and "items" in data.get("event_parameters", {}):
				items = data["event_parameters"]["items"]

			for item in items:
				product_data.append({
					"Item ID": item.get("item_id", ""),
					"Product Name": item.get("item_name", ""),
					"Category": item.get("item_category", ""),
					"Variant": item.get("item_variant", ""),
					"Brand": item.get("item_brand", ""),
					"Price": item.get("price", ""),
					"Currency": data["event_parameters"].get("currency", "USD")
				})

		except json.JSONDecodeError as e:
			print(f"Error parsing JSON: {e}")
			print(data)
		except Exception as e:
			print(f"Error: {e}")

		if not product_data:
			return "No product items found in the dataLayer."

		return product_data

	def extract_product_data_detail(self, script_content):
		pattern = r'initData:\s*({.*?}),\s*function'
		matches = re.findall(pattern, script_content, re.DOTALL)

		if not matches[0]:
			return "No product data found in the script."

		product_data = []

		try:
			raw_json = matches[0][0:-2]
			data = self.clean_json_string(raw_json)
			# print('=========================================')
			# print(data)

			# Check if this is the view_item event with product data
			# if data.get("event_name") == "view_item" and "items" in data.get("event_parameters", {}):
			variants = data["productVariants"]

			for item in variants:
				product_data.append({
					"Item ID": item.get("sku", ""),
					"Product Name": item['product'].get("title", ""),
					"Category": item['product'].get("type", ""),
					"Variant": item.get("title", ""),
					"Brand": item["product"].get("vendor", ""),
					"Price": item["price"].get("amount", 0.00),
					"Currency": item["price"].get("currencyCode", "USD"),
					"Image Src": item["image"].get("src", "")
				})

		except json.JSONDecodeError as e:
			print(f"Error parsing JSON: {e}")
			print(variants)
		except Exception as e:
			print(f"Error: {e}")

		if not product_data:
			return "No product items found in the dataLayer."

		return product_data

	def get_data(self):
		logger.info('Getting data from database...')
		conn = duckdb.connect("thelashop.db")
		curr = conn.cursor()
		curr.execute("SELECT url, html FROM product_src LIMIT 1 OFFSET 10")
		datas = curr.fetchall()
		product_datas = list()

		with open('shopify_schema.json', 'r') as file:
			product_schema = json.load(file)

		for data in datas:
			print('===============================================================')
			print(f"Parsing {data[0]} ...")
			current_product = product_schema.copy()
			tree = HTMLParser(data[1])

			script_tags = tree.css('script')
			script_content = None

			# for script in script_tags:
			# 	print('=========================================================')
			# 	print(script.text())

			for script in script_tags:
				if 'inventory_quantity' in script.text():
					script_content = script.text()
					break
			if script_content:
				product_var = json.loads(script_content)

			for script in script_tags:
				if 'var seo_html' in script.text():
					script_content = script.text()
					break
			if script_content:
				product_data = self.extract_product_data(script_content)

			print(product_var)
			print('========================================================================')
			print(product_data)

			current_product['Handle'] = data[0].split('/')[-1]
			current_product['Title'] = product_data['Product Name']
			current_product['Body (HTML)'] = self.clean_html(tree.css_first('div.pg__tabs > div > div > div').html)
			current_product['Vendor'] = product_data['Brand']
			breadcrumbs = tree.css('li[itemprop="itemListElement"]')
			breadcrumb_list = [breadcrumb.text(strip=True) for breadcrumb in breadcrumbs]
			current_product['Product Category'] = ' > '.join(breadcrumb_list[1:-1])
			current_product['Type'] = breadcrumb_list[-1]
			current_product['Tags'] = ', '.join(breadcrumb_list[1:-1])
			# product_elem = tree.css_first('div#shopify-section-pr_summary')
			option_label = tree.css_first('span.spr_variant-label')
			if not option_label:
				pass
			if option_label:
				current_product['Option1 Name'] = option_label.text(strip=True).split(':')[0]
			image_elements = tree.css('img.img-abs.pg__img--auto-ratio')
			image_srcs = [elem.attrs['src'] for elem in image_elements]

			option1_values = list()
			option2_values = list()
			option3_values = list()
			variant_skus = list()
			variant_weight = list()
			variant_qty = list()
			variant_cost = list()
			variant_image = list()
			variant_requires_shipping = list()
			variant_taxable = list()

			variant_list = list(product_var.values())
			print('=========================================================')
			print(variant_list)
			for variant in variant_list:
				if current_product['Option1 Name'] != '':
					if variant['options'] != 'None':
						option1_values.extend(variant['options'])
				else:
					option1_values = ''
		# 		if current_product['Option2 Name'] != '':
		# 			if variant['option2'] != 'None':
		# 				option2_values.append(variant['option2'])
		# 		else:
		# 			option2_values = ''

		# 		if current_product['Option3 Name'] != '':
		# 			if variant['option3'] != 'None':
		# 				option3_values.append(variant['option3'])
		# 		else:
		# 			option3_values = ''

				variant_skus.append(variant['sku'])
				variant_weight.append('')
				variant_qty.append(10 if variant['inventory_quantity'] > 0 else 0)
		# 		variant_cost.append(variant['Price'])
		# 		try:
		# 			variant_image.append(f"https:{variant['Image Src']}")
		# 		except Exception:
		# 			variant_image.append('')
		# 		variant_requires_shipping.append(True)
		# 		variant_taxable.append(True)

			current_product['Option1 Value'] = option1_values
		# 	current_product['Option2 Value'] = option2_values
		# 	current_product['Option3 Value'] = option3_values
			current_product['Variant SKU'] = variant_skus
			current_product['Variant Grams'] = variant_weight
			current_product['Variant Inventory Qty'] = variant_qty
		# 	current_product['Google Shopping / Custom Label 0'] = 'YCU'
		# 	current_product['Variant Image'] = variant_image
		# 	current_product['Cost per item'] = variant_cost
		# 	current_product['Variant Price'] = [self.get_price(x) for x in variant_cost]
		# 	current_product['Variant Compare At Price'] = ''
		# 	current_product['Variant Requires Shipping'] = variant_requires_shipping
		# 	current_product['Variant Taxable'] = variant_taxable
		# 	try:
		# 		current_product['Image Src'] = [self.get_fullscale_image_url(f'https:{url}') for url in image_srcs]
		# 		current_product['Image Alt Text'] = [url.split('/')[-1].split('?')[0] for url in image_srcs]
		# 	except KeyError:
		# 		pass

		print('==========================================================')
		print(current_product)
		# 	product_datas.append(current_product)

		# df = pd.DataFrame.from_records(product_datas)

		# logger.info('Data Extracted!')

		# return df

	def transform_product_datas(self, df):
		# try:
		# First check for problematic rows
		# problems = self.debug_explode_columns(df, [
		# 	'Option1 Value', 'Variant SKU', 'Variant Grams', 'Variant Inventory Qty',
		#     'Variant Price', 'Variant Requires Shipping', 'Variant Taxable', 'Variant Image',
		#     'Cost per item'
		# ])

		# if len(problems) > 0:
		#     print("Found problematic rows:")
		#     print(problems[[
		#         'Handle', 'Option1 Name', 'Option1 Value', 'Variant SKU'
		#     ]])
		# else:
		# If no problems found, proceed with explode

		df = df.explode([
			'Option1 Value', 'Variant SKU', 'Variant Grams', 'Variant Inventory Qty',
			'Variant Price', 'Variant Requires Shipping', 'Variant Taxable', 'Variant Image',
			'Cost per item'
		],
			ignore_index=True
		)

		with open('variant_unused_columns.csv', 'r') as file:
			rows = csv.reader(file)
			variant_unused_columns = [row[0] for row in rows]
			df.loc[df.duplicated('Handle', keep='first'), variant_unused_columns] = ''

		df = df.explode(['Image Src', 'Image Alt Text'], ignore_index=True)
		with open('images_unused_columns.csv', 'r') as file:
			rows = csv.reader(file)
			images_unused_columns = [row[0] for row in rows]
			df.loc[df.duplicated('Variant SKU', keep='first'), images_unused_columns] = ''

		return df

	def fetch_search_result_html(self, url):
		total_pages = self.get_product_count(url)
		# total_pages = 1
		urls = [f'{url}?page={page}' for page in range(1, total_pages + 1)]
		search_results_html = asyncio.run(self.fetch_all(urls))
		self.insert_to_db(search_results_html, database_name='thelashop.db', table_name='search_src')

	def get_product_urls(self):
		logger.info('Getting data from database...')
		conn = duckdb.connect("thelashop.db")
		curr = conn.cursor()
		curr.execute("SELECT url, html FROM  search_src")
		datas = curr.fetchall()
		results = list()

		for data in datas:
			tree = HTMLParser(data[1])
			product_elems = tree.css('a.item__name')
			product_urls = list()
			for elem in product_elems:
				product_urls.append(f"{self.base_url}{elem.attributes.get('href')}")
			results.extend(product_urls)

		return results

	def fetch_product_html(self, urls):
		product_htmls = asyncio.run(self.fetch_all(urls))
		self.insert_to_db(product_htmls, database_name='thelashop.db', table_name='product_src')

	def create_csv(self, df, csv_path):
		logger.info("Write data into csv...")
		df.to_csv(csv_path, index=False)
		logger.info("Done")
