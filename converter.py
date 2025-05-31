import os
import pandas as pd
import re
import json
from urllib.parse import quote, unquote
from ast import literal_eval
from html import unescape

weight_unit_mapper = {'lb': 'POUNDS', 'kg': 'KILOGRAMS', 'g': 'GRAMS', 'oz': 'OUNCES'}
tracker_mapper = {'shopify': True, '': False}


def to_handle(title, alt_title):
    if (pd.isna(title)) | (title == 0):
        if pd.isna(alt_title):
            result = None
        else:
            alt_title.replace('-', '')
            pattern = re.compile(r"\b[a-zA-Z0-9]+\b")
            matches = pattern.findall(alt_title.lower().strip())
            result = '-'.join(matches)
    else:
        title.replace('-', '')
        pattern = re.compile(r"\b[a-zA-Z0-9]+\b")
        matches = pattern.findall(title.lower().strip())
        result = '-'.join(matches)

    return result


def get_title(title, alt_title):
    if (pd.isna(title)) | (title == 0):
        if pd.isna(alt_title):
            result = None
        else:
            result = alt_title
    else:
        result = title

    return result


def generate_category(*args):
    cat_list = [x for x in list(args[0]) if str(x) != 'nan']
    if len(cat_list) == 0:

        return ''

    return ' > '.join(cat_list)


def to_tags(theme):
    if (theme == '') | pd.isna(theme):
        result = ''
    else:
        result = theme.replace(';', ',')

    return result


def generate_image(*args):
    image_urls = [quote(x, safe=':/?&=') for x in list(args[0]) if str(x) != 'nan']
    if len(image_urls) == 0:

        return ''

    else:

        return image_urls


def generate_alt_text(*args):
    image_alt_text = [unquote(x).split('/')[-1].split('.')[0].strip() for x in args[0]]
    if len(image_alt_text) == 0:

        return ''

    else:

        return image_alt_text


def to_body_html(desc):
    if not isinstance(desc, str):
        desc_str = str(desc)
    else:
        desc_str = desc
    result = unescape(desc_str).replace("ORIENTAL TRADING", "TRENDTIMES")\
        .replace("morriscostumes.com", "trendtimes.com")\
        .replace("br", "<br/>")\
        .replace("Oriental Trading", "Trendtimes")

    return result


def to_shopify(morris_file_path):
    morris_df = pd.read_excel(morris_file_path)
    shopify_df = pd.DataFrame()
    shopify_df['Handle'] = morris_df.apply(lambda x: to_handle(x['ProductName'], alt_title=x['FormattedName']), axis=1)
    shopify_df['Title'] = morris_df.apply(lambda x: get_title(x['FormattedName'], alt_title=x['ProductName']), axis=1)
    shopify_df['Body (HTML)'] = morris_df['FullDescription'].apply(to_body_html)
    shopify_df['Vendor'] = morris_df['Brand']
    shopify_df['Product Category'] = morris_df.apply(lambda x: generate_category((x['PrimaryCategory'],
                                                                                  x['SecondaryCategory'],
                                                                                  x['ThirdCategory'])), axis=1)
    shopify_df['Type'] = 'Costumes'
    shopify_df['Tags'] = morris_df['Theme'].apply(to_tags)
    shopify_df['Published'] = True
    shopify_df['Option1 Name'] = morris_df['VariationType1']
    shopify_df['Option1 Value'] = morris_df['VariationValue1']
    shopify_df['Option1 Linked To'] = ''
    shopify_df['Option2 Name'] = morris_df['VariationType2']
    shopify_df['Option2 Value'] = morris_df['VariationValue2']
    shopify_df['Option2 Linked To'] = ''
    shopify_df['Option3 Name'] = ''
    shopify_df['Option3 Value'] = ''
    shopify_df['Option3 Linked To'] = ''
    shopify_df['Variant SKU'] = morris_df['Sku']
    shopify_df['Variant Grams'] = morris_df['ItemWeight']
    shopify_df['Variant Inventory Tracker'] = 'shopify'
    shopify_df['Variant Inventory Qty'] = morris_df['QOH']
    shopify_df['Variant Inventory Policy'] = 'deny'
    shopify_df['Variant Inventory Fulfillment Service'] = 'manual'
    # shopify_df['Variant Price'] = morris_df['MapPrice']
    shopify_df['Variant Price'] = morris_df['MapPrice']
    shopify_df['Variant Compare At Price'] = ''
    shopify_df['Variant Requires Shipping'] = True
    shopify_df['Variant Taxable'] = True
    shopify_df['Variant Barcode'] = morris_df['Selling Unit Master UPC']
    shopify_df['Image Src'] = morris_df.apply(lambda x: generate_image((x['PrimaryImgLink'],
                                                                        x['ImgAlternate1'],
                                                                        x['ImgAlternate2'],
                                                                        x['ImgAlternate3'],
                                                                        x['ImgAlternate4'],
                                                                        x['ImgAlternate5'],
                                                                        x['ImgAlternate6'])), axis=1)
    shopify_df['Image Position'] = 1
    shopify_df['Image Alt Text'] = shopify_df['Image Src'].apply(generate_alt_text)
    shopify_df['Gift Card'] = ''
    shopify_df['SEO Title'] = ''
    shopify_df['SEO Description'] = ''
    shopify_df['Google Shopping / Google Product Category'] = shopify_df['Product Category']
    shopify_df['Google Shopping / Gender'] = morris_df['Gender']
    shopify_df['Google Shopping / Age Group'] = morris_df['Age Group']
    shopify_df['Google Shopping / MPN'] = shopify_df['Variant Barcode']
    shopify_df['Google Shopping / Condition'] = 'New'
    shopify_df['Google Shopping / Custom Product'] = ''
    shopify_df['Google Shopping / Custom Label 0'] = ''
    shopify_df['Google Shopping / Custom Label 1'] = ''
    shopify_df['Google Shopping / Custom Label 2'] = ''
    shopify_df['Google Shopping / Custom Label 3'] = ''
    shopify_df['Google Shopping / Custom Label 4'] = ''
    shopify_df['enable_best_price (product.metafields.custom.enable_best_price)'] = True
    shopify_df['Product rating count (product.metafields.reviews.rating_count)'] = ''
    shopify_df['Variant Image'] = morris_df['PrimaryImgLink']
    shopify_df['Variant Weight Unit'] = 'lb'
    shopify_df['Variant Tax Code'] = ''
    shopify_df['Cost per item'] = morris_df['Price']
    shopify_df['Included / United States'] = ''
    shopify_df['Price / United States'] = ''
    shopify_df['Compare At Price / United States'] = ''
    shopify_df['Included / International'] = ''
    shopify_df['Price / International'] = ''
    shopify_df['Compare At Price / International'] = ''
    shopify_df['Status'] = 'active'
    shopify_df.dropna(axis=0, subset='Handle', inplace=True, ignore_index=True)
    shopify_df.fillna('', inplace=True)
    shopify_df = deduplicate_handles(shopify_df)

    shopify_df.to_csv('data/temp.csv', index=False)


def fill_opt(opt_name=None, opt_value=None):
    if opt_name != '':
        opt_attr = {'name': opt_name, 'values': {'name': opt_value}}

        return opt_attr


def fill_opt_var(opt_name=None, opt_value=None):
    if opt_name != '':
        opt_attr = {'name': opt_value, 'optionName': opt_name}

        return opt_attr


def fill_media(original_src, alt):
    if original_src != '':
        media_attr = {
            'originalSource': original_src,
            'mediaContentType': 'IMAGE',
            'alt': alt
        }

        return media_attr


def fill_variant_id(shopify_df, product_id_filepath, mode):
    # Fill variant id
    variant_ids_df = pd.read_csv(product_id_filepath)
    shopify_df = pd.merge(shopify_df, variant_ids_df, how='left', left_on='id', right_on='product_id')
    shopify_df.fillna('', inplace=True)
    shopify_df.drop(columns=['Unnamed: 0', 'handle', 'product_id'], inplace=True)
    if mode == 'create':
        shopify_df.to_csv('data/create_product_variants_with_vids_invids.csv', index=False)
    elif mode == 'update':
        shopify_df.to_csv('data/update_product_variants_with_vids_invids.csv', index=False)
    else:
        print('Mode is undefined')


def str_to_bool(s):
    if (s == 'True') | (s == 'true'):

        return True

    elif (s == 'False') | (s == 'false'):

        return False

    else:

        return s


def get_skus():
    shopify_df = pd.read_csv('data/temp.csv')

    return list(shopify_df['Variant SKU'])


def get_handles(filepath, nrows=250):
    shopify_df = pd.read_csv(filepath)
    try:
        handles = list(shopify_df['Unique Handle'])
    except Exception:
        handles = list(shopify_df['handle'])
    chunked_handles = [handles[i:i + nrows] for i in range(0, len(handles), nrows)]

    return chunked_handles


def chunk_data(filepath, usecols=None, nrows=250):
    chunked_df = list()
    if usecols:
        df = pd.read_csv(filepath, usecols=usecols)
    else:
        df = pd.read_csv(filepath)
    for start in range(0, len(df), nrows):
        chunked_df.append(df[start:start + nrows])

    return chunked_df


def group_create_update():
    # Fill product id
    shopify_df = pd.read_csv('data/temp.csv')
    product_ids_df = pd.read_csv('data/product_ids.csv')
    shopify_df = pd.merge(shopify_df, product_ids_df, how='left', left_on='Unique Handle', right_on='handle')
    shopify_df.fillna('', inplace=True)

    # group update create
    create_df = shopify_df[shopify_df['id'] == '']
    update_df = shopify_df[shopify_df['id'] != '']
    create_df.to_csv('data/create_products.csv')
    update_df.to_csv('data/update_products.csv')


def fill_product_id(product_df, product_id_filepath, mode):
    # Fill product id
    shopify_df = product_df
    product_ids_df = pd.read_csv(product_id_filepath)
    shopify_df = pd.merge(shopify_df, product_ids_df, how='left', left_on='Unique Handle', right_on='handle')
    shopify_df.fillna('', inplace=True)
    shopify_df.drop(columns=['handle_x', 'id_x', 'handle_y'], inplace=True)
    shopify_df.rename({'id_y': 'id'}, axis=1, inplace=True)
    if mode == 'create':
        shopify_df.to_csv('data/create_products_with_id.csv', index=False)
    elif mode == 'update':
        shopify_df.to_csv('data/update_products_with_id.csv', index=False)
    else:
        print('Mode is undefined')


def csv_to_jsonl(csv_filename, jsonl_filename, mode='pc'):
    print("Converting csv to jsonl file...")
    df = pd.read_csv(csv_filename)
    df.fillna('', inplace=True)
    datas = None
    opts = ['Option1 Name', 'Option2 Name', 'Option3 Name']
    if mode == 'vc':
        # Create formatted dictionary
        datas = []
        for index in df.index:
            data_dict = {"productId": str, "strategy": "REMOVE_STANDALONE_VARIANT", "variants": list()}
            data_dict['productId'] = df.iloc[index]['id']

            variants = list()
            metafields = list()
            variant = dict()
            variant['barcode'] = str(df.iloc[index]['Variant Barcode'])
            if df.iloc[index]['Variant Compare At Price'] == '':
                pass
            else:
                variant['compareAtPrice'] = round(float(df.iloc[index]['Variant Compare At Price']), 2)
            # variant['id'] = df.iloc[index]['id']

            variant_inv_item = dict()
            variant_inv_item['cost'] = str(df.iloc[index]['Cost per item'])
            # variant_inv_item['countryCodeOfOrigin'] = df.iloc[index]['Variant Barcode']
            # variant_inv_item['countryHarmonizedSystemCodes'] = df.iloc[index]['Variant Barcode']
            # variant_inv_item['harmonizedSystemCode'] = df.iloc[index]['Variant Barcode']

            variant_measure = {'weight': {'unit': 'GRAMS', 'value': 0.0}}
            try:
                variant_measure['weight']['unit'] = weight_unit_mapper[df.iloc[index]['Variant Weight Unit']]
                variant_measure['weight']['value'] = float(df.iloc[index]['Variant Grams'])
            except:
                pass

            variant_inv_item['measurement'] = variant_measure
            # variant_inv_item['provinceCodeOfOrigin'] = df.iloc[index]['Variant Barcode']
            # variant_inv_item['requiresShipping'] = df.iloc[index]['Variant Requires Shipping']
            variant_inv_item['requiresShipping'] = str_to_bool('true')
            variant_inv_item['sku'] = df.iloc[index]['Variant SKU']
            variant_inv_item['tracked'] = tracker_mapper[df.iloc[index]['Variant Inventory Tracker']]
            variant['inventoryItem'] = variant_inv_item
            variant['inventoryPolicy'] = df.iloc[index]['Variant Inventory Policy'].upper()

            variants_inv_qty = list()
            if df.iloc[index]['Variant Inventory Qty'] == '':
                variant_inv_qty = {'availableQuantity': 0, 'locationId': os.getenv('SHOPIFY_LOCATION_ID')}
            else:
                variant_inv_qty = {'availableQuantity': 0, 'locationId': os.getenv('SHOPIFY_LOCATION_ID')}
                try:
                    variant_inv_qty['availableQuantity'] = int(df.iloc[index]['Variant Inventory Qty'])
                except ValueError:
                    variant_inv_qty['availableQuantity'] = int(df.iloc[index]['Variant Inventory Qty'].replace(',', ''))
                variant_inv_qty['locationId'] = os.getenv('SHOPIFY_LOCATION_ID')

            variants_inv_qty.append(variant_inv_qty)
            variant['inventoryQuantities'] = variants_inv_qty

            # variant['mediaId'] = df.iloc[index]['Variant Barcode']
            # variant['mediaSrc'] = df.iloc[index]['Variant Barcode']

            # metafield = dict()
            # metafield['id'] = df.iloc[index]['Variant Barcode']
            # metafield['key'] = 'custom'
            # metafield['namespace'] = 'enable_best_price'
            # metafield['type'] = 'boolean'
            # metafield['value'] = str(df.iloc[index]['enable_best_price (product.metafields.custom.enable_best_price)'])
            # metafields.append(metafield)

            # variant['metafields'] = metafields

            product_options = [fill_opt_var(df.iloc[index][opt], df.iloc[index][opt.replace('Name', 'Value')]) for opt in opts]

            if (product_options[0] is not None) | (product_options[1] is not None) | (product_options[2] is not None):
                product_options = [x for x in product_options if x is not None]
                variant['optionValues'] = product_options

            try:
                variant['price'] = round(float(df.iloc[index]['Variant Price']), 2)
            except:
                variant['price'] = 0.00

            # variant['taxCode'] = df.iloc[index]['Variant Barcode']
            # variant['taxable'] = df.iloc[index]['Variant Taxable']
            variant['taxable'] = str_to_bool('true')
            variants.append(variant)

            data_dict['variants'] = variants
            datas.append(data_dict.copy())

    elif mode == 'pc':
        # Create formatted dictionary
        datas = []
        for index in df.index:
            data_dict = {"input": dict(), "media": list()}
            # data_dict['input']['category'] = ''
            # data_dict['input']['claimOwnership'] = {'bundles': str_to_bool('False')}
            # data_dict['input']['collectionToJoin'] = ''
            # data_dict['input']['collectionToLeave'] = ''
            # data_dict['input']['combinedListingRole'] = 'PARENT'
            data_dict['input']['customProductType'] = df.iloc[index]['Type']
            data_dict['input']['descriptionHtml'] = df.iloc[index]['Body (HTML)']
            data_dict['input']['giftCard'] = str_to_bool('False') #df.iloc[index]['Gift Card']
            # data_dict['input']['giftCardTemplateSuffix'] = ''
            data_dict['input']['handle'] = df.iloc[index]['Unique Handle']
            # data_dict['input']['id'] = ''
            data_dict['input']['metafields'] = {#'id': '',
                                                'key': 'enable_best_price',
                                                'namespace': 'custom',
                                                'type': 'boolean',
                                                'value': str_to_bool(df.iloc[index]['enable_best_price (product.metafields.custom.enable_best_price)'])
                                                }
            product_options = [fill_opt(df.iloc[index][opt], df.iloc[index][opt.replace('Name', 'Value')]) for opt in opts]

            if (product_options[0] is not None) | (product_options[1] is not None) | (product_options[2] is not None):
                product_options = [x for x in product_options if x is not None]
                data_dict['input']['productOptions'] = product_options

            # data_dict['input']['productType'] = df.iloc[index]['Type']
            data_dict['input']['redirectNewHandle'] = str_to_bool('True')
            data_dict['input']['requiresSellingPlan'] = str_to_bool('False')
            data_dict['input']['seo'] = {'description': df.iloc[index]['SEO Description'],
                                         'title': df.iloc[index]['SEO Title']
                                         }
            data_dict['input']['status'] = df.iloc[index]['Status'].upper()
            data_dict['input']['tags'] = df.iloc[index]['Tags']
            # data_dict['input']['templateSuffix'] = ''
            data_dict['input']['title'] = df.iloc[index]['Title']
            data_dict['input']['vendor'] = df.iloc[index]['Vendor']

            media_list = []
            media = dict()
            if (pd.isna(df.iloc[index]['Link'])) | (df.iloc[index]['Link'] == ''):
                media_list.append(media)
            else:
                try:
                    links = literal_eval(df.iloc[index]['Link'])
                    alt_texts = literal_eval(df.iloc[index]['Image Alt Text'])
                    print(links)
                    for i in range(0, len(links)):
                        try:
                            media['alt'] = alt_texts[i]
                        except:
                            media['alt'] = ''
                        media['mediaContentType'] = 'IMAGE'
                        media['originalSource'] = links[i]
                        media_list.append(media)
                    data_dict['media'] = media_list
                except Exception:
                    pass

            datas.append(data_dict.copy())

    elif mode == 'pu':
        datas = []
        for index in df.index:
            data_dict = {"input": dict(), "media": list()}
            # data_dict['input']['category'] = ''
            # data_dict['input']['claimOwnership'] = {'bundles': str_to_bool('False')}
            # data_dict['input']['collectionToJoin'] = ''
            # data_dict['input']['collectionToLeave'] = ''
            # data_dict['input']['combinedListingRole'] = 'PARENT'
            data_dict['input']['customProductType'] = df.iloc[index]['Type']
            data_dict['input']['descriptionHtml'] = df.iloc[index]['Body (HTML)']
            data_dict['input']['giftCard'] = str_to_bool('False') #df.iloc[index]['Gift Card']
            # data_dict['input']['giftCardTemplateSuffix'] = ''
            # data_dict['input']['handle'] = df.iloc[index]['Unique Handle']
            data_dict['input']['id'] = df.iloc[index]['id']
            # data_dict['input']['metafields'] = {'id': df.iloc[index]['metafield_id'],
            #                                     'value': str_to_bool(df.iloc[index]['enable_best_price (product.metafields.custom.enable_best_price)'])
            #                                     }
            # product_options = [fill_opt(df.iloc[index][opt], df.iloc[index][opt.replace('Name', 'Value')]) for opt in opts]

            # if (product_options[0] is not None) | (product_options[1] is not None) | (product_options[2] is not None):
            #     product_options = [x for x in product_options if x is not None]
            #     data_dict['input']['productOptions'] = product_options

            # data_dict['input']['productType'] = df.iloc[index]['Type']
            data_dict['input']['redirectNewHandle'] = str_to_bool('True')
            data_dict['input']['requiresSellingPlan'] = str_to_bool('False')
            data_dict['input']['seo'] = {'description': df.iloc[index]['SEO Description'],
                                         'title': df.iloc[index]['SEO Title']
                                         }
            data_dict['input']['status'] = df.iloc[index]['Status'].upper()
            data_dict['input']['tags'] = df.iloc[index]['Tags']
            # data_dict['input']['templateSuffix'] = ''
            data_dict['input']['title'] = df.iloc[index]['Title']
            data_dict['input']['vendor'] = df.iloc[index]['Vendor']

            media_list = []
            media = dict()
            if (pd.isna(df.iloc[index]['Link'])) | (df.iloc[index]['Link'] == ''):
                media_list.append(media)
            else:

                links = literal_eval(df.iloc[index]['Link'])
                alt_texts = literal_eval(df.iloc[index]['Image Alt Text'])
                print(links)
                for i in range(0, len(links)):
                    try:
                        media['alt'] = alt_texts[i]
                    except:
                        media['alt'] = ''
                    media['mediaContentType'] = 'IMAGE'
                    media['originalSource'] = links[i]
                    media_list.append(media)
                data_dict['media'] = media_list

            datas.append(data_dict.copy())

    elif mode == 'vup':
        datas = []
        for index in df.index:
            data_dict = {"allowPartialUpdates": False, "productId": '', "variants": list()}
            data_dict['productId'] = df.iloc[index]['id']

            variants = list()
            variant = dict()
            variant['id'] = df.iloc[index]['variant_id']
            variant['barcode'] = str(df.iloc[index]['Variant Barcode'])
            if df.iloc[index]['Variant Compare At Price'] == '':
                pass
            else:
                variant['compareAtPrice'] = round(float(df.iloc[index]['Variant Compare At Price']), 2)

            variant_measure = {'weight': {'unit': 'GRAMS', 'value': 0.0}}
            try:
                variant_measure['weight']['unit'] = weight_unit_mapper[df.iloc[index]['Variant Weight Unit']]
                variant_measure['weight']['value'] = float(df.iloc[index]['Variant Grams'])
            except:
                pass

            variant['inventoryPolicy'] = df.iloc[index]['Variant Inventory Policy'].upper()

            var_inv_item = dict()
            var_inv_item['cost'] = str(df.iloc[index]['Cost per item'])
            var_inv_item['tracked'] = True
            var_inv_item['measurement'] = variant_measure
            var_inv_item['requiresShipping'] = str_to_bool('true')
            var_inv_item['sku'] = df.iloc[index]['Variant SKU']
            var_inv_item['tracked'] = tracker_mapper[df.iloc[index]['Variant Inventory Tracker']]
            variant['inventoryItem'] = var_inv_item

            product_options = [fill_opt_var(df.iloc[index][opt], df.iloc[index][opt.replace('Name', 'Value')]) for opt in opts]
            if (product_options[0] is not None) | (product_options[1] is not None) | (product_options[2] is not None):
                product_options = [x for x in product_options if x is not None]
                variant['optionValues'] = product_options

            try:
                variant['price'] = round(float(df.iloc[index]['Variant Price']), 2)
            except:
                variant['price'] = 0.00

            variant['taxable'] = str_to_bool('true')
            variants.append(variant)
            data_dict['variants'] = variants
            datas.append(data_dict.copy())

    elif mode == 'ap':
        datas = []
        for index in df.index:
            print(df.iloc[index])
            data_dict = {"input": dict(), "media": list()}
            data_dict['input']['id'] = df.iloc[index]['id']
            data_dict['input']['status'] = 'ACTIVE'
            datas.append(data_dict.copy())

    elif mode == 'pp':
        datas = []
        for index in df.index:
            data_dict = {"id": '', "input": list()}
            data_dict['id'] = df.iloc[index]['id']
            publication_ids = ['gid://shopify/Publication/131749707833', 'gid://shopify/Publication/131749773369', 'gid://shopify/Publication/131749838905', 'gid://shopify/Publication/132635131961']
            publication_inputs = list()
            publication_input = dict()
            for publication_id in publication_ids:
                publication_input['publicationId'] = publication_id
                # publication_input['publishDate'] = None
                publication_inputs.append(publication_input.copy())
            data_dict['input'] = publication_inputs

            datas.append(data_dict.copy())

    else:
        print('Mode value is not available')

    print(datas)

    if datas:
        with open(jsonl_filename, 'w') as jsonlfile:
            for item in datas:
                json.dump(item, jsonlfile, default=str)
                jsonlfile.write('\n')


def csv_to_quantities(csv_filename):
    print("Converting csv to quantities...")
    df = pd.read_csv(csv_filename)
    df.fillna('', inplace=True)
    quantities = list()
    for index in df.index:
        if (df.iloc[index]['Variant Inventory Qty'] == '') | (df.iloc[index]['Variant Price'] == 0):
            qty = {
                "inventoryItemId": df.iloc[index]['inventory_id'],
                "locationId": "gid://shopify/Location/73063170105",
                "quantity": 0
            }
        else:
            try:
                variant_inv_qty = int(df.iloc[index]['Variant Inventory Qty'])
            except ValueError:
                variant_inv_qty = int(df.iloc[index]['Variant Inventory Qty'].replace(',', ''))
            qty = {
                "inventoryItemId": df.iloc[index]['inventory_id'],
                "locationId": "gid://shopify/Location/73063170105",
                "quantity": variant_inv_qty
            }
        quantities.append(qty.copy())

    return quantities


def merge_images(product_df: pd.DataFrame, image_df: pd.DataFrame, mode='create'):
    print('Merging images...')
    grouped_image_df = image_df.groupby('Handle')['Link'].agg(list).reset_index()
    print(grouped_image_df)
    result_df = product_df.merge(grouped_image_df, how='left', left_on='Handle', right_on='Handle')
    if mode == 'create':
        result_df.to_csv('data/create_products_with_images.csv', index=False)
    elif mode == 'update':
        result_df.to_csv('data/update_products_with_images.csv', index=False)
    else:
        print('Mode is undefined')


def extract_video_url():
    df = pd.read_excel('data/All_Products_PWHSL.xlsx', usecols=['ProductName', 'FormattedName', 'FullDescription'])
    df['Handle'] = df.apply(lambda x: to_handle(x['ProductName'], alt_title=x['FormattedName']), axis=1)
    sel_df = df[df['FullDescription'].str.contains('https://', na=False)]
    sel_df.to_csv('video_data.csv', index=False)


def deduplicate_handles(df):
    # Create a copy of the dataframe to avoid modifying the original
    df = df.copy()

    # Group by handle and create a cumulative count
    df['handle_count'] = df.groupby('Handle').cumcount()

    # Define a function to modify handles
    def modify_handle(row):
        if row['handle_count'] == 0:
            return row['Handle']
        else:
            return f"{row['Handle']}-{row['handle_count']}"

    # Apply the function to create new handles
    df['Unique Handle'] = df.apply(modify_handle, axis=1)

    # Drop the temporary column
    df = df.drop('handle_count', axis=1)

    return df


# if __name__ == '__main__':
    # to_shopify('data/All_Products_PWHSL.xlsx')
    #
    # product_df = pd.read_csv('data/create_products.csv')
    # image_df = pd.read_csv('data/product_images.csv')
    # merge_images(product_df, image_df=image_df)
    # csv_to_jsonl()

    # chunked_df = chunk_data('../data/create_products.csv', nrows=250)
    # for df in chunked_df:
    #     print(df.head())

    # extract_video_url()
    # pass
