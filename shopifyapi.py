from glob import glob
from time import sleep
import httpx
from dataclasses import dataclass
import json
import os
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, date
from converter import csv_to_jsonl, get_handles

load_dotenv()


@dataclass
class ShopifyApp:
    store_name: str = None
    access_token: str = None

    # Create
    ## Session
    def create_session(self):
        print("Creating session...")
        client = httpx.Client()
        headers = {
            'X-Shopify-Access-Token': self.access_token,
            'Content-Type': 'application/json'
        }
        client.headers.update(headers)

        return client


    ## Product
    def create_product(self, client):
        print("Creating product...")
        mutation = '''
                    mutation (
                            $handle: String,
                            $title: String,
                            $vendor: String,
                            $productType: String,
                            $variantTitle: String,
                            $variantPrice: Money,
                            $inventoryManagement: ProductVariantInventoryManagement,
                            $inventoryPolicy: ProductVariantInventoryPolicy,
                            $mediaOriginalSource: String!,
                            $mediaContentType: MediaContentType!
                    )
                    {
                        productCreate(
                            input: {
                                handle: $handle,
                                title: $title,
                                productType: $productType,
                                vendor: $vendor
                                variants: [
                                    {
                                        title: $variantTitle,
                                        price: $variantPrice,
                                        inventoryManagement: $inventoryManagement,
                                        inventoryPolicy: $inventoryPolicy
                                    }
                                ]
                            }
                            media: {
                                originalSource: $mediaOriginalSource,
                                mediaContentType: $mediaContentType
                            }
                        )
                        {
                            product {
                                id
                            }
                        }
                    }
                    '''

        variables = {
            'handle': "BAB063",
            'title': "Xmas Rocks Beavis And Butt-Head Shirt",
            'productType': "Shirts",
            'vendor': "MyStore",
            'variantsTitle': "Default",
            'variantPrice': "79.99",
            'inventoryManagement': 'SHOPIFY',
            'inventoryPolicy': 'DENY',
            'mediaOriginalSource': "https://80steess3.imgix.net/production/products/BAB061/xmas-rocks-beavis-and-butt-head-hoodie.master.png",
            'mediaContentType': 'IMAGE'
            }

        response = client.post(f'https://{self.store_name}.myshopify.com/admin/api/2024-07/graphql.json',
                               json={"query": mutation, 'variables':variables})
        print(response)
        print(response.json())
        print('')

    def create_products(self, client, staged_target):
        print('Creating products...')
        mutation = '''
            mutation ($stagedUploadPath: String!){
                bulkOperationRunMutation(
                    mutation: "mutation call($input: ProductInput!, $media: [CreateMediaInput!]) {
                        productCreate(input: $input, media: $media) {
                            product {
                                id
                                title
                                variants(first: 10) {
                                    edges {
                                        node {
                                            id
                                            title
                                            inventoryQuantity
                                        }
                                    }
                                }
                            }
                            userErrors {
                                message
                                field
                            }
                        }
                    }",
                    stagedUploadPath: $stagedUploadPath
                )   {
                        bulkOperation {
                            id
                            url
                            status
                        }
                        userErrors {
                            message
                            field
                        }
                    }
            }
        '''

        variables = {
            "stagedUploadPath": staged_target['data']['stagedUploadsCreate']['stagedTargets'][0]['parameters'][3]['value']
        }

        response = client.post(f'https://{self.store_name}.myshopify.com/admin/api/2024-07/graphql.json',
                               json={"query": mutation, "variables": variables})

        print(response)
        print(response.json())
        print('')


    def create_variants(self, client, staged_target):
        print('Creating products...')
        mutation = '''
            mutation ($stagedUploadPath: String!){
                bulkOperationRunMutation(
                    mutation: "mutation call($productId: ID!, $strategy:ProductVariantsBulkCreateStrategy, $variants: [ProductVariantsBulkInput!]!) {
                        productVariantsBulkCreate(productId: $productId, strategy: $strategy, variants: $variants) {
                            product {
                                id
                                title
                                variants(first: 10) {
                                    edges {
                                        node {
                                            id
                                            title
                                            inventoryQuantity
                                        }
                                    }
                                }
                            }
                            userErrors {
                                message
                                field
                            }
                        }
                    }",
                    stagedUploadPath: $stagedUploadPath
                )   {
                        bulkOperation {
                            id
                            url
                            status
                        }
                        userErrors {
                            message
                            field
                        }
                    }
            }
        '''

        variables = {
            "stagedUploadPath": staged_target['data']['stagedUploadsCreate']['stagedTargets'][0]['parameters'][3]['value']
        }

        response = client.post(f'https://{self.store_name}.myshopify.com/admin/api/2024-07/graphql.json',
                               json={"query": mutation, "variables": variables})

        print(response)
        print(response.json())
        print('')


    def update_variants(self, client, staged_target):
        print('Creating products...')
        mutation = '''
            mutation ($stagedUploadPath: String!){
                bulkOperationRunMutation(
                    mutation: "mutation call($allowPartialUpdates: Boolean, $productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
                        productVariantsBulkUpdate(allowPartialUpdates: $allowPartialUpdates, productId: $productId, variants: $variants) {
                            product {
                                id
                                title
                                variants(first: 10) {
                                    edges {
                                        node {
                                            id
                                            title
                                            inventoryQuantity
                                        }
                                    }
                                }
                            }
                            userErrors {
                                message
                                field
                            }
                        }
                    }",
                    stagedUploadPath: $stagedUploadPath
                )   {
                        bulkOperation {
                            id
                            url
                            status
                        }
                        userErrors {
                            message
                            field
                        }
                    }
            }
        '''

        variables = {
            "stagedUploadPath": staged_target['data']['stagedUploadsCreate']['stagedTargets'][0]['parameters'][3]['value']
        }

        response = client.post(f'https://{self.store_name}.myshopify.com/admin/api/2024-07/graphql.json',
                               json={"query": mutation, "variables": variables})

        print(response)
        print(response.json())
        print('')

    def update_inventories(self, client, quantities):
        mutation = '''
        mutation inventorySetQuantities($input: InventorySetQuantitiesInput!) {
                             inventorySetQuantities(input: $input)
                             {
                                userErrors {
                                            field
                                            message
                                }
                             }
        }
        '''

        variables = {
            "input": {
                "ignoreCompareQuantity": True,
                "name": "available",
                "quantities": quantities,
                "reason": "correction"
            }
        }

        while 1:
            try:
                response = client.post(f'https://{self.store_name}.myshopify.com/admin/api/2024-07/graphql.json',
                                       json={'query': mutation, 'variables': variables})
                print(response)
                print(response.json())
                print('')
                break
            except Exception as e:
                print(e)


    ## Stage Upload
    def generate_staged_target(self, client):
        print("Creating stage upload...")
        mutation = '''
                    mutation {
                        stagedUploadsCreate(
                            input:{
                                resource: BULK_MUTATION_VARIABLES,
                                filename: "bulk_op_vars.jsonl",
                                mimeType: "text/jsonl",
                                httpMethod: POST
                            }
                        )
                        {
                            userErrors{
                                field,
                                message
                            }
                            stagedTargets{
                                url,
                                resourceUrl,
                                parameters {
                                    name,
                                    value
                                }
                            }
                        }
                    }
                    '''

        response = client.post(f'https://{self.store_name}.myshopify.com/admin/api/2024-07/graphql.json',
                               json={"query": mutation})
        print(response)
        print(response.json())
        print('')
        return response.json()


    # Read
    ## Shop
    def query_shop(self, client):
        print("Fetching shop data...")
        query = '''
                {
                    shop{
                        name
                    }
                }
                '''

        response = client.post(f'https://{self.store_name}.myshopify.com/admin/api/2024-07/graphql.json',
                               json={"query": query})
        print(response)
        print(response.json())
        print('')


    ## Product
    def query_products(self, client, ):
        print("Fetching product data...")
        query = '''
                {
                    products(first: 3) {
                        edges {
                            node {
                                id
                                title
                            }
                        }
                    }
                }
                '''

        response = client.post(f'https://{self.store_name}.myshopify.com/admin/api/2024-07/graphql.json',
                               json={"query": query})
        print(response)
        print(response.json())
        print('')

    def query_locations(self, client):
        print("Fetching product data...")
        query = '''
                {
                    locations(first: 3) {
                        edges {
                            node {
                                id
                            }
                        }
                    }
                }
                '''

        response = client.post(f'https://{self.store_name}.myshopify.com/admin/api/2024-07/graphql.json',
                               json={"query": query})
        print(response)
        print(response.json())
        print('')

        return response.json()

    def get_products_id_by_handle(self, client, handles):
        print('Getting product id...')
        f_handles = ','.join(handles)
        query = '''
            query(
                $query: String
            )
            {
                products(first: 250, query: $query) {
                    edges {
                        node {
                            handle
                            id
                        }
                    }
                    pageInfo {
                        endCursor
                        hasNextPage
                    }
                }
            }
        '''
        variables = {'query': "handle:{}".format(f_handles)}
        response = client.post(f'https://{self.store_name}.myshopify.com/admin/api/2024-07/graphql.json',
                               json={"query": query, 'variables':variables})
        print(response)
        print(response.json())
        print('')

        return response.json()


    def get_variants_id_by_query(self, client, variables):
        print('Getting product id...')
        query = '''
            query(
                $query: String
            )
            {
                productVariants(first: 250, query: $query) {
                    edges {
                        node {
                            product {
                                id
                            }
                            id
                            inventoryItem{
                                id
                            }
                        }
                    }
                    pageInfo {
                        endCursor
                        hasNextPage
                    }
                }
            }
        '''
        response = client.post(f'https://{self.store_name}.myshopify.com/admin/api/2024-07/graphql.json',
                               json={"query": query, 'variables':variables})
        print(response)
        print(response.json())
        print('')

        return response.json()


    def get_products_id_by_sku(self, client, skus):
        print('Getting product id...')
        query = '''
            query(
                $query: String
            )
            {
                products(first: 250, query: $query) {
                    edges {
                        node {
                            handle
                            id
                        }
                    }
                    pageInfo {
                        endCursor
                        hasNextPage
                    }
                }
            }
        '''
        variables = {'query': "sku:{}".format(skus)}
        response = client.post(f'https://{self.store_name}.myshopify.com/admin/api/2024-07/graphql.json',
                               json={"query": query, 'variables':variables})
        print(response)
        print(response.json())
        print('')

        return response.json()


    def get_products_id_by_query(self, client, variables):
        print('Getting product id...')
        query = '''
            query(
                $query: String
            )
            {
                products(first: 250, query: $query) {
                    edges {
                        node {
                            handle
                            id
                            publishedAt
                        }
                    }
                    pageInfo {
                        endCursor
                        hasNextPage
                    }
                }
            }
        '''
        response = client.post(f'https://{self.store_name}.myshopify.com/admin/api/2024-07/graphql.json',
                               json={"query": query, 'variables':variables})
        print(response)
        print(response.json())
        print('')

        return response.json()


    def query_inventories(self):
        print('Getting inventories...', end='')
        query = '''
            query inventoryItems {
                inventoryItems(first: 250) {
                    edges {
                        node {
                            id
                            tracked
                            sku
                        }
                    }
                }
            }
        '''
        response = client.post(f'https://{self.store_name}.myshopify.com/admin/api/2024-07/graphql.json',
                               json={"query": query})
        print(response)
        print(response.json())
        print('')

        return response.json()


    # Update
    ## Product
    def update_product(self, client, handle, tags):
        id = self.query_product_by_handle(client, handle=handle)
        mutation = '''
        mutation productUpdate($input: ProductInput!) {
                             productUpdate(input: $input)
                             {
                                userErrors {
                                            field
                                            message
                                }
                             }
        }
        '''
        variables = {
            "input": {
                "id": id,
                "tags": tags
            }
        }

        while 1:
            try:
                response = client.post(f'https://{self.store_name}.myshopify.com/admin/api/2024-07/graphql.json',
                                       json={'query': mutation, 'variables': variables})
                print(response)
                print(response.json())
                print('')
                break
            except Exception as e:
                print(e)


    def update_products(self, client, staged_target):
        print('Updating products...')
        mutation = '''
            mutation ($stagedUploadPath: String!){
                bulkOperationRunMutation(
                    mutation: "mutation call($input: ProductInput!, $media: [CreateMediaInput!]) {
                        productUpdate(input: $input, media: $media) {
                            product {
                                id
                                title
                                variants(first: 10) {
                                    edges {
                                        node {
                                            id
                                            title
                                            inventoryQuantity
                                        }
                                    }
                                }
                            }
                            userErrors {
                                message
                                field
                            }
                        }
                    }",
                    stagedUploadPath: $stagedUploadPath
                )   {
                        bulkOperation {
                            id
                            url
                            status
                        }
                        userErrors {
                            message
                            field
                        }
                    }
            }
        '''

        variables = {
            "stagedUploadPath": staged_target['data']['stagedUploadsCreate']['stagedTargets'][0]['parameters'][3]['value']
        }

        response = client.post(f'https://{self.store_name}.myshopify.com/admin/api/2024-07/graphql.json',
                               json={"query": mutation, "variables": variables})

        print(response)
        print(response.json())
        print('')


    # Bulk operation support
    def csv_to_jsonl(self, csv_filename, jsonl_filename):
        print("Converting csv to jsonl file...")
        df = pd.read_csv(csv_filename, nrows=2)

        # Create formatted dictionary
        datas = []
        for index in df.index:
            data_dict = {"input": dict(), "media": dict()}
            data_dict['input']['handle'] = df.iloc[index]['Handle']
            data_dict['input']['title'] = df.iloc[index]['Title']
            data_dict['input']['descriptionHtml'] = df.iloc[index]['Body (HTML)']
            data_dict['input']['vendor'] = df.iloc[index]['Vendor']
            data_dict['input']['productCategory'] = df.iloc[index]['Product Category']
            data_dict['input']['productType'] = df.iloc[index]['Type']
            data_dict['input']['tags'] = df.iloc[index]['Tags']
            data_dict['input']['options'] = [df.iloc[index]['Option1 Name'],
                                             df.iloc[index]['Option2 Name'],
                                             df.iloc[index]['Option3 Name']
                                             ]

            # Convert symbol to unit
            if df.iloc[index]['Variant Weight Unit'] == "g":
                df.loc[index, 'Variant Weight Unit'] = "GRAMS"
            elif df.iloc[index]['Variant Weight Unit'] == "kg":
                df.loc[index, 'Variant Weight Unit'] = "KILOGRAMS"
            elif df.iloc[index]['Variant Weight Unit'] == "lb":
                df.loc[index, 'Variant Weight Unit'] = "POUNDS"


            # Variant Attributes
            data_dict['input']['variants'] = [
                {'sku': df.iloc[index]['Variant SKU'],
                 'options': [
                     df.iloc[index]['Option1 Value'],
                     df.iloc[index]['Option2 Value'],
                     df.iloc[index]['Option3 Value']
                 ],
                 'weight': int(df.iloc[index]['Variant Grams']),
                 'weightUnit': df.iloc[index]['Variant Weight Unit'],
                 'inventoryManagement': df.iloc[index]['Variant Inventory Tracker'].upper(),
                 'inventoryPolicy': df.iloc[index]['Variant Inventory Policy'].upper(),
                 'price': str(df.iloc[index]['Variant Price']),
                 'compareAtPrice': str(df.iloc[index]['Variant Compare At Price']),
                 'requiresShipping': bool(df.iloc[index]['Variant Requires Shipping']),
                 'taxable': bool(df.iloc[index]['Variant Taxable']),
                 'imageSrc': f"https:{df.iloc[index]['Image Src']}",
                 'title': 'Default'
                 }
            ]

            data_dict['input']['giftCard'] = bool(df.iloc[index]['Gift Card'])
            data_dict['input']['status'] = df.iloc[index]['Status'].upper()
            data_dict['media'] = {'originalSource': f"https:{df.iloc[index]['Image Src']}", 'mediaContentType': 'IMAGE'}

            datas.append(data_dict.copy())
        print(datas)
        with open(os.path.join(jsonl_filename), 'w') as jsonlfile:
            for item in datas:
                json.dump(item, jsonlfile)
                jsonlfile.write('\n')


    def upload_jsonl(self, staged_target, jsonl_path):
        print("Uploading jsonl file to staged path...")
        url = staged_target['data']['stagedUploadsCreate']['stagedTargets'][0]['url']
        parameters = staged_target['data']['stagedUploadsCreate']['stagedTargets'][0]['parameters']
        files = dict()
        for parameter in parameters:
            files[f"{parameter['name']}"] = (None, parameter['value'])
        files['file'] = open(jsonl_path, 'rb')

        # with httpx.Client(timeout=None, follow_redirects=True) as sess:
        response = httpx.post(url, files=files)

        print(response)
        print(response.content)
        print('')

    def webhook_subscription(self, client):
        print("Subscribing webhook...")
        mutation = '''
                    mutation {
                        webhookSubscriptionCreate(
                            topic: BULK_OPERATIONS_FINISH
                            webhookSubscription: {
                                format: JSON,
                                callbackUrl: "https://12345.ngrok.io/"
                                }
                        )
                        {
                            userErrors {
                                field
                                message
                            }
                            webhookSubscription {
                                id
                            }
                        }
                    }
        '''

        response = client.post(f'https://{self.store_name}.myshopify.com/admin/api/2024-07/graphql.json',
                               json={"query": mutation})
        print(response)
        print(response.json())
        print('')

    def pool_operation_status(self,client):
        print("Pooling operation status...")
        query = '''
                    query {
                        currentBulkOperation(type: MUTATION) {
                            id
                            status
                            errorCode
                            createdAt
                            completedAt
                            objectCount
                            fileSize
                            url
                            partialDataUrl
                        }
                    }
                '''

        response = client.post(f'https://{self.store_name}.myshopify.com/admin/api/2024-07/graphql.json',
                               json={"query": query})
        print(response)
        print(response.json())
        print('')

        return response.json()


    def import_bulk_data(self, client, csv_filename, jsonl_filename):
        self.csv_to_jsonl(csv_filename=csv_filename, jsonl_filename=jsonl_filename)
        staged_target = self.generate_staged_target(client)
        self.upload_jsonl(staged_target=staged_target, jsonl_path=jsonl_filename)
        self.create_products(client, staged_target=staged_target)

    # def create_collection(self, client):
    #     print('Creating collection...')
    #     mutation = '''
    #     mutation ($descriptionHtml: String!, $title: String!){
    #         collectionCreate(
    #             input: {
    #                 descriptionHtml: $descriptionHtml
    #                 title: $title
    #             }
    #         )
    #         {
    #             collection{
    #                 id
    #                 productsCount
    #             }
    #             userErrors{
    #                 field
    #                 message
    #             }
    #         }
    #     }
    #     '''
    #
    #     variables = {
    #         'descriptionHtml': "<p>This Collection is created as a training material</p>",
    #         'title': "Collection1"
    #     }
    #
    #     response = client.post(f'https://{self.store_name}.myshopify.com/admin/api/2024-07/graphql.json',
    #                            json={"query": mutation, 'variables': variables})
    #     print(response)
    #     print(response.json())
    #     print('')

    def get_publications(self, client):
        print('Getting publications list...')
        query = '''
        query {
            publications(first: 10){
                edges{
                    node{
                        id
                        name
                    }
                }
            }
        }
        '''

        response = client.post(f'https://{self.store_name}.myshopify.com/admin/api/2024-07/graphql.json',
                               json={"query": query})
        print(response)
        print(response.json())
        print('')

    def publish_collection(self, client):
        print('Publishing collection...')
        mutation = '''
        mutation {
            collectionPublish(
                input: {
                    id: "",
                    collectionPublications: {
                        publicationId: "gid://shopify/Publication/178396725562"
                        }
                    }
                )
            )
            {
                collectionPublications{
                    publishDate
                }
                userErrors{
                    field
                    message
            }
        }    
        '''

        response = client.post(f'https://{self.store_name}.myshopify.com/admin/api/2024-07/graphql.json',
                               json={"query": mutation})
        print(response)
        print(response.json())
        print('')

    def get_collections(self, client, cursor=None):
        print('Getting collection list...')
        if cursor:
            query = '''
                    query getAllCollection($after: String){
                        collections(first: 250, after: $after){
                            nodes{
                                  handle
                                  id
                                  title
                            }
                            pageInfo{
                                     endCursor
                                     hasNextPage
                            }

                        }
                    }
            '''
        else:
            query = '''
                    query {
                        collections(first: 250){
                            nodes{
                                  handle
                                  id
                                  title
                            }
                            pageInfo{
                                     endCursor
                                     hasNextPage
                            }

                        }
                    }
            '''
        variables = {'after': cursor}

        response = client.post(f'https://{self.store_name}.myshopify.com/admin/api/2024-07/graphql.json',
                               json={"query": query, "variables": variables})
        print(response)
        print(response.json())
        print('')

        return response.json()

    def check_bulk_operation_status(self, client, bulk_operation_id):
        query = f'''
            query {{
                node(id: "{bulk_operation_id}") {{
                    ... on BulkOperation {{
                        id
                        status
                    }}
                }}
            }}
        '''

        response = client.post(f'https://{self.store_name}.myshopify.com/admin/api/2024-07/graphql.json',
                               json={"query": query})

        response_data = response.json()
        status = response_data['data']['node']['status']
        return status

    def products_to_collection(self, client):
        pass

    def get_file(self, client, created_at, updated_at, after):
        print("Fetching file data...")
        if after == '':
            query = '''
                    query getFilesByCreatedAt($query:String!){
                        files(first:250, query:$query) {
                            edges {
                                node {
                                    ... on MediaImage {
                                        id
                                        alt
                                        image {
                                            id
                                            altText
                                        }
                                    }
                                }
                            }
                            pageInfo{
                                hasNextPage
                                endCursor
                            }
                        }
                    }
                    '''

            variables = {'query': "(created_at:>={}) AND (updated_at:<={})".format(created_at, updated_at)}

        else:

            query = '''
            query getFilesByCreatedAt($query:String!, $after:String!){
                files(first:250, after:$after, query:$query) {
                    edges {
                        node {
                            ... on MediaImage {
                                id
                                alt
                                image {
                                    id
                                    altText
                                }
                            }
                        }
                    }
                    pageInfo{
                        hasNextPage
                        endCursor
                    }
                }
            }
            '''

            variables = {'query': "(created_at:>={}) AND (updated_at:<={})".format(created_at, updated_at),
                         'after': after}
        retries = 0
        while retries <3:
            response = client.post(f'https://{self.store_name}.myshopify.com/admin/api/2024-07/graphql.json',
                                   json={'query': query, 'variables': variables})
                                   # json={'query': query})
            try:
                result = response.json()
                print(result['data'])
                break
            except:
                retries += 1
                sleep(1)
                continue

        return result

    def bulk_get_file(self):
        # print("Getting bulk file...")
        # mutation = """
        # mutation bulkOperationRunQuery($query: String!) {
        #     bulkOperationRunQuery(query: $query) {
        #         bulkOperation {
        #             # BulkOperation fields
        #             }
        #         userErrors {
        #             field
        #             message
        #         }
        #     }
        # }
        # """
        #
        # response = client.post(f'https://{self.store_name}.myshopify.com/admin/api/2024-07/graphql.json',
        #                        json={'query': mutation, 'variables': variables})
        pass

    def edit_file(self, client, file_id, file_name, altText):
        print("Update filename...")
        extention = '.' + altText.rsplit('.', 1)[-1]
        mutation = '''
                mutation fileUpdate($files:[FileUpdateInput!]!)
                {
                    fileUpdate(files: $files) {
                        files {
                            id
                        }
                        userErrors {
                            field
                            message
                        }
                    }
                }
                '''

        variables = {
            'files': [
                {
                    'id': file_id,
                    'filename': file_name + extention
                }
            ]
        }

        print(variables)
        while 1:
            try:
                response = client.post(f'https://{self.store_name}.myshopify.com/admin/api/2024-07/graphql.json',
                                       json={'query': mutation, 'variables': variables})
                print(response)
                print(response.json())
                print('')
                break
            except Exception as e:
                print(e)

    def get_variants(self, client, sku):
        print("Getting variant...")
        query = '''
                query getVariantsBySKU($query:String!){
                    productVariants(first:250, query:$query) {
                        edges {
                            node {
                                id
                                }
                            }
                        }
                    }
                '''

        variables = {'query': "sku:{}".format(sku)}

        retries = 0
        while retries < 3:
            response = client.post(f'https://{self.store_name}.myshopify.com/admin/api/2024-07/graphql.json',
                                   json={'query': query, 'variables': variables})
            try:
                result = response.json()
                print(result)
                break
            except Exception as e:
                print(e)
                retries += 1
                sleep(1)
                continue

        return result['data']['productVariants']['edges'][0]['node']['id']


    def create_collection(self, client, descriptionHtml, image_src, title, appliedDisjuntively, column, relation, condition):
        # product_id = pd.read_csv(f'Product_By_Category2/ {title}.csv', usecols='product_id')['product_id'].tolist()

        mutation = '''
        mutation createCollection($input: CollectionInput!) {
                             collectionCreate(input: $input)
                             {
                                collection {
                                            id
                                            title
                                }
                                userErrors {
                                            field
                                            message
                                }
                             }
        } 
        '''
        variables = {
                     "input": {
                               "descriptionHtml": descriptionHtml,
                               # "image": {
                               #           "src": image_src
                               #          },
                               # "products": product_id,
                               "ruleSet": {"appliedDisjunctively": appliedDisjuntively,
                                           "rules": {"column": column,
                                                     "relation": relation,
                                                     "condition": condition
                                                     }
                                           },
                               "title": title
                     }
        }

        while 1:
            try:
                response = client.post(f'https://{self.store_name}.myshopify.com/admin/api/2024-07/graphql.json',
                                       json={'query': mutation, 'variables': variables})
                print(response)
                print(response.json())
                print('')
                break
            except Exception as e:
                print(e)

    def check_access_scopes(self, client):
        print("Checking access scopes...")
        query = '''
            query {
                appInstallation {
                    accessScopes {
                        handle
                        description
                    }
                }
            }
        '''

        response = client.post(f'https://{self.store_name}.myshopify.com/admin/api/2025-04/graphql.json',
                               json={"query": query})
        print(response)
        print(response.json())
        print('')
        print("Access scopes collected!")

    def query_product_by_handle(self, client, handle):
        # print(handle)
        print("Fetching product data by handle...")
        query = '''
                        query getProductDetailByHandle($handle:String!){
                            productByHandle(handle: $handle) {
                                id
                                status
                                publishedAt
                                resourcePublicationOnCurrentPublication{
                                    isPublished
                                    publishDate
                                }
                            }
                        }
                        '''

        variables = {'handle': "{}".format(handle)}

        response = client.post(f'https://{self.store_name}.myshopify.com/admin/api/2025-04/graphql.json',
                                   json={'query': query, 'variables': variables})

        print(response)
        print(response.json())
        print('')


        return response.json()


    def import_status(self, client):
        # Check Bulk Import status
        print('Checking')
        global s
        response = s.pool_operation_status(client)
        if response['data']['currentBulkOperation']['status'] == 'COMPLETED':
            created = True
        else:
            sleep(10)
            created = False

        return created


    def publish_unpublish(self, client, staged_target):
        print('Publishing products...')
        mutation = '''
            mutation ($stagedUploadPath: String!){
                bulkOperationRunMutation(
                    mutation: "mutation call($id: ID!, $input: [PublicationInput!]!) {
                        publishablePublish(id: $id, input: $input) {
                            publishable {
                                availablePublicationsCount {
                                    count
                                }
                                resourcePublicationsCount {
                                    count
                                }
                            }
                            shop {
                                publicationCount
                            }
                            userErrors {
                                message
                                field
                            }
                        }
                    }",
                    stagedUploadPath: $stagedUploadPath
                )   {
                        bulkOperation {
                            id
                            url
                            status
                        }
                        userErrors {
                            message
                            field
                        }
                    }
            }
        '''

        variables = {
            "stagedUploadPath": staged_target['data']['stagedUploadsCreate']['stagedTargets'][0]['parameters'][3]['value']
        }

        response = client.post(f'https://{self.store_name}.myshopify.com/admin/api/2024-07/graphql.json',
                               json={"query": mutation, "variables": variables})

        print(response)
        print(response.json())
        print('')


    def remove_scheduled_publish_date_updated(self, client, product_id, publication_id=None):
        print(f'Removing scheduled publish date for product {product_id}...')
        mutation = '''
        mutation productPublishOnPublication($id: ID!, $input: ProductPublishInput!) {
            productPublishOnPublication(id: $id, input: $input) {
                product {
                    id
                    title
                    publishedAt
                    resourcePublicationOnCurrentPublication {
                        publishDate
                    }
                }
                userErrors {
                    field
                    message
                }
            }
        }
        '''
        variables = {
            "id": product_id,
            "input": {
                "publicationId": publication_id,
                "publishDate": None
            }
        }

        response = client.post(f'https://{self.store_name}.myshopify.com/admin/api/2024-07/graphql.json',
                               json={"query": mutation, "variables": variables})

        result = response.json()
        print(json.dumps(result, indent=2))

        if 'errors' in result:
            print(f"Error: {result['errors']}")
            return None

        updated_product = result['data']['productPublishOnPublication']['product']
        if updated_product['resourcePublicationOnCurrentPublication']['publishDate'] is None:
            print(f"Successfully removed scheduled publish date for product {updated_product['title']}")
        else:
            print(f"Failed to remove scheduled publish date for product {updated_product['title']}")

        return updated_product


    def get_metafields(self, client):
        query = '''
            query {
                metafieldDefinitions(first: 250, ownerType: PRODUCT) {
                    edges {
                        node {
                            id
                            name
                        }
                    }
                }
            }
        '''

        response = client.post(f'https://{self.store_name}.myshopify.com/admin/api/2025-04/graphql.json',
                                   json={'query': query})

        print(response)
        print(response.json())
        print('')

        return response.json()

if __name__ == '__main__':

    s = ShopifyApp(store_name=os.getenv('STORE_NAME'), access_token=os.getenv('ACCESS_TOKEN'))
    client = s.create_session()

    # handles = ['38-exit-ez-fx-kit', 'rest-in-peace-cross-tombstone']
    # response = s.get_products_id_by_handle(client, handles=handles)
    # print(response)
    # s.get_metafields(client)

    # activate product
    # has_next_page = True
    # while has_next_page:
    #     variables = {'query': "status:{}".format('DRAFT')}
    #     response = s.get_products_id_by_query(client=client, variables=variables)
    #     has_next_page = response['data']['products']['pageInfo']['hasNextPage']
    #     datas = response['data']['products']['edges']
    #     records = [data['node'] for data in datas]
    #     df = pd.DataFrame.from_records(records)
    #     df.to_csv('data/draft_products_id.csv', index=False)
    #     csv_to_jsonl(csv_filename='data/draft_products_id.csv', jsonl_filename='bulk_op_vars.jsonl', mode='ap')
    #     staged_target = s.generate_staged_target(client)
    #     s.upload_jsonl(staged_target=staged_target, jsonl_path="bulk_op_vars.jsonl")
    #     s.update_products(client, staged_target=staged_target)
    #     created = False
    #     while not created:
    #         created = s.import_status(client)


    # publish unpublish
    # has_next_page = True
    # while has_next_page:
    #     variables = {'query': "published_status:{} AND status:{}".format('unpublished', 'ACTIVE')}
    #     response = s.get_products_id_by_query(client=client, variables=variables)
    #     has_next_page = response['data']['products']['pageInfo']['hasNextPage']
    #     datas = response['data']['products']['edges']
    #     records = [data['node'] for data in datas]
    #     df = pd.DataFrame.from_records(records)
    #     df.to_csv('data/unpublished_products_id.csv', index=False)
    #     csv_to_jsonl(csv_filename='data/unpublished_products_id.csv', jsonl_filename='bulk_op_vars.jsonl', mode='pp')
    #     staged_target = s.generate_staged_target(client)
    #     s.upload_jsonl(staged_target=staged_target, jsonl_path="bulk_op_vars.jsonl")
    #     s.publish_unpublish(client, staged_target=staged_target)
    #     created = False
    #     while not created:
    #         created = s.import_status(client)


    # s.query_product_by_handle(client, handle='812-8-82')

    # s.remove_scheduled_publish_date_updated(client, 'gid://shopify/Product/7625659777081')

    # s.query_locations(client)
    # path = './Product_By_Category2/*.csv'
    # filenames = glob(path)
    # print(filenames)
    # for filename in filenames:
    #     df = pd.read_csv(filename)
    #     df['product_id'] = df.apply(lambda x: s.query_product_by_handle(client, x['Handle']), axis=1)
    #     df.to_csv(filename, index=False)


    # df = pd.read_csv('Collection_Feeds_rev2.csv')
    # df.apply(lambda x: s.create_collection(client=client, descriptionHtml=x['descriptionHtml'],
    #                                        image_src=x['imageSrc'], title=x['title'],
    #                                        appliedDisjuntively=x['appliedDisjuntively'], column=x['column'],
    #                                        relation=x['relation'], condition=x['title']
    #                                        ),
    #          axis=1
    #          )

    # df = pd.read_csv('products_update_tag_rev1.csv')
    # df = df[df['Handle'].str.contains('lifting-forklift-frame')]
    # df.apply(lambda x: s.update_product(client=client, handle=x['Handle'], tags=x['Tags']), axis=1)

    # df = pd.read_csv('barcode_feed_5.csv')
    # print(df.columns)
    # df.apply(lambda x: s.update_variants(client=client, sku=x['Variant SKU'], barcode=x['New Barcode']), axis=1)

    # s.check_access_scopes(client)

    # rules = [
    #     {"column": "TITLE", "relation": "CONTAINS", "condition": "Animal"},
    #     {"column": "TITLE", "relation": "CONTAINS", "condition": "Horse"},
    #     {"column": "TITLE", "relation": "CONTAINS", "condition": "Llama"}
    # ]
    # s.create_collection(client,
    #                     descriptionHtml='<p>Animal Ride On Toy Description</p>',
    #                     image_src="https://cdn.shopify.com/s/files/1/2245/9711/products/s-l500_4224ddbe-dd74-4287-8057-3521271e1e6f.jpg?v=1696392199",
    #                     rules=rules, title="Animal Ride On Toy")

    # s.get_variants(client, '294329484754-none-$0-none-$0-')
    # s.update_variants(client=client, sku='294329484754-none-$0-none-$/0-', price='1.00', compareAtPrice='2.00')

    # updated_at = '2023-09-24T00:00:00Z'
    # created_at = '2023-09-23T00:00:00Z'
    # file_data = s.get_file(client, created_at=created_at, updated_at=updated_at, after='')
    # print(file_data)

    # print(file_data)
    # file_id = file_data['data']['files']['edges'][0]['node']['id']
    # print(file_id)
    # s.edit_file(client, file_id=file_id)
    # s.query_shop(client)
    # s.query_product(client)
    # s.create_product(client)
    # s.csv_to_jsonl(csv_filename='result.csv', jsonl_filename='test2.jsonl')
    # staged_target = s.generate_staged_target(client)
    # s.upload_jsonl(staged_target=staged_target, jsonl_path="D:/Naru/shopifyAPI/bulk_op_vars.jsonl")
    # s.create_products(client, staged_target=staged_target)
    # s.import_bulk_data(client=client, csv_filename='result.csv', jsonl_filename='bulk_op_vars.jsonl')
    # s.webhook_subscription(client)
    # s.create_collection(client)
    # s.query_products(client)
    # s.get_publications(client)

    # ==============================================get all collections===================================
    # has_next_page = True
    # cursor = None
    # results = list()
    # while has_next_page:
    #     response = s.get_collections(client, cursor=cursor)
    #     records = response['data']['collections']['nodes']
    #     results.extend(records)
    #     has_next_page = response['data']['collections']['pageInfo']['hasNextPage']
    #     cursor = response['data']['collections']['pageInfo']['endCursor']
    # results_df = pd.DataFrame.from_records(results)
    # results_df.to_csv('data/existing_collection_list.csv', index=False)

    # ============================================get product id by handle===============================
    # collection_df = pd.read_csv('data/collection_list.csv')
    # chunked_handles = get_handles('data/collection_list.csv')
    # product_ids = list()
    # for handles in chunked_handles:
    #     product_ids.extend(s.get_products_id_by_handle(client, handles=handles)['data']['products']['edges'])
    # print(f'count:{len(product_ids)}')
    # extracted_product_ids = [x['node'] for x in product_ids]
    # product_id_handle_df = pd.DataFrame.from_records(extracted_product_ids)
    # product_id_handle_df.to_csv('data/product_as_collection_ids.csv', index=False)

    # ============================================get inventories===============================
    # s.query_inventories()

    # s.query_product_by_handle(client, handle='game-of-thrones-drogon-prop')

    # s.pool_operation_status(client)
    # print(s.check_bulk_operation_status(client, bulk_operation_id='gid://shopify/BulkOperation/3252439023930'))
    # handles = ['rest-in-peace-cross-tombstone-1', 'trick-or-treat-yo-self-makeup-bag', '38-exit-ez-fx-kit-1']
    # f_handles = ','.join(handles)
    # s.get_products_id_by_handle(client, handles=f_handles)

    # ============================ create collections ==================================
    # df = pd.read_csv('data/not available collection.csv')
    # df.loc[:, 'appliedDisjuntively'] = True
    # df.loc[:, 'imageSrc'] = ''
    # df.loc[:, 'title'] = df['Collection Name'].apply(lambda x: x.strip())
    # df.loc[:, 'column'] = 'TAG'
    # df.loc[:, 'relation'] = 'EQUALS'
    # df.loc[:, 'caption'] = ''

    # # print(df)
    # df.apply(
    #     lambda x: s.create_collection(
    #         client=client,
    #         descriptionHtml=x['caption'],
    #         image_src=x['imageSrc'],
    #         title=x['title'],
    #         appliedDisjuntively=x['appliedDisjuntively'],
    #         column=x['column'],
    #         relation=x['relation'],
    #         condition=x['title']
    #     ),
    #     axis=1
    # )
