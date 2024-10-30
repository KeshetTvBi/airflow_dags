import requests
import openpyxl
import io
import pandas


class graph_client:
    def __init__(self,client_id,client_secret , tennet_id,scope = 'https://graph.microsoft.com/.default' , grant_type = ' client_credentials'):
        self.client_id = client_id
        self.client_secret = client_secret
        self.tennet_id = tennet_id
        self.scope = scope
        self.grant_type = grant_type
        self.token = None
        self.get_token()

    def get_token(self):
        payload_obj = {'client_id':self.client_id,'client_secret':self.client_secret,'tennet_id':self.tennet_id,'scope':self.scope,'grant_type':self.grant_type}
        payload_uri = '&'.join([f'{k}={v}' for k,v in payload_obj.items()])
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        req = requests.get(f'https://login.microsoftonline.com/{self.tennet_id}/oauth2/v2.0/token' , headers=headers,data=payload_uri)
        try:
            self.token = req.json()['access_token']
        except Exception as e:
            print('error getting token')

    def make_request(self,url,method,headers={},payload={}):
        if 'Authorization' not in headers.keys():
            headers['Authorization'] = f'Bearer {self.token}'
        return requests.request(method,url,data=payload,headers=headers)

    def get_site_by_path(self,host,site_path):
        url = f'https://graph.microsoft.com/v1.0/sites/{host}:/{site_path}'
        return self.make_request(url,'GET').json()

    def list_drives_in_site(self,site_id_or_path,host='',lookup_type='id'):
        if lookup_type == 'path':
            site_id_or_path = self.get_site_by_path(host,site_id_or_path)['id']
        url = f'https://graph.microsoft.com/v1.0/sites/{site_id_or_path}/drives'
        return self.make_request(url,'GET').json()

    def create_drive_url_path(self,path):
        if not path.startswith('root'):
            path = 'root:/' + path.strip('/')
        return path.strip('/')

    def list_items_in_drive_path(self,drive_id,path='root'):
        path = self.create_drive_url_path(path)
        url = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/{path}:/children'
        return self.make_request(url,'GET').json()

    def get_file_contents(self,drive_id,path):
        path = self.create_drive_url_path(path)
        url = f'https://graph.microsoft.com/v1.0/drives/{drive_id}/{path}:/content'
        req = self.make_request(url, 'GET')
        byte_content = req.content
        try:
            byte_content.decode('utf-8')
            return 'string',req.text
        except UnicodeError:
            return 'binary',byte_content

    def get_excel_file_data(self,drive_id,path,sheet_name,data_format='dict_list',first_row_is_header = True):
        raw_data = self.get_file_contents(drive_id,path)
        wb = openpyxl.load_workbook(io.BytesIO(raw_data[1]))
        sheet = wb[sheet_name]

        if data_format == 'dict_list':
            headers = [x.value for x in sheet[1]]
            if not first_row_is_header:
                headers = [f'val_{i+1}' for i in range(len(headers))]
                return [{headers[i]: cell.value for i, cell in enumerate(row)} for row in sheet.rows]
            return [{headers[i]:cell.value for i,cell in enumerate(row)} for row in [x for x in sheet.rows][1:]]
        elif data_format =='list_of_lists':
            if not first_row_is_header:
                return [[cell.value for i, cell in enumerate(row)] for row in sheet.rows]
            else:
                return [[cell.value for i, cell in enumerate(row)] for row in [x for x in sheet.rows][1:]]
        elif data_format == 'pandas':
            if first_row_is_header:
                df = pandas.DataFrame([[cell.value for cell in row] for row in sheet.rows])
                df, df.columns = df[1:] , df.iloc[0]
                return df
            else:
                return pandas.DataFrame([[cell.value for cell in row] for row in sheet.rows])


    def download_file(self,drive_id, drive_path,local_path):
        data_type,file_content = self.get_file_contents(drive_id,drive_path)

        if data_type == 'string':
            with open(local_path, 'w' , encoding='utf-8') as f:
                f.write(file_content)
        elif data_type == 'binary':
            with open(local_path, 'wb' ) as f:
                f.write(file_content)