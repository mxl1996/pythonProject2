import os
import requests
import time

t = int(round(time.time() * 1000))
url1 = "http://10.0.9.42:18080/gateway/dataflow/api/v1/dataflows/uploadDataflow/?_timestap="
url = url1 + str(t)

payload = {}

files = []
list = os.listdir('/Users/blank/Documents/导入/')

for i in range(len(list)):
    dir = '/Users/blank/Documents/导入/' + list[i]
    # print(dir)
    file_i = ('file', (list[i], open(dir, 'rb'), 'application/json'))
    files.append(file_i)
print(files)
headers = {
    'accountId': '1',
    'userId': '1',
    'Cookie': 'aops-sessionId=81159ae0-843b-43bb-b074-1faf10fc840c'
}

response = requests.request("POST", url, headers=headers, data=payload, files=files)

print(response.text)
print(response.status_code)
