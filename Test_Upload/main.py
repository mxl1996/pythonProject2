import os.path
import time
import json
from get_cookies import GetCookies
from file_num import get_file_num, get_file_name
from request_api import HttpRequest

method = 'POST'

time_now = int(round(time.time() * 1000))
url_path = "http://10.0.9.42:18080/gateway/dataflow/api/v1/dataflows/uploadDataflow/?_timestap="
url = url_path + str(time_now)

path = '/Users/blank/Documents/导入/*.json'
# files = [
#     ('file', ('1.json', open(get_file_num(path=path), 'rb'), 'application/json')),
# ]

headers = {
    'accountId': '1',
    'userId': '1',
    'Cookie': getattr(GetCookies, "Cookie")
}

for i in get_file_name(path):
    files = [('file', (os.path.split(i)[1], open(i, 'rb'), 'application/json'))]
    for j in range(0, get_file_num(path)):
        res = HttpRequest(method=method, url=url, headers=eval(json.dumps(headers)), files=files).http_request()
        if res.cookies:
            setattr(GetCookies, "Cookie", res.cookies)
    print(res.status_code)