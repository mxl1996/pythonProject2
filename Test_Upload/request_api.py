import requests


class HttpRequest:
    def __init__(self, method, url, headers, data=None, cookies=None, files=None):
        self.method = method
        self.url = url
        self.data = data
        self.cookies = cookies
        self.headers = headers
        self.files = files

    def http_request(self):
        try:
            if self.method.upper() == "GET":
                res = requests.get(self.url, params=self.data)
            elif self.method.upper() == "POST":
                res = requests.post(self.url, data=self.data, headers=self.headers, files=self.files)
            elif self.method.upper() == "DELETE":
                res = requests.delete(self.url, data=self.data)
            else:
                print("传入的请求方式有误")
        except Exception as e:
            print(self.method)
            print("请求方式有误，出现的错误是：{0}".format(e))

            raise e
        return res
