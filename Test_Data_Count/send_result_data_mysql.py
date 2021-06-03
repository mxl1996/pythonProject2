import pymysql
from scrapy import settings
from pymysql.converters import escape_string
from read_result_data_kafka import get_kafka_data


class DataToMysql:
    def __init__(self, host, user, password, db, port):
        try:
            self.conn = pymysql.connect(host=host, user=user, password=password, db=db, port=port)
            self.cursor = self.conn.cursor()
        except pymysql.Error as e:
            print("数据库连接信息报错")
            raise e

    def write(self, table_name, info_dict):
        """
        根据table_name与info自动生成建表语句和insert插入语句
        :param table_name: 数据需写入的表名
        :param info_dict: 需要写入的内容，类型为字典
        :return:
        """
        sql_key = ''
        sql_value = ''
        for key in info_dict.keys():  # 生成insert插入语句
            sql_value = (sql_value + '"' + str(info_dict[key]) + '"' + ',')
            sql_key = sql_key + ' ' + key + ','

        try:
            self.cursor.execute(
                "INSERT INTO %s (%s) VALUES (%s)" % (table_name, sql_key[:-1], sql_value[:-1]))
            self.conn.commit()  # 提交当前事务
        except pymysql.Error as e:
            if str(e).split(',')[0].split('(')[1] == "1146":  # 当表不存在时，生成建表语句并建表
                sql_key_str = ''  # 用于数据库创建语句
                columnStyle = ' text'  # 数据库字段类型
                for key in info_dict.keys():
                    sql_key_str = sql_key_str + ' ' + key + columnStyle + ','
                self.cursor.execute("CREATE TABLE %s (%s)" % (table_name, sql_key_str[:-1]))
                self.cursor.execute("INSERT INTO %s (%s) VALUES (%s)" %
                                    (table_name, sql_key[:-1], sql_value[:-1]))
                self.conn.commit()  # 提交当前事务
            else:
                raise e


if __name__ == '__main__':
    for i in get_kafka_data():
        mysql_info = DataToMysql('10.0.9.45', 'Rootmaster', 'Rootmaster@777', 'test', 18103)
        mysql_info.write('test4', i)
        print("写入成功%s" % i)

# str_dir={"a":1,"b":10}
# str_dic={"name": "卢秀荣", "age": 10, "gender": "男", "uv": 357, "num1": 30, "num2": 973,  "id": 46, "stringtime": 1622532999864}
