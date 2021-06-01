import time

import pymysql
from faker import Faker

conn = pymysql.connect(host="10.0.9.45", port=18103, user="Rootmaster", password="Rootmaster@777", db="test",
                       charset="utf8")

cursor = conn.cursor()
# sql1 = """drop table if exists dm_test"""
# sql2 = """
# create table dm_test(
# user_id int primary key auto_increment,
# pro_name varchar(255),
# use_fee double(255,2),
# phone_num varchar(255),
# com_name  varchar(255),
# day datetime not null
# )
# """
# cursor.execute(sql1)
# cursor.execute(sql2)
fake = Faker("zh-CN")
# 根据需求设计想要的数据，以及随机生成的数据条数
for i in range(100,200):
    sql = """insert into test(c1,c2) values ('%s','%s')""" % (fake.name(),i)
    # 执行sql并插入到数据库中
    cursor.execute(sql)
    time.sleep(1)
    print("插入成功")


conn.commit()
cursor.close()
conn.close()

