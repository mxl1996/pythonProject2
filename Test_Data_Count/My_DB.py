# encoding: utf-8

import pymysql


class AmanMySQL():
    # 初始化
    def __init__(self, host, user, passwd, dbName):
        self.host = host
        self.user = user
        self.passwd = passwd
        self.dbName = dbName

    # 连接数据库，需要传数据库地址、用户名、密码、数据库名称，默认设置了编码信息
    def connet(self):
        try:
            self.db = pymysql.connect(self.host, self.user, self.passwd, self.dbName, use_unicode=True, charset='utf8')
            self.cursor = self.db.cursor()
        except Exception as e:
            return e

    # 关闭数据库连接
    def close(self):
        try:
            self.cursor.close()
            self.db.close()
        except Exception as e:
            return e

    # 查询操作，查询单条数据
    def get_one(self, sql):
        # res = None
        try:
            self.connet()
            self.cursor.execute(sql)
            res = self.cursor.fetchone()
            self.close()
        except Exception:
            res = None
        return res

    # 查询操作，查询多条数据
    def get_all(self, sql):
        # res = ()
        try:
            self.connet()
            self.cursor.execute(sql)
            res = self.cursor.fetchall()
            self.close()
        except Exception:
            res = ()
        return res

    # 查询数据库对象
    def get_all_obj(self, sql, tableName, *args):
        resList = []
        fieldsList = []
        try:
            if (len(args) > 0):
                for item in args:
                    fieldsList.append(item)
            else:
                fieldsSql = "select COLUMN_NAME from information_schema.COLUMNS where table_name = '%s' and table_schema = '%s'" % (
                    tableName, self.dbName)
                fields = self.get_all(fieldsSql)
                for item in fields:
                    fieldsList.append(item[0])

            # 执行查询数据sql
            res = self.get_all(sql)
            for item in res:
                obj = {}
                count = 0
                for x in item:
                    obj[fieldsList[count]] = x
                    count += 1
                resList.append(obj)
            return resList
        except Exception as e:
            return e

    # 数据库插入、更新、删除操作
    def insert(self, sql):
        return self.__edit(sql)

    def update(self, sql):
        return self.__edit(sql)

    def delete(self, sql):
        return self.__edit(sql)

    def __edit(self, sql):
        # count = 0
        try:
            self.connet()
            count = self.cursor.execute(sql)
            self.db.commit()
            self.close()
        except Exception as e:
            self.db.rollback()
            count = 0
        return count


if __name__ == '__main__':
    # 调用
    if __name__ == '__main__':
        # 使用
        mysql = {
            "host": "127.0.0.1",
            "port": "3306",
            "user": "test",
            "passwd": "123456",
            "dbName": "testMysql",
        }
        useDB = AmanMySQL(mysql["host"], mysql["user"], mysql["passwd"], mysql["dbName"])

        # 查询
        print('************************************select查询*********************************')
        sql = "select case_name from case_manage"  # 不带条件sql
        sqlWithCondition = "select case_name from case_manage where create_at > '2020-08-20 11:18:47'"  # 带条件sql
        getOne = useDB.get_one(sql)
        getAll = useDB.get_all(sql)
        getAllWithCondition = useDB.get_all(sqlWithCondition)
        print('len(getOne)=%d' % (len(getOne),))
        print('getOne=%s' % (getOne,))
        print('len(getAll)=%d' % (len(getAll),))
        print('getAll=%s' % (getAll,))
        print('len(getAllWithCondition)=%d' % (len(getAllWithCondition),))
        print('getAllWithCondition=%s' % (getAllWithCondition,))
        print()

        # 更新
        print('************************************update更新*********************************')
        select_sql_1 = "select case_name from case_manage where case_id=2"
        update_sql = "update case_manage set case_name = '测试测试-06' where case_id=2"
        select_res_1 = useDB.get_one(select_sql_1)
        update_res = useDB.update(update_sql)
        select_res_2 = useDB.get_one(select_sql_1)
        print('数据更新前：%s' % (select_res_1,))
        print('数据更新结果：%s' % (update_res,))
        print('数据更新后：%s' % (select_res_2,))
        print()

        # 删除
        print('************************************delete删除*********************************')
        select_sql_2 = "select count(1) from case_group"
        insert_sql = "delete from case_group where group_name = '测试插入数据'"
        select_res_3 = useDB.get_one(select_sql_2)
        delete_res = useDB.insert(insert_sql)
        select_res_4 = useDB.get_one(select_sql_2)
        print('数据删除前行数：%s' % (select_res_3,))
        print('数据删除结果：%s' % (delete_res,))
        print('数据删除后行数：%s' % (select_res_4,))
        print()

        # 插入
        print('************************************insert插入*********************************')
        select_sql_3 = "select count(1) from case_group"
        insert_sql = "insert  into case_group(group_name,group_code,ext,create_by,update_by,create_at,update_at,status,isDel) values ('测试插入数据','hello','0','管理员','乾隆大帝','2020-06-15 10:37:56','2020-06-16 15:43:03',1,0)"
        select_res_5 = useDB.get_one(select_sql_3)
        insert_res = useDB.insert(insert_sql)
        select_res_6 = useDB.get_one(select_sql_3)
        print('数据插入前行数：%s' % (select_res_5,))
        print('数据插入结果：%s' % (insert_res,))
        print('数据插入后行数：%s' % (select_res_6,))
        print()


