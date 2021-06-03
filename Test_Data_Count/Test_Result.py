from My_DB import AmanMySQL

mysql = {
    "host": "10.0.9.45",
    "port": "18103",
    "user": "Rootmaster",
    "passwd": "Rootmaster@777",
    "dbName": "test",
}
sql2="select * from test3;"
sql = "select id,name,age from (select id ,name ,age from test2 union all select id,name,age from test3) test1 group by id,age having count(*)=1 order by id"
useDB = AmanMySQL(mysql["host"], mysql["user"], mysql["passwd"], mysql["dbName"])
getall=useDB.get_all(sql2)
print(getall)
