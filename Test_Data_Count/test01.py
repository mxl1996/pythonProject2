# encoding=utf-8
import datetime
import configparser
import re
import pymysql
from vertica_python import connect
import vertica_python
import json
from confluent_kafka import Consumer, KafkaError
import csv
import logging
import os
import time
import signal
import sys

# 写日志
logging.basicConfig(filename=os.path.join(os.getcwd(), 'log_tracking.txt'), level=logging.WARN, filemode='a',
                    format='%(asctime)s - %(levelname)s: %(message)s')


def writeErrorLog(errSrc, errType, errMsg):
    try:
        v_log_file = 'err_tracking.log';
        v_file = open(v_log_file, 'a')
        v_file.write(datetime.datetime.strftime(datetime.datetime.now(),
                                                "%Y-%m-%d %H:%M:%S") + " - " + errSrc + " - " + errType + " : " + errMsg + '\n')
        v_file.flush()
    except Exception as data:
        v_err_file = open('err_tracking.log', 'a')
        v_err_file.write(str(data) + '\n')
        v_err_file.write(datetime.datetime.strftime(datetime.datetime.now(),
                                                    "%Y-%m-%d %H:%M:%S") + " - " + errSrc + " - " + errType + " : " + errMsg + '\n')
        v_err_file.flush()
        v_err_file.close()
    finally:
        v_file.close()


class RH_Consumer:
    # 读取配置文件的配置信息，并初始化一些类需要的变量
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read('config.ini')
        self.host = self.config.get('Global', 'host')
        self.user = self.config.get('Global', 'user')
        self.passwd = self.config.get('Global', 'passwd')
        self.schema = self.config.get('Global', 'schema')
        self.port = int(self.config.get('Global', 'port'))
        self.kafka_server = self.config.get('Global', 'kafka_server')
        self.kafka_topic = self.config.get('Global', 'kafka_topic')
        self.consumer_group = self.config.get('Global', 'consumer_group')
        self.dd_host = self.config.get('Global', 'dd_host')
        self.dd_user = self.config.get('Global', 'dd_user')
        self.dd_passwd = self.config.get('Global', 'dd_passwd')
        self.dd_port = int(self.config.get('Global', 'dd_port'))
        self.dd_socket = self.config.get('Global', 'dd_socket')
        self.operation_time = datetime.datetime.now()
        self.stop_flag = 0
        self.src_table_name = []
        self.__init_db()
        self.__init_mes_db()
        self._get_all_src_table()

    # 连接写入目标数据库
    def __init_db(self):
        try:
            self.conn_info = {'host': self.host, 'port': self.port, 'user': self.user, 'password': self.passwd,
                              'db': 'tracking'}
            self.mysql_db = pymysql.connect(**self.conn_info, charset="utf8")
            self.mysql_cur = self.mysql_db.cursor()
        except Exception as data:
            writeErrorLog('__init_db', 'Error', str(data))

    # 连接生产数据库，用于获取相关维度信息
    def __init_mes_db(self):
        try:
            self.mes_mysql_db = pymysql.connect(host=self.dd_host, user=self.dd_user, passwd=self.dd_passwd,
                                                port=self.dd_port, unix_socket=self.dd_socket, charset="utf8")
            self.mes_mysql_cur = self.mes_mysql_db.cursor()
        except Exception as data:
            writeErrorLog('__init_db', 'Error', str(data))

    # 关闭数据库
    def _release_db(self):
        self.mysql_cur.close()
        self.mysql_db.close()
        self.mes_mysql_cur.close()
        self.mes_mysql_db.close()

    # 获取所有的配置表信息（需要获取的表）
    def _get_all_src_table(self):
        try:
            # 获取table的信息
            select_src_table_names = "select distinct src_table_name from tracking.tracking_table_mapping_rule"
            self.mysql_cur.execute(select_src_table_names)
            rows = self.mysql_cur.fetchall()
            for item in rows:
                self.src_table_name.append(item[0])
            return self.src_table_name
        except Exception as data:
            writeErrorLog('_get_all_src_table', 'Error', str(data))
            logging.error('_get_all_src_table: ' + str(data))

    # 获取src表的目标表信息
    def _get_tgt_table_name(self, table_name, table_schema):
        try:
            # 获取table的信息（table_name是schema|tablename）
            select_tgt_table_names = "select distinct tgt_table_name from tracking.tracking_table_mapping_rule where src_table_name = '%s' and src_table_schema = '%s'" % (
            table_name, table_schema)
            self.mysql_cur.execute(select_tgt_table_names)
            rows = self.mysql_cur.fetchall()
            tgt_table_names = []
            for item in rows:
                tgt_table_names.append(item[0])
            return tgt_table_names
        except Exception as data:
            writeErrorLog('_get_tgt_table_name', 'Error', str(data))
            logging.error('_get_tgt_table_name: ' + str(data))

    # 根据获取到输入的table_name,读取表的配置信息 会以json格式返回获取到的数据
    def _get_config(self, table_name, tgt_table_name, table_schema):
        try:
            # 获取table的信息（table_name是schema|tablename）
            select_table_config = "select coalesce( src_system, '' ) as src_system,coalesce ( src_table_schema, '' ) as src_table_schema,coalesce ( src_table_name, '' ) as src_table_name,coalesce ( tgt_operation, '{}' ) as tgt_operation,active_flag,coalesce ( tgt_system, '' ) as tgt_system,coalesce ( tgt_table_schema, '' ) as tgt_table_schema,coalesce ( tgt_table_name, '' ) as tgt_table_name from tracking.tracking_table_mapping_rule where src_table_name = '%s' and tgt_table_name='%s' and src_table_schema = '%s' " % (
            table_name, tgt_table_name, table_schema)
            self.mysql_cur.execute(select_table_config)
            rows = self.mysql_cur.fetchall()
            for item in rows:
                self.src_system = item[0]
                self.src_table_schema = item[1]
                self.src_table_name = item[2]
                self.tgt_operation = item[3]
                self.active_flag = item[4]
                self.tgt_system = item[5]
                self.tgt_table_schema = item[6]
                self.tgt_table_name = item[7]
            # 解析出self.tgt_operation 中以后所需要的数据
            self.tgt_operation = eval(self.tgt_operation)
            result_data = {'src_system': self.src_system,
                           'src_table_schema': self.src_table_schema,
                           'src_table_name': self.src_table_name,
                           'tgt_operation': self.tgt_operation,
                           'active_flag': self.active_flag,
                           'tgt_system': self.tgt_system,
                           'tgt_table_schema': self.tgt_table_schema,
                           'tgt_table_name': self.tgt_table_name,
                           # 解析出来的self.tgt_operation里的信息
                           'source_primary_key': self.tgt_operation['source_primary_key'],
                           'source_all_column': self.tgt_operation['source_all_column'],
                           'target_primary_key': self.tgt_operation['target_primary_key'],
                           'target_column': self.tgt_operation['target_column'],
                           'source_level': self.tgt_operation['source_level']}
            return result_data
        except Exception as data:
            writeErrorLog('_get_config', 'Error', str(data) + ':table is not available')
            logging.error('_get_config: ' + str(data))

    # 主方法的入口
    def _do(self):
        try:
            # 配置consumer的信息，可以配置很多其他信息
            c = Consumer({
                'bootstrap.servers': self.kafka_server,
                'group.id': self.consumer_group,
                'default.topic.config': {
                    'auto.offset.reset': 'smallest',
                    'enable.auto.commit': False}
            })
            # 定义消费kafka中的主题
            c.subscribe([self.kafka_topic])
            while True:
                msg = c.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(msg.error())
                        break
                text = msg.value().decode(encoding="utf-8")

                # kfk_text = eval(text)
                kfk_text = json.loads(text)
                # 此处判断kfk数据是否在配置表中，如果在则进行下一步，如果不在则忽略
                # 添加异常处理目的是为了如果这条数据写入有问题，就不commit，方便下次处理还可以继续消费
                try:
                    kfk_table = kfk_text['table']
                    if kfk_table in ['order_mails']:
                        print(type(text), text)
                        logging.warning('-------------- start exec table time : ' + str(
                            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())) + '---------------------')
                        kfk_text = str(kfk_text)
                        kfk_text = kfk_text.replace(": None", ": ''")
                        kfk_text = eval(kfk_text)
                        kfk_datas = kfk_text['data']
                        kfk_type = kfk_text['type']
                        kfk_old = kfk_text['old']
                        logging.warning(' table_name: ' + str(kfk_table) + ' table_type : ' + kfk_type)
                        if kfk_type == 'UPDATE':
                            continue
                            print('update')
                            for i, data in enumerate(kfk_datas):
                                kfk_text['data'] = eval("[" + str(data) + "]")
                                kfk_text['old'] = eval("[" + str(kfk_old[i]) + "]")
                                self._get_rh_from_kafka(kfk_text)
                        else:
                            print('insert')
                            for data in kfk_datas:
                                kfk_text['data'] = eval("[" + str(data) + "]")
                                print(type(kfk_text), kfk_text)
                                self._get_rh_from_kafka(kfk_text)
                        logging.warning('----------------end exec table time : ' + str(
                            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())) + '---------------')
                    c.commit()
                except Exception as data:
                    writeErrorLog('_do', 'exce data Error', str(data))
                    logging.error('_do: ' + str(data))
                # 如果停止程序
                if self.stop_flag == 1:
                    self._exit_consumer()
            c.close()
        except Exception as data:
            print(data)
            writeErrorLog('_do', 'Error', str(data))
            logging.error('_do: ' + str(data))

    def _trans_path(self, tgt_path):
        new_tgt_path = tgt_path.replace('.', '\".\"').replace('$\".', '$.') + '\"'
        return new_tgt_path

    # 此方法用来获取kafka中的数据，
    def _get_rh_from_kafka(self, kfk_text):
        try:
            # 解析获取到的kfk中的数据流
            self.kfk_tb_schema = kfk_text["database"]  # schema
            self.kfk_tb_name = kfk_text["table"]  # table_name
            self.kfk_data = kfk_text['data'][0]  # data
            self.kfk_type = kfk_text['type']  # 数据类型type
            self.kfk_es = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(kfk_text['es'] / 1000)))  # 数据表更的时间

            # 获取kfk传递过来src表的配置信息，读取配置表信息-----可能为空 需要添加判断
            tgt_table_names = self._get_tgt_table_name(self.kfk_tb_name, self.kfk_tb_schema)
            if len(tgt_table_names) != 0:
                for tgt_table_name_for_config in tgt_table_names:
                    tb_config = self._get_config(self.kfk_tb_name, tgt_table_name_for_config, self.kfk_tb_schema)
                    tgt_pk_key = tb_config['target_primary_key']  # 目标表的主键（order_no/order_item_id）
                    tgt_schema = tb_config['tgt_table_schema']  # 目标表的schema
                    tgt_table_name = tb_config['tgt_table_name']  # 目标表的名称（目前只有两个目标表tracking_order，tracking_order_item）
                    src_table_name = tb_config['src_table_name']  # 源表的名称（schema|table_name）
                    src_table_schema = tb_config['src_table_schema']
                    tgt_columns = tb_config['target_column']  # 获取插入到目标表中字段的配置信息（例如该表在order_info的插入路径等配置信息）
                    src_level = tb_config['source_level']  # 源表的level，目前有三种root，leaf，father
                    src_pk_key = tb_config['source_primary_key']  # 源表的主键
                    src_pk_value = self.kfk_data[src_pk_key]  # 源表的主键值（从kfk中获取到）
                    tgt_operation = tb_config['tgt_operation']  # 源表的其他配置，在下面处理时候再进行解析

                    # 处理的逻辑是，将表类型分为三类，root，leaf，father分开处理，分别处理其insert，update和delete的操作
                    if self.kfk_type == 'INSERT':  # 判断kfk的操作类型是INSERT，UPDATE,DELETE
                        if src_level == 'root':  # 判断该数据是否是root表
                            tgt_pk_value = self.kfk_data[tgt_pk_key]  # 如果是root表，则获取目标表表的主键的值（和src_pk_value的值相同）
                            for item in tgt_columns:  # item取值范围：order_info、order_progress等循环插入列，按照配置分别写入，因为可能一张表在不同列中插入位置不同
                                tgt_column = "{'" + item + "'" + ":" + str(tgt_columns[item][
                                                                               'source_column']) + "}"  # 拼成如下形式，目的为了_get_data_from_kfk传入参数，例如{"order_info": ["order_no", "cust_no"]}
                                tgt_column = eval(tgt_column)  # 将字符串转换成dict类型
                                if str(tgt_columns[item]['target_path']) == '{}':
                                    logging.warning(str(item) + " is null,please check")
                                else:
                                    tgt_path = list(tgt_columns[item]['target_path'].values())[0]  # 表在配置中，写入目标表的路径
                                    (table_insert_data, table_insert_data_for_leaf, insert_data,
                                     catalog_type) = self._get_data_from_kfk(kfk_text, tgt_column, src_table_name,
                                                                             tgt_pk_value)  # 调用方法，返回三种格式的json，为了不同的写入方式传参
                                    # 调用将kfk中数据入库的方法
                                    self._insert_data_from_kfk_for_root(tgt_schema, tgt_table_name, tgt_pk_key,
                                                                        tgt_pk_value, item, table_insert_data,
                                                                        tgt_path)  # 将kfk中主数据写入数据库
                                    self._insert_father_data(src_table_schema, src_table_name, insert_data, tgt_path,
                                                             tgt_pk_value, item, catalog_type,
                                                             tgt_table_name_for_config)  # 将主数据涉及到父表写入
                        # 子表insert思路：通过配置表找到上层关联表的键值，通过键值到数据库中查找到子表属于的记录（order_no/order_item_id）的值，从而可以确认子表的写入的绝对路径（拼上表名称或者是拼上键对应值），然后按照路径写入，补全父表
                        elif src_level == 'leaf':  # 判断kfk的操作类型是INSERT，UPDATE,DELETE
                            parent_pk_info = tgt_operation['parent_pk_key']
                            for item in tgt_columns:  # item取值范围：order_info、order_progress、order_operation、order_adjudgement
                                tgt_column = "{'" + item + "'" + ":" + str(tgt_columns[item][
                                                                               'source_column']) + "}"  # #拼成如下形式，目的为了_get_data_from_kfk传入参数，例如{"order_info": ["order_no", "cust_no"]}
                                tgt_column = eval(tgt_column)  # 将字符串转换成dict类型
                                if str(tgt_columns[item][
                                           'target_path']) == '{}':  # 因为子节点可能不会每一列都会配置写入信息（这个是不是不判断也可以，只要不配置即可，如果判断，root中也需要判断吗？）
                                    logging.warning(str(item) + " is null,please check")
                                else:
                                    tgt_path = list(tgt_columns[item]['target_path'].keys())[0]  # 获取写入的路径
                                    (table_insert_data, table_insert_data_for_leaf, insert_data,
                                     catalog_type) = self._get_data_from_kfk(kfk_text, tgt_column, src_table_name,
                                                                             src_pk_value)  # #调用方法，返回三种格式的json，为了不同的写入方式传参
                                    (parent_tgt_path, tgt_pk_value_new) = self._get_tgt_info_for_leaf(item, tgt_path,
                                                                                                      tgt_schema,
                                                                                                      tgt_table_name,
                                                                                                      tgt_pk_key,
                                                                                                      parent_pk_info,
                                                                                                      self.kfk_data)  # 获取子节点表的需要写入的目标表的主键的值和上一层的写入真实绝对路径
                                    tgt_path_true = parent_tgt_path + "." + src_table_name  # 获取子表写入的绝对路径（一直到子表的表名的路径）
                                    self._insert_data_from_kfk_for_leaf(tgt_schema, tgt_table_name, tgt_pk_key,
                                                                        tgt_pk_value_new, item,
                                                                        table_insert_data_for_leaf, tgt_path_true,
                                                                        src_pk_value, insert_data)  # 将从kafka获取的数据入库
                                    tgt_path_new = tgt_path_true + r'.\"' + src_pk_value + r'\"'  # 获取子表写入的绝对路径（一直到子表的主键值的路径）
                                    self._insert_father_data(src_table_name, insert_data, tgt_path_new,
                                                             tgt_pk_value_new, item, catalog_type,
                                                             tgt_table_name_for_config)  # 递归，写入子表的父表信息
                        elif src_level == 'father':  # 针对父表数据在主表和子表数据之后产生的情况
                            for item in tgt_columns:  # item取值范围：order_info、order_progress、order_operation、order_adjudgement
                                tgt_column = "{'" + item + "'" + ":" + str(tgt_columns[item][
                                                                               'source_column']) + "}"  # tgt_column例如{"order_info": ["order_no", "cust_no"]}
                                tgt_column = eval(tgt_column)  # 拼接目标列和目标列的值的信息
                                if str(tgt_columns[item]['target_path']) == '{}':
                                    logging.warning(str(item) + " is null,please check")
                                else:
                                    tgt_paths = list(tgt_columns[item]['target_path'].values())
                                    (table_insert_data, table_insert_data_for_leaf, insert_data,
                                     catalog_type) = self._get_data_from_kfk(kfk_text, tgt_column, src_table_name,
                                                                             src_pk_value)  # 从kafka获取的需要插入的json串
                                    if 'product' in src_table_name.lower():
                                        catalog_type = 'PRODUCT'
                                    elif 'service' in src_table_name.lower():
                                        catalog_type = 'SERVICE'
                                    else:
                                        catalog_type = '0'
                                    for tgt_path in tgt_paths:
                                        tgt_info_for_father = self._get_tgt_info_for_father(tgt_path, src_pk_key,
                                                                                            src_pk_value, tgt_pk_key,
                                                                                            tgt_schema, tgt_table_name,
                                                                                            item, catalog_type)
                                        if len(tgt_info_for_father) == 0:
                                            logging.warning('can not available the data of the root and leaf table ')
                                        else:
                                            for i in range(len(tgt_info_for_father)):
                                                tgt_pk_value_new = tgt_info_for_father[i][0]
                                                tgt_path_new = ('.'.join(tgt_info_for_father[i][1].split('.')[:-1]))[1:]
                                                self._insert_data_from_db(tgt_schema, tgt_table_name, tgt_pk_key,
                                                                          tgt_pk_value_new, item, tgt_path_new,
                                                                          insert_data)
                                                self._insert_father_data(src_table_name, insert_data, tgt_path_new,
                                                                         tgt_pk_value_new, item, catalog_type,
                                                                         tgt_table_name_for_config)

                    elif self.kfk_type == 'UPDATE':  # update处理方式
                        # 主表update思路 ：找到更新的记录，将需要更新的字段按照配置的路径更新（主表的路径不存在多层），再补全父表，写入历史纪录
                        if src_level == 'root':  # 判断是否是root表
                            tgt_pk_value = self.kfk_data[tgt_pk_key]  ##如果是root表，则获取目标表表的主键的值（和src_pk_value的值相同）
                            for item in tgt_columns:  # item取值范围：order_info、order_progress、order_operation、order_adjudgement
                                tgt_column = "{'" + item + "'" + ":" + str(tgt_columns[item][
                                                                               'source_column']) + "}"  # tgt_column例如{"order_info": ["order_no", "cust_no"]}
                                tgt_column = eval(tgt_column)  # 拼接目标列和目标列的值的信息
                                if str(tgt_columns[item]['target_path']) == '{}':
                                    logging.warning(str(item) + " is null,please check")
                                else:
                                    update_columns = kfk_text['old'][0]  # 获取kfk中变更信息
                                    tgt_path = list(tgt_columns[item]['target_path'].values())[0]
                                    (table_insert_data, table_insert_data_for_leaf, insert_data,
                                     catalog_type) = self._get_data_from_kfk(kfk_text, tgt_column, src_table_name,
                                                                             tgt_pk_value)
                                    self._update_data(tgt_schema, tgt_table_name, tgt_pk_key, src_table_name,
                                                      update_columns, insert_data, tgt_path, tgt_pk_value, item,
                                                      catalog_type, tgt_table_name_for_config, src_table_schema)  # 更新数据
                                    # 将变更历史写入
                                    if 'alter_column' in list(tgt_columns[item].keys()):
                                        record_history_column = tgt_columns[item]['alter_column']
                                        self._insert_history_data(update_columns, insert_data, tgt_path,
                                                                  record_history_column, self.kfk_es, item, tgt_schema,
                                                                  tgt_table_name, tgt_pk_key, tgt_pk_value)
                                    else:
                                        logging.warning(str(item) + " alter_column is not available")
                        # 子表update思路：通过配置表找到上层关联表的键值，通过键值到数据库中查找到子表属于的记录（order_no/order_item_id）的值，从而可以确认子表的写入的绝对路径（拼上表名称或者是拼上键对应值），然后按照路径更新对饮的字段，补全父表
                        elif src_level == 'leaf':  ## 判断是否是root表
                            parent_pk_info = tgt_operation['parent_pk_key']
                            for item in tgt_columns:  # item取值范围：order_info、order_progress、order_operation、order_adjudgement
                                tgt_column = "{'" + item + "'" + ":" + str(tgt_columns[item][
                                                                               'source_column']) + "}"  # tgt_column例如{"order_info": ["order_no", "cust_no"]}
                                tgt_column = eval(tgt_column)  # 拼接目标列和目标列的值的信息
                                if str(tgt_columns[item]['target_path']) == '{}':
                                    logging.warning(str(item) + " is null,please check")
                                else:
                                    update_columns = kfk_text['old'][0]  # 获取到变更信息
                                    tgt_path = list(tgt_columns[item]['target_path'].keys())[0]
                                    (table_insert_data, table_insert_data_for_leaf, insert_data,
                                     catalog_type) = self._get_data_from_kfk(kfk_text, tgt_column, src_table_name,
                                                                             src_pk_value)  # 从kafka获取的需要插入的json串
                                    (parent_tgt_path, tgt_pk_value_new) = self._get_tgt_info_for_leaf(item, tgt_path,
                                                                                                      tgt_schema,
                                                                                                      tgt_table_name,
                                                                                                      tgt_pk_key,
                                                                                                      parent_pk_info,
                                                                                                      self.kfk_data)  # 获取子表上一层主键路径
                                    tgt_path_true = parent_tgt_path + "." + src_table_name  ##获取子表写入的绝对路径（一直到子表的表名的路径）
                                    tgt_path_new = tgt_path_true + r'.\"' + src_pk_value + r'\"'  # 获取子表写入的绝对路径（一直到子表的主键值）
                                    self._update_data(tgt_schema, tgt_table_name, tgt_pk_key, src_table_name,
                                                      update_columns, insert_data, tgt_path_new, tgt_pk_value_new, item,
                                                      catalog_type, tgt_table_name_for_config, src_table_schema)
                                    if 'alter_column' in list(tgt_columns[item].keys()):
                                        record_history_column = tgt_columns[item]['alter_column']
                                        self._insert_history_data(update_columns, insert_data, tgt_path_new,
                                                                  record_history_column, self.kfk_es, item, tgt_schema,
                                                                  tgt_table_name, tgt_pk_key, tgt_pk_value_new)
                                    else:
                                        logging.warning(str(item) + " alter_column is not available")
                        # 父表更新的思路：从配置表获取所有目标路径，循环每一个路径，通过模糊匹配找到所有的目标主键值及准确路径，然后一条条更新，并将涉及的下一级信息补全
                        elif src_level == 'father':  # 判断该数据是否是kfk入库信息如果不是就pass
                            for item in tgt_columns:  # item取值范围：order_info、order_progress、order_operation、order_adjudgement
                                tgt_column = "{'" + item + "'" + ":" + str(tgt_columns[item][
                                                                               'source_column']) + "}"  # tgt_column例如{"order_info": ["order_no", "cust_no"]}
                                tgt_column = eval(tgt_column)  # 拼接目标列和目标列的值的信息
                                if str(tgt_columns[item]['target_path']) == '{}':
                                    logging.warning(str(item) + " is null,please check")
                                else:
                                    update_columns = kfk_text['old'][0]  # 获取到变更信息
                                    tgt_paths = list(tgt_columns[item]['target_path'].values())
                                    (table_insert_data, table_insert_data_for_leaf, insert_data,
                                     catalog_type) = self._get_data_from_kfk(kfk_text, tgt_column, src_table_name,
                                                                             src_pk_value)  # 从kafka获取的需要插入的json串
                                    if 'product' in src_table_name.lower():
                                        catalog_type = 'PRODUCT'
                                    elif 'service' in src_table_name.lower():
                                        catalog_type = 'SERVICE'
                                    else:
                                        catalog_type = '0'
                                    for tgt_path in tgt_paths:
                                        tgt_info_for_father = self._get_tgt_info_for_father(tgt_path, src_pk_key,
                                                                                            src_pk_value, tgt_pk_key,
                                                                                            tgt_schema, tgt_table_name,
                                                                                            item, catalog_type)
                                        for i in range(len(tgt_info_for_father)):
                                            tgt_pk_value_new = tgt_info_for_father[i][0]
                                            tgt_path_new = ('.'.join(tgt_info_for_father[i][1].split('.')[:-1]))[1:]
                                            self._update_data(tgt_schema, tgt_table_name, tgt_pk_key, src_table_name,
                                                              update_columns, insert_data, tgt_path_new,
                                                              tgt_pk_value_new, item, catalog_type,
                                                              tgt_table_name_for_config, src_table_schema)
                    # 删除操作思路：root表直接删除所有的记录，leaf删除按照路径删除目标，再加上判断如果子节点中没有数据，将对应的表名的字段删除
                    elif self.kfk_type == 'DELETE':
                        if src_level == 'root':
                            tgt_pk_value = self.kfk_data[tgt_pk_key]
                            self._delete_data_for_root(tgt_pk_key, tgt_pk_value, tgt_schema, tgt_table_name)
                        elif src_level == 'leaf':  #
                            parent_pk_info = tgt_operation['parent_pk_key']
                            for item in tgt_columns:  # item取值范围：order_info、order_progress、order_operation、order_adjudgement
                                tgt_column = "{'" + item + "'" + ":" + str(tgt_columns[item][
                                                                               'source_column']) + "}"  # tgt_column例如{"order_info": ["order_no", "cust_no"]}
                                tgt_column = eval(tgt_column)  # 拼接目标列和目标列的值的信息
                                if str(tgt_columns[item]['target_path']) == '{}':
                                    logging.warning(str(item) + " is null,please check")
                                else:
                                    tgt_path = list(tgt_columns[item]['target_path'].keys())[0]
                                    (parent_tgt_path, tgt_pk_value_new) = self._get_tgt_info_for_leaf(item, tgt_path,
                                                                                                      tgt_schema,
                                                                                                      tgt_table_name,
                                                                                                      tgt_pk_key,
                                                                                                      parent_pk_info,
                                                                                                      self.kfk_data)  # 获取子表上一层主键路径
                                    tgt_path_true = parent_tgt_path + "." + src_table_name  # 获取子表上一层表的路径
                                    tgt_path_new = tgt_path_true + r'.\"' + src_pk_value + r'\"'
                                    self._delete_data_for_leaf(tgt_schema, tgt_table_name, item, tgt_path_new,
                                                               tgt_pk_key, tgt_pk_value_new, tgt_path_true)
        except Exception as data:
            writeErrorLog('_get_rh_from_kafka', 'Error', str(data))
            logging.error('_get_rh_from_kafka: ' + str(data))

    def _get_tgt_info_for_father(self, tgt_path, src_pk_key, src_pk_value, tgt_pk_key, tgt_schema, tgt_table_name,
                                 tgt_column, catalog_type):
        try:
            tgt_path_true = tgt_path + "." + src_pk_key
            if catalog_type == '0':
                select_sql_for_father = "select " + tgt_pk_key + ",json_search(" + tgt_column + ",\'all\',\'" + src_pk_value + "\',null,\'" + tgt_path_true + "\') from " + tgt_schema + "." + tgt_table_name + " where json_extract(json_extract(" + tgt_column + ",\'" + tgt_path_true + "\'),\'$[0]\')=\'" + src_pk_value + "\';"
            else:
                select_sql_for_father = "select " + tgt_pk_key + ",json_search(" + tgt_column + ",\'all\',\'" + src_pk_value + "\',null,\'" + tgt_path_true + "\') from " + tgt_schema + "." + tgt_table_name + " where json_extract(json_extract(" + tgt_column + ",\'" + tgt_path_true + "\'),\'$[0]\')=\'" + src_pk_value + "\' and json_extract(" + tgt_column + ",\'$." + tgt_table_name + ".type=\'" + catalog_type + "\';"
            self.mysql_cur.execute(select_sql_for_father)
            tgt_info_for_father = self.mysql_cur.fetchall()
            return tgt_info_for_father
        except Exception as data:
            writeErrorLog('_get_tgt_info_for_father', 'Error', str(data))
            logging.error('_get_tgt_info_for_father: ' + str(data))

    def _delete_data_for_root(self, tgt_pk_key, tgt_pk_value, tgt_schema, tgt_table_name):
        try:
            delete_sql = "delete from " + tgt_schema + "." + tgt_table_name + " where " + tgt_pk_key + "=\'" + str(
                tgt_pk_value) + "\';"
            self.mysql_cur.execute(delete_sql)
            self.mysql_db.commit()
        except Exception as data:
            writeErrorLog('_delete_data_for_root', 'Error', str(data))
            logging.error('_delete_data_for_root: ' + str(data))

    def _delete_data_for_leaf(self, tgt_schema, tgt_table_name, tgt_column, tgt_path, tgt_pk_key, tgt_pk_value,
                              tgt_path_true):
        try:
            delete_sql = "update " + tgt_schema + "." + tgt_table_name + " set " + tgt_column + "=json_remove(" + tgt_column + ",\'" + tgt_path + "\') where " + tgt_pk_key + "=\'" + str(
                tgt_pk_value) + "\';"
            self.mysql_cur.execute(delete_sql)
            self.mysql_db.commit()
            select_sql = "select json_extract(" + tgt_column + ",\'" + tgt_path_true + "\') from " + tgt_schema + "." + tgt_table_name + " where " + tgt_pk_key + "=\'" + str(
                tgt_pk_value) + "\';"
            self.mysql_cur.execute(select_sql)
            tgt_column_value = self.mysql_cur.fetchall()[0][0]
            if tgt_column_value == r'{}':
                table_delete_sql = "update " + tgt_schema + "." + tgt_table_name + " set " + tgt_column + "=json_remove(" + tgt_column + ",\'" + tgt_path_true + "\') where " + tgt_pk_key + "=\'" + str(
                    tgt_pk_value) + "\';"
                self.mysql_cur.execute(table_delete_sql)
                self.mysql_db.commit()
        except Exception as data:
            writeErrorLog('_delete_data_for_leaf', 'Error', str(data))
            logging.error('_delete_data_for_leaf: ' + str(data))

    def _insert_history_data(self, update_columns, insert_data, tgt_path, record_history_column, data_time, tgt_column,
                             tgt_schema, tgt_table_name, tgt_pk_key, tgt_pk_value):
        try:
            update_columns_key = list(update_columns.keys())
            for item in record_history_column:
                if item in update_columns_key:
                    tgt_path_for_column = tgt_path + '.alter_data.' + item
                    tgt_path_for_alter = tgt_path + '.alter_data'
                    select_sql_for_alter_column_path = 'select json_extract(' + tgt_column + ',\'' + tgt_path_for_column + '\')' + ' from ' + tgt_schema + '.' + tgt_table_name + ' where ' + tgt_pk_key + '=\'' + str(
                        tgt_pk_value) + '\';'
                    select_sql_for_alter_path = 'select json_extract(' + tgt_column + ',\'' + tgt_path_for_alter + '\')' + ' from ' + tgt_schema + '.' + tgt_table_name + ' where ' + tgt_pk_key + '=\'' + str(
                        tgt_pk_value) + '\';'
                    self.mysql_cur.execute(select_sql_for_alter_column_path)
                    tgt_path_vlaue_for_column = self.mysql_cur.fetchall()
                    self.mysql_cur.execute(select_sql_for_alter_path)
                    tgt_path_vlaue_for_alter = self.mysql_cur.fetchall()
                    old_data = update_columns[item]
                    new_data = eval(insert_data)[item]
                    if tgt_path_vlaue_for_alter[0][0] == None:
                        history_data = '{\"' + item + '\":[{\"old_data\":\"' + str(
                            old_data) + '\",\"new_data\":\"' + str(new_data) + '\",\"time\":\"' + data_time + '\"}]}'
                        insert_sql = "update " + tgt_schema + "." + tgt_table_name + " set " + tgt_column + "=json_insert(" + tgt_column + ",\'" + tgt_path_for_alter + "\',cast(\'" + history_data + "\' as json)) where " + tgt_pk_key + "= '" + str(
                            tgt_pk_value) + "';"
                    else:
                        if tgt_path_vlaue_for_column[0][0] == None:
                            history_data = '[{\"old_data\":\"' + str(old_data) + '\",\"new_data\":\"' + str(
                                new_data) + '\",\"time\":\"' + data_time + '\"}]'
                            insert_sql = "update " + tgt_schema + "." + tgt_table_name + " set " + tgt_column + "=json_insert(" + tgt_column + ",\'" + tgt_path_for_column + "\',cast(\'" + history_data + "\' as json)) where " + tgt_pk_key + "= '" + str(
                                tgt_pk_value) + "';"
                        else:
                            history_data = '{\"old_data\":\"' + str(old_data) + '\",\"new_data\":\"' + str(
                                new_data) + '\",\"time\":\"' + data_time + '\"}'
                            insert_sql = "update " + tgt_schema + "." + tgt_table_name + " set " + tgt_column + "=json_array_append(" + tgt_column + ",\'" + tgt_path_for_column + "\',cast(\'" + history_data + "\' as json)) where " + tgt_pk_key + "= '" + str(
                                tgt_pk_value) + "';"
                self.mysql_cur.execute(insert_sql)
                self.mysql_db.commit()
        except Exception as data:
            writeErrorLog('_insert_history_data', 'Error', str(data))
            logging.error('_insert_history_data: ' + str(data))

    # 将kfk中的数据，进行转换，转换成不同的写入方式需要的json格式
    def _get_data_from_kfk(self, text, tgt_column, src_table_name, src_table_pk_value):
        try:
            tgt_column_json = tgt_column  # 传入的目标表的列名称
            tgt_column_key = ''
            for key in tgt_column_json:  # 循环tgt_column中的key值
                json_column_key = '{'
                for item in tgt_column_json[key]:
                    json_column_key += '"' + item + '":"' + text['data'][0][item].replace('"', r'\\"') + '",'
                    tgt_column_item = json_column_key[:-1]
                tgt_column_key += tgt_column_item + '},'
                if 'type' in text['data'][0]:
                    catalog_type = text['data'][0]['type']
                else:
                    catalog_type = '0'
            table_insert_data = '{\"' + src_table_name + '\":' + tgt_column_key[
                                                                 :-1] + '}'  # 拼接成如下带有表名和主键值格式{"order":{"order_no":"100"}}
            insert_data = tgt_column_key[:-1]  # 拼接成如下不带表名和不带主键值的格式{"order_no":"100"}
            table_insert_data_for_leaf = '{\"' + src_table_pk_value + '\":' + insert_data + '}'  # 拼接成如下带有主键值格式的{"100":{"order_no":"100"}}
            print(insert_data)
            return (table_insert_data, table_insert_data_for_leaf, insert_data, catalog_type)  # 返回数据
        except Exception as data:
            writeErrorLog('_get_data_from_kfk', 'Error', str(data))
            logging.error('_get_data_from_kfk: ' + str(data))

    def _insert_data_from_kfk_for_root(self, tgt_schema, tgt_table_name, tgt_table_pk, tgt_table_value, tgt_column,
                                       table_insert_data, tgt_path):
        try:
            # 先判断主键是否存在，如果存在则插入其他数据，如果不存在，则先插入主键信息
            select_tb_count = 'select count(*) from ' + tgt_schema + "." + tgt_table_name + ' where ' + tgt_table_pk + '=\'' + tgt_table_value + '\';'
            # 判断列中是否存在数据
            select_tb_column_count = 'select case when coalesce(' + tgt_column + ', \'\') = \'\' then 1 else 0 end from ' + tgt_schema + "." + tgt_table_name + ' where ' + tgt_table_pk + '=\'' + tgt_table_value + '\';'
            self.mysql_cur.execute(select_tb_count)
            tb_count = self.mysql_cur.fetchall()
            self.mysql_cur.execute(select_tb_column_count)
            tb_column_count = self.mysql_cur.fetchall()
            # 判断是否存在数据，如果不存在，则先插入主键（order_no/order_item_id）再将数据写入到列中
            if tb_count[0][0] == 0:
                insert_pk_sql = "insert into " + tgt_schema + "." + tgt_table_name + "(" + tgt_table_pk + ") values ('" + tgt_table_value + "')"
                self.mysql_cur.execute(insert_pk_sql)
                self.mysql_db.commit()
                update_sql = "update " + tgt_schema + "." + tgt_table_name + " set " + tgt_column + "= cast('" + table_insert_data + "' as json) where " + tgt_table_pk + "= '" + tgt_table_value + "';"
            else:
                # 如果主键存在，列为空，则需要 直接 写入带有table_name格式的json
                if tb_column_count[0][0] == 1:  # 当目标字段为空
                    update_sql = "update " + tgt_schema + "." + tgt_table_name + " set " + tgt_column + "= cast('" + table_insert_data + "' as json) where " + tgt_table_pk + "= '" + tgt_table_value + "';"
                else:
                    # 如果主键存在，列不为空，则需要使用json_insert方法写入带有table_name格式的json
                    update_sql = "update " + tgt_schema + "." + tgt_table_name + " set " + tgt_column + "=json_insert(" + tgt_column + ",\'" + tgt_path + "\',cast(\'" + table_insert_data + "\' as json)) where " + tgt_table_pk + "=\'" + tgt_table_value + "\';"
            self.mysql_cur.execute(update_sql)
            self.mysql_db.commit()
        except Exception as data:
            writeErrorLog('_insert_data_from_kfk_for_root', 'Error', str(data))
            logging.error('_insert_data_from_kfk_for_root: ' + str(data))

    def _get_tgt_pk_value_for_leaf(self, tgt_table_pk, tgt_schema, tgt_table_name, tgt_column, tgt_path,
                                   parent_pk_value):
        try:
            select_tgt_pk_sql = "select " + tgt_table_pk + " from " + tgt_schema + "." + tgt_table_name + " where json_extract(" + tgt_column + ",\'" + tgt_path + "\')=\'" + parent_pk_value + "\';"
            self.mysql_cur.execute(select_tgt_pk_sql)
            tgt_pk_value = self.mysql_cur.fetchall()[0][0]
            return tgt_pk_value
        except Exception as data:
            writeErrorLog('_get_tgt_pk_value_for_leaf', 'Error', str(data))
            logging.error('_get_tgt_pk_value_for_leaf: ' + str(data))

    # 获取子节点表的需要写入的目标表的主键的值和上一层的写入真实绝对路径
    def _get_tgt_info_for_leaf(self, tgt_column, tgt_path, tgt_schema, tgt_table_name, tgt_pk_key, parent_pk_info,
                               kafka_data):
        try:
            if_tgt_path = '.'.join(tgt_path.split('.')[:-1])
            i = 0
            json_search_sql = ''
            where_sql = ''
            if if_tgt_path == '$':
                for parent_pk_key in list(parent_pk_info.keys()):
                    parent_pk_value = kafka_data[parent_pk_info[parent_pk_key]]
                    json_search_sql += ",json_search(" + tgt_column + ", 'one','" + str(
                        parent_pk_value) + "', null, '" + tgt_path + "." + parent_pk_key + "') as tgt_path" + str(i)
                    where_sql += " tgt_path" + str(i) + " is not null and"
                    i = i + 1
            else:
                for parent_pk_key in list(parent_pk_info.keys()):
                    parent_pk_value = kafka_data[parent_pk_info[parent_pk_key]]
                    json_search_sql += ",json_search(" + tgt_column + ", 'one','" + str(
                        parent_pk_value) + "', null, '" + tgt_path + ".*." + parent_pk_key + "') as tgt_path" + str(i)
                    where_sql += " tgt_path" + str(i) + " is not null and"
                    i = i + 1
            select_sql = "select " + tgt_pk_key + ",tgt_path0 from (select " + tgt_pk_key + json_search_sql + " from " + tgt_schema + "." + tgt_table_name + ") t where " + where_sql[
                                                                                                                                                                            :-4] + ";"
            self.mysql_cur.execute(select_sql)
            rows = self.mysql_cur.fetchall()[0]
            tgt_path_new = ('.'.join(rows[1].split('.')[:-1]))[1:]
            tgt_pk_value_new = rows[0]
            return (tgt_path_new, tgt_pk_value_new)
        except Exception as data:
            writeErrorLog('_get_tgt_info_for_leaf', 'Error', str(data))
            logging.error('_get_tgt_info_for_leaf: ' + str(data))

    def _insert_data_from_kfk_for_leaf(self, tgt_schema, tgt_table_name, tgt_table_pk, tgt_table_value, tgt_column,
                                       table_insert_data_for_leaf, tgt_path, src_pk_value, insert_data):
        try:
            select_tb_column_key = 'select case when coalesce(json_extract(' + tgt_column + ',\'' + tgt_path + '\') , \'\') = \'\' then 1 else 0 end from ' + tgt_schema + "." + tgt_table_name + ' where ' + tgt_table_pk + '=\'' + str(
                tgt_table_value) + '\';'
            self.mysql_cur.execute(select_tb_column_key)
            column_key_data = self.mysql_cur.fetchall()
            if column_key_data[0][0] == 1:  # 当主键存在并且目标字段不为空路径不存在，
                tgt_path_new = tgt_path
                tgt_insert_data = table_insert_data_for_leaf
            else:
                tgt_path_new = tgt_path + r'.\"' + str(src_pk_value) + r'\"'
                tgt_insert_data = insert_data
            update_sql = "update " + tgt_schema + "." + tgt_table_name + " set " + tgt_column + "=json_insert(" + tgt_column + ",\'" + tgt_path_new + "\',cast(\'" + tgt_insert_data + "\' as json)) where " + tgt_table_pk + "=\'" + str(
                tgt_table_value) + "\';"
            self.mysql_cur.execute(update_sql)
            self.mysql_db.commit()
        except Exception as data:
            writeErrorLog('_insert_data_from_kfk_for_leaf', 'Error', str(data))
            logging.error('_insert_data_from_kfk_for_leaf: ' + str(data))

    # 将父表数据写入（父表数据从生产库中获取，按照对应的配置路径写入数据库中）
    def _insert_father_data(self, src_table_schema, scr_table_name, insert_data, src_path, root_pk_value, tgt_column,
                            catalog_type, tgt_table_name_for_config):
        try:
            src_config_data = self._get_config(scr_table_name, tgt_table_name_for_config,
                                               src_table_schema)  # 获取初始表的配置信息（此处获取是为了递归时候传入下一层的表名，获取对应的配置信息）
            src_foreign_info = src_config_data['target_column'][tgt_column][
                'source_foreign_info']  # 从数据库配置表中获取source_foreign_info的信息，也就是外键的信息，包括外键，外键的表，以及外键表中的主键名称
            if len(json.dumps(
                    src_foreign_info)) == 2:  # 当没有外键的时候，配置表只存在‘{}'长度为2，就不需要向下递归执行，对应的source_foreign_info=[]，长度为2
                logging.warning(scr_table_name + " :Recursive over")
            else:
                for src_pk_key in src_foreign_info:  # 获取当前表与下层父表的关联键（例如customer表的配置获取到org_id，"source_foreign_info": {"org_id": {"customer.organization": "org_id"}}）
                    foreign_table_name_tmp = list(src_foreign_info[src_pk_key].keys())[
                        0]  # 获取外键对应的表名foreign_table_name（organization），（每次传入的key对应一个外键表，只存在一个列，order_info，所以取第一个元素即可）
                    foreign_table_schema = foreign_table_name_tmp.split('.')[0]
                    foreign_table_name_tmp = foreign_table_name_tmp.split('.')[1]
                    if '#' in foreign_table_name_tmp:
                        foreign_table_name = foreign_table_name_tmp.replace('#', catalog_type).lower()
                    else:
                        foreign_table_name = foreign_table_name_tmp
                    foreign_table_pk_key = list(src_foreign_info[src_pk_key].values())[
                        0]  # 获取外键对应的表的关联键foreign_table_key，即org_id
                    foreign_datas = self._get_config(foreign_table_name, tgt_table_name_for_config,
                                                     foreign_table_schema)  # 获取外键表的配置信息，以便下面获取配置表的信息
                    foreign_column = foreign_datas['target_column'][
                        tgt_column]  # 获取要插入的目标表列是order_info/order_progress)（organization写入目标表的列的配置信息）
                    foreign_schema = foreign_datas['src_table_schema']  # 获取表的schema（organization的原始src schema）
                    foreign_table_pk_value = eval(str(insert_data))[
                        src_pk_key]  # 获取外键对应的value（即organization在kfk数据中对应的值）
                    # 获取外键对应表的配置信息(写入数据库需要用)
                    tgt_schema = foreign_datas['tgt_table_schema']
                    tgt_table_name = foreign_datas['tgt_table_name']
                    tgt_pk_key = foreign_datas['target_primary_key']
                    tgt_pk_value = root_pk_value  # 目标表主键的值
                    # 获取数据，并在其中获取后，写入数据库（此处部分参数是为了给insert服务）
                    for foreign_path in foreign_column['target_path']:
                        src_tgt_path = list(foreign_path.keys())[0]
                        foreign_tgt_path = list(foreign_path.values())[0]
                        if re.sub('.\"\S*?\"', r'*', src_path) == src_tgt_path and re.sub('.\"\S*?\"', r'*',
                                                                                          src_path) + '.' + src_pk_key == foreign_tgt_path:
                            next_src_path = src_path + '.' + src_pk_key
                            next_insert_data = self._get_data_from_db(foreign_column, foreign_table_name,
                                                                      foreign_schema, foreign_table_pk_key,
                                                                      foreign_table_pk_value, tgt_schema,
                                                                      tgt_table_name, tgt_pk_key, tgt_pk_value,
                                                                      src_path, tgt_column, next_src_path)
                            self._insert_father_data(foreign_table_schema, foreign_table_name, next_insert_data,
                                                     next_src_path, root_pk_value, tgt_column, catalog_type,
                                                     tgt_table_name_for_config)
                        else:
                            logging.warning(foreign_table_name + ' :have no next level')
        except Exception as data:
            writeErrorLog('_insert_father_data', 'Error', str(data))
            logging.error('_insert_father_data: ' + str(data))

    # 从数据库中获取数据，并将获取到的数据，直接插入数据库中，返回递归需要使用的数据
    def _get_data_from_db(self, src_tgt_column, src_table_name, src_table_schema, src_table_pk_key, src_table_pk_value,
                          tgt_schema, tgt_table_name, tgt_pk_key, tgt_pk_value, src_path, tgt_column, tgt_path):
        try:
            result_data = '{'
            src_column = src_tgt_column['source_column']  # 读取需要获取的字段
            if len(src_column) == 0:
                logging(str(src_column) + ' length equal 0 error ')
            else:
                for item in src_column:  # 拼接好sql语句，获取数据
                    select_sql1 = 'concat(\''
                    select_sql1 += u'"' + item + '":"\',coalesce(' + item + ',\'\'),\'",'
                    select_sql1 = select_sql1[:-1] + '\')'
                    select_sql = "select " + select_sql1 + " from " + src_table_schema + "." + src_table_name + " where " + src_table_pk_key + "=\'" + src_table_pk_value + "\';"
                    # 使用execute方法执行SQL语句
                    self.mes_mysql_cur.execute(select_sql)
                    # 使用 fetchone() 方法获取一条数据
                    data = self.mes_mysql_cur.fetchall()
                    if len(data) == 0:
                        result_data += ''
                    else:
                        result_data += data[0][0] + ','
                if result_data != '{':
                    tgt_value = result_data[:-1] + '}'
                else:
                    tgt_value = result_data + '\"' + src_table_pk_key + '\":\"' + src_table_pk_value + '\"}'
                self._insert_data_from_db(tgt_schema, tgt_table_name, tgt_pk_key, tgt_pk_value, tgt_column, tgt_path,
                                          tgt_value)  # 将获取到的父表数据写入数据库
            return tgt_value  # 返回写入的数据，和真是的写入路径（因为路径在配置表中层数多的是用*代替的，不是真正的绝对路径，这里返回的是绝对路径）
        except Exception as data:
            writeErrorLog('_get_data_from_db', 'Error', str(data))
            logging.error('_get_data_from_db: ' + str(data))

    # 将父表写入数据库
    def _insert_data_from_db(self, tgt_schema, tgt_table_name, tgt_pk_key, tgt_pk_value, tgt_column, tgt_path,
                             tgt_value):
        try:
            insert_sql = "update " + tgt_schema + "." + tgt_table_name + " set " + tgt_column + "=json_replace(" + tgt_column + ",\'" + tgt_path + "\',cast(\'" + tgt_value + "\' as json)) where " + tgt_pk_key + "=\'" + str(
                tgt_pk_value) + "\';"
            # self.mysql_cur.execute(insert_sql.encode("utf-8").decode("latin1"))
            self.mysql_cur.execute(insert_sql)
            self.mysql_db.commit()
        except Exception as data:
            writeErrorLog('_insert_data_from_db', 'Error', str(data))
            logging.error('_insert_data_from_db: ' + str(data))

    # 当变更数据为外键时，补全外键对应的信息
    def _update_data(self, tgt_schema, tgt_table_name, tgt_pk_key, src_table_name, update_columns, insert_data,
                     src_path, root_pk_value, tgt_column, catalog_type, tgt_table_name_for_config, src_table_schema):
        try:
            # 判断是否涉及外键信息，判断变更的字段是否在外键信息里，将在的组成新的外键json，在调用_get_data_from_db进行更新数据
            insert_data = json.loads(insert_data)
            for update_column in update_columns:  #
                if update_column in list(insert_data.keys()):
                    update_column_data = '\"' + insert_data[update_column] + '\"'
                    tgt_path = src_path + '.' + update_column
                    self._insert_data_from_db(tgt_schema, tgt_table_name, tgt_pk_key, root_pk_value, tgt_column,
                                              tgt_path, update_column_data)
            self._insert_father_data(src_table_schema, src_table_name, insert_data, src_path, root_pk_value, tgt_column,
                                     catalog_type, tgt_table_name_for_config)
        except Exception as data:
            writeErrorLog('_update_data', 'Error', str(data))
            logging.error('_update_data: ' + str(data))

    # 退出消费消息
    def _exit_consumer(self):
        self._release_db()
        sys.exit()


def exit_program(signum, frame):
    logging.info("Received Signal: %s at frame: %s" % (signum, frame))
    p.stop_flag = 1


def main():
    # 实例化对象
    p = RH_Consumer()
    signal.signal(signal.SIGTERM, exit_program)
    # while True:
    p._do()


main()
