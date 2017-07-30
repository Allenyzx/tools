#coding: UTF-8
#!/usr/bin/env python
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm.session import sessionmaker


"""
author: Allen Yang
describe: 为DATAFRAME数据结构制作的SQL工具 
version : 0.01
update_time: 2017-07-30
"""



class Sqlforpandas:

    def __init__(self,connection):
        'mysql+pymysql://root:chenluyao@localhost:3306/local'
        if type(connection) == dict:
            conn =connection
            connection = '{0}://{1}:{2}@{3}:{4}/{5}?charset=utf8'.format(
                conn['engine'],conn['user'],conn['password'],conn['host'],str(conn['port']),conn['database'])
            self.session = sessionmaker(bind=create_engine(connection))()
        elif type(connection) == str:
            self.session = sessionmaker(bind=create_engine(connection))()
        else:
            try:
                self.session = sessionmaker(bind=connection)()
            except:
                raise ValueError('CONNECTION type is wrong')

    def __update(self):
        self.session.flush()
        return self.session.commit()


    def __close(self):
        return self.session.close()


    def execute(self,context):
        return self.session.execute(context)

    # 添加限制条件
    def __filter(self,item,filter_columns):
         return  'WHERE {}'.format(' AND '.join(['`{0}` = {1}'.format(key,item[key]) for key in filter_columns]))


    # 转文本格式，如果int则无需加''，str需增加''
    def __turnIntStr(self,data,columns):
        if 'str' in set(columns.values()) and len(set(columns.values())) == 1:
            data = data.applymap(lambda x: "'{}'".format(x))

        elif 'int' in set(columns.values()) and len(set(columns.values())) == 1:
            data = data.applymap(lambda x: "{}".format(x))

        else:
            for key in columns.keys():
                if columns[key] == 'str':
                    data[key] = data[key].apply(lambda x: "'{}'".format(x))
                elif columns[key] == 'int':
                    data[key] = data[key].apply(lambda x: "{}".format(x))
                else:
                    raise ValueError('data type parm set error')
        return data

    # 查询是否有该表
    def show_table(self,table):
        if type(table) == str:
            context = "SHOW TABLES LIKE '{}' ".format(table)
            if  self.execute(context).fetchone() is None:
                return False
            else:
                return True
        else:
            raise ValueError('TABLE NAME type wrong')

    # 删除表
    def drop(self,table):
        if type(table) == str:
            if self.show_table(table) == True:
                context = "DROP TABLE {0}".format(table)
                self.execute(context)
                return self.__close()
            else:
                raise ValueError('DataBase has no TABLE NAME {0}'.format(table))
        else:
            raise ValueError('TABLE NAME type wrong')


    def insert(self,table,data,insert_columns):

        if type(table) == str:
            if type(data) == pd.DataFrame and data.empty is False:

                data =self.__turnIntStr(data,insert_columns).to_dict(orient='records')

                for item in data:
                    context = "INSERT INTO `{0}` ({1}) VALUES ({2}) ".format(table,",".join(insert_columns),",".join([item[key] for key in insert_columns ]))
                    self.execute(context=context)
                    self.__update()

                return self.__close()

            else:
                raise ValueError('By INSERT,data is empty or type wrong')
        else:
            raise ValueError('TABLE NAME type wrong')

    def update(self,table,data,insert_columns,filter_columns=False):

        if type(table) == str:
            if type(data) == pd.DataFrame and data.empty is False:
                data =self.__turnIntStr(data,insert_columns).to_dict(orient='records')
                for item in data:
                    context = "UPDATE `{0}` SET {1} ".format(table,' , '.join(['`{0}` = {1}'.format(key,item[key]) for key in insert_columns]))

                    if filter_columns is not False and type(filter_columns) == dict:
                        context += self.__filter(item, filter_columns)

                    self.execute(context=context)
                    self.__update()
                return self.__close()

            else:
                raise ValueError('By UPDATE,data is empty or type wrong')
        else:
            raise ValueError('TABLE NAME type wrong')



    def create(self,table,create_columns,replace=False):
        if type(table) == str:
            if type(create_columns) == dict and len(create_columns) > 0 :

                if replace == True and self.show_table(table) == True:
                    self.drop(table)

                context = 'CREATE TABLE {0} ({1})'.format(table,' , '.join(['`{0}` {1}'.format(key,create_columns[key]) for key in create_columns]))

                self.execute(context)

                return self.__close()

            else:
                raise ValueError('By CREATE,create_columns type or data wrong')
        else:
            raise ValueError('TABLE NAME type wrong')
