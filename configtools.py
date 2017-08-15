# coding:utf-8
import configparser



# 基础设置
class ConfigSet:

    def __init__(self,path):
        self.path = path
        self.cf = configparser.ConfigParser()
        self.cf.read(path)

    def _get_value(self,key,key_value,is_int=False):

        if is_int == True:
            context = self.cf.getint(str(key),key_value)
        else:
            context = self.cf.get(str(key),key_value)

        return context


    def _update(self,key,key_value,value):
        self.cf.set(str(key), key_value, value)
        return self.cf.write(open(self.path, 'w'))



# 在原来的基础上新增树结构索引
class ConfigTools(ConfigSet):

    def __init__(self,config_path,known_keys='',sep='_'):
        """
        :param config_path: ini文件默认路径 
        :param known_keys: 已知路径
        :param sep: 分隔符，默认'_'
        """
        ConfigSet.__init__(self,path=config_path)
        self.known_keys = known_keys
        self.sep = sep

    def new_str(self,key_list):
        return '{}'.format(self.sep).join(key_list)

    def get_value(self,key_list,key_value,is_int=False):
        if self.known_keys != '' and type(self.known_keys) is list:
            key_list = self.known_keys + key_list

        if type(key_list) is list:
            key = self.new_str(key_list)
        else:
            key = key_list

        return self._get_value(key,key_value,is_int=is_int)

    def update(self,key_list,key_value,value):
        if self.known_keys != '' and type(self.known_keys) is list:
            key_list = self.known_keys + key_list

        if type(key_list) is list:
            key = self.new_str(key_list)
        else:
            key = key_list

        return self._update(key,key_value,value)



if __name__ == '__main__':
    import os
    sys_path = os.path.dirname(os.path.dirname(__file__))
    print(sys_path)
    cf = ConfigTools(sys_path+'/config/test.ini')
    # a = cf.cf.options('nctr_borrow')
    b = cf.get_value('nctr_borrow','model')
    print(b)


