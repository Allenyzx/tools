# -*- coding:utf-8 -*-
import logging
import sys
import os
syspath = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) + '/Qctr_v1'
sys.path.insert(0, syspath)

__author__ = 'allen'


# _logging = __import__('logging')
_sys = __import__('sys')

# _logging.basicConfig(filemode='w')


# FORMATTER = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
FORMATTER = '%(asctime)s - %(name)s - %(message)s'


class __P__:
    class __OperateLog__:

        def __init__(self, OPERATE_NAME, OPERATE_LOG, SET_FILE):
            self.OPERATE_NAME = OPERATE_NAME
            self.OPERATE_LOG = OPERATE_LOG

            self.formatter = logging.Formatter(FORMATTER)

            self.printing = logging.StreamHandler()
            self.printing.setFormatter(self.formatter)

            self.logger = logging.getLogger(self.OPERATE_NAME)
            self.logger.addHandler(self.printing)

            self.logger.setLevel(logging.DEBUG)
            self.file = logging.FileHandler(self.OPERATE_LOG, mode='a', encoding='utf8')
            self.file.setFormatter(self.formatter)

            self.__printf_zero__()

        def __set_level__(self, level):
                if level == 'debug':
                    self.logger.setLevel(logging.DEBUG)
                elif level == 'info':
                    self.logger.setLevel(logging.INFO)
                elif level == 'warning':
                    self.logger.setLevel(logging.WARNING)
                elif level == 'error':
                    self.logger.setLevel(logging.ERROR)
                elif level == 'critical':
                    self.logger.setLevel(logging.CRITICAL)
                else:
                    print('set level write current words!')

        def __printf_debug__(self, *args):

            if args:

                self.logger.addHandler(self.file)

                self.logger.debug(*args)

            return True

        def __printf_info__(self, *args):

            if args:

                self.logger.addHandler(self.file)
                self.logger.info(*args)

            return True

        def __printf_warning__(self, *args):

            if args:

                self.logger.addHandler(self.file)
                self.logger.warning(*args)

            return True

        def __printf_error__(self, *args):

            if args:

                self.logger.addHandler(self.file)
                self.logger.error(*args)

            return True

        def __printf_critical__(self, *args):

            if args:

                self.logger.addHandler(self.file)
                self.logger.critical(*args)

            return True

        def __printf_zero__(self, *args):
            # pass
            self.file.close()
            if os.path.exists(self.OPERATE_LOG):
                if os.path.getsize(self.OPERATE_LOG) == 0:
                    os.remove(self.OPERATE_LOG)

            return True


class Items:
    def __init__(self, OPERATE_NAME, OPERATE_LOG, SET_FILE=1):
        self.OPERATE_LOG = OPERATE_LOG
        self.OL = __P__.__OperateLog__(OPERATE_NAME=OPERATE_NAME, OPERATE_LOG=OPERATE_LOG, SET_FILE=SET_FILE)

    def set_level(self, level):
        self.OL.__set_level__(level)

    def printf_debug(self, *args):
        # self.OL.__set_file__()
        self.OL.__printf_debug__(*args)

    def printf_info(self, *args):
        self.OL.__printf_info__(*args)

    def printf_warning(self, *args):
        self.OL.__printf_warning__(*args)

    def printf_error(self, *args):
        self.OL.__printf_error__(*args)

    def printf_critical(self, *args):
        self.OL.__printf_critical__(*args)


_sys.modules['Items'] = Items


file_path = os.path.dirname(__file__)
top_path = os.path.dirname(os.path.dirname(__file__))
file_path_log = os.path.join(os.path.dirname(os.path.dirname(__file__)), '_log')


def _tolog(operate_name,logfileName=''):
    f = Items(OPERATE_NAME=operate_name, OPERATE_LOG=file_path_log + '/' + logfileName)
    return f
