# coding:utf-8
import numpy as np
import pandas as pd
import os


"""
BASE思路：
:param:模型的parameter
:return:模型的评估值
基于param迭代参数，每一轮只轮询一个参数，在轮到最后参数后取最大值的return的参数，固定该参数，进行其他迭代，类推。

TPE思路：


"""




class Base:

    def __init__(self,parmRange,seed):
        # 需要的参数选择范围
        self.parmRange = parmRange
        # 种子范围
        self.seed = seed
        self.pdoutput = pd.DataFrame()



    def fit(self,model,assess='min',save=False,csv_path='',show=False):
        """
        :param model: 
        :param assess: max 取最大， min 取最小 
        :return: 
        """

        for key in self.parmRange:
            assessDic = {}
            last = self.parmRange[key][-1]

            for changed in self.parmRange[key]:
                self.seed[key] = changed
                modelAssess = model(self.seed)

                output = self.seed
                output['assess'] = modelAssess

                # 展示
                if show is True:
                    self.pdoutput = self._store(self.pdoutput,output)
                    print(self.pdoutput)

                assessDic[assess] = changed
                if changed == last:
                    if assess == 'min':
                        assessValue = assessDic[min(assessDic.keys())]
                    elif assess == 'max':
                        assessValue = assessDic[max(assessDic.keys())]
                    else:
                        raise ValueError('assess parm error')

            self.seed[key] = assessValue

        # 存储
        if save == True:
            if csv_path != '':
                self._save_csv(pdoutput=self.pdoutput, csv_path=csv_path)
            else:
                local_path = os.path.dirname(__file__)
                self._save_csv(pdoutput=self.pdoutput,csv_path=local_path+'/tmp.csv')

        return 

    def _save_csv(self,pdoutput,csv_path):
        return pdoutput.to_csv(csv_path,index=False,encoding='GBK')


    def _store(self,pdoutput,dic):
        df = pd.DataFrame().from_dict(dic,orient='index').T
        pdoutput = pdoutput.append(df)
        return pdoutput.reset_index(drop=True)

if __name__ == '__main__':

    # 初始参数:
    initParam = {'n_estimators': 120,
                 'oob_score': False,
                 'max_features': 30,
                 'max_depth': 19,
                 'min_samples_split': 2,
                 'min_samples_leaf': 1}


    param ={
            # 'n_estimators':range(5,155,5),
            # 'oob_score':(True,False),
            # 'max_features':range(1,50),
            # 'max_depth':range(1,50,1),
            # 'min_samples_split':range(2,11,1),
            'min_samples_leaf':range(1,3,1)
            }
    #

    b = Base(parmRange=param,seed=initParam)
    # 注意：
    #  train(parm,kk=1,bb=2)
    # 其中,parm为模型参数，必须第一个传参，后面可输入其他参数但需固定值
    # train中对应参数用字典传递进去
    b.fit(model=train,show=True,save=True)
