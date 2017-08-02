
# 存储csv并展示
def store_csv(csv_path,dic):
    df = pd.DataFrame().from_dict(dic, orient='index').T
    def _save_csv(pdoutput, csv_path):
        if os.path.exists(csv_path) is False:
            return pdoutput.to_csv(csv_path, index=False, encoding='GBK')
        else:
            return pdoutput.to_csv(csv_path, index=False, encoding='GBK',header=False,mode='a')

    if csv_path != '':
        _save_csv(pdoutput=df, csv_path=csv_path)
        read = pd.read_csv(csv_path,encoding='GBK')
        print(read)
    else:
        local_path = os.path.dirname(__file__)
        _save_csv(pdoutput=df, csv_path=local_path + '/tmp.csv')
