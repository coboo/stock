import tushare as ts
def run():
    df = ts.get_today_all()
    df.to_csv('d:\python\profit.csv',encoding='utf-8')

if __name__ =='__main__':
    run()