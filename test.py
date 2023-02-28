import akshare as ak
def download_price():
    # stock_list_str = Variable.get("stock_list")
    # stock_list_json = Variable.get("stock_list_json", deserialize_json=True)
    # stock_sse_summary = ak.stock_sse_summary()
    sy = "sh900905"
    stock_zh_b_daily_qfq_df = ak.stock_zh_b_daily(symbol=sy, start_date="20101103", end_date="20201116",
                                              adjust="qfq")
    # ticker = "aapl"
    # msft = yf.Ticker(ticker)
    # hist = msft.history(period="max")
    # print(type(hist))
    # print(hist.shape)
    # print(hist)
    print(type(stock_zh_b_daily_qfq_df))
    print(stock_zh_b_daily_qfq_df.shape)
    print(stock_zh_b_daily_qfq_df)
    # with open(f'/Users/jiazhenwang/workspace/airflow/logs/{sy}.csv','w') as writer:
    #     stock_zh_b_daily_qfq_df.to_csv(writer, index=True)
    print("Finished downloading price data")

download_price()



# mongodump --uri=mongodb://wjz:wjz123456@172.25.170.246:27017,172.25.171.105:27017,172.25.171.6:27017 --authenticationDatabase=admin -d=Logitech -c=RawDataBucket -q='{"timestamp": {"$eq": 1675872000000}}' -o=/Users/jiazhenwang/Downloads/dump2/20230209
# mongodump --uri=mongodb://wjz:wjz123456@172.25.170.246:27017,172.25.171.105:27017,172.25.171.6:27017 --authenticationDatabase=admin -d=Logitech -c=RawDataBucket -q='{"timestamp": {"$eq": 1675785600000}}' -o=/Users/jiazhenwang/Downloads/dump2/20230208
# mongodump --uri=mongodb://wjz:wjz123456@172.25.170.246:27017,172.25.171.105:27017,172.25.171.6:27017 --authenticationDatabase=admin -d=Logitech -c=RawDataBucket -q='{"timestamp": {"$eq": 1675699200000}}' -o=/Users/jiazhenwang/Downloads/dump2/20230207
# mongodump --uri=mongodb://wjz:wjz123456@172.25.170.246:27017,172.25.171.105:27017,172.25.171.6:27017 --authenticationDatabase=admin -d=Logitech -c=RawDataBucket -q='{"timestamp": {"$eq": 1675612800000}}' -o=/Users/jiazhenwang/Downloads/dump2/20230206
# mongodump --uri=mongodb://wjz:wjz123456@172.25.170.246:27017,172.25.171.105:27017,172.25.171.6:27017 --authenticationDatabase=admin -d=Logitech -c=RawDataBucket -q='{"timestamp": {"$eq": 1675526400000}}' -o=/Users/jiazhenwang/Downloads/dump2/20230205
# mongodump --uri=mongodb://wjz:wjz123456@172.25.170.246:27017,172.25.171.105:27017,172.25.171.6:27017 --authenticationDatabase=admin -d=Logitech -c=RawDataBucket -q='{"timestamp": {"$eq": 1675440000000}}' -o=/Users/jiazhenwang/Downloads/dump2/20230204
# mongodump --uri=mongodb://wjz:wjz123456@172.25.170.246:27017,172.25.171.105:27017,172.25.171.6:27017 --authenticationDatabase=admin -d=Logitech -c=RawDataBucket -q='{"timestamp": {"$eq": 1675353600000}}' -o=/Users/jiazhenwang/Downloads/dump2/20230203
# mongodump --uri=mongodb://wjz:wjz123456@172.25.170.246:27017,172.25.171.105:27017,172.25.171.6:27017 --authenticationDatabase=admin -d=Logitech -c=RawDataBucket -q='{"timestamp": {"$eq": 1675267200000}}' -o=/Users/jiazhenwang/Downloads/dump2/20230202
# mongodump --uri=mongodb://wjz:wjz123456@172.25.170.246:27017,172.25.171.105:27017,172.25.171.6:27017 --authenticationDatabase=admin -d=Logitech -c=RawDataBucket -q='{"timestamp": {"$eq": 1675180800000}}' -o=/Users/jiazhenwang/Downloads/dump2/20230201
# mongodb://wjz:wjz123456@172.25.170.246:27017,172.25.171.105:27017,172.25.171.6:27017/?tls=false&readPreference=secondaryPreferred&authMechanism=DEFAULT&authSource=admin&replicaSet=rs0