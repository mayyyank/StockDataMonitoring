import json, requests, time
from kafka import KafkaProducer

# from Consumer import stop_event


def kafkaProducer():
    api_key='TKISFL7U6OWSLNY3'
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'))

    companies = ['AAPL','MSFT','GOOGL','AMZN','TSLA','NVDA','META','AVGO','AMD','ORCL',
                 'KO','PEP','WMT','MCD','PG','JPM','BAC','WFC','GS','JNJ','PFE','F',
                 'GM','BA','NFLX']
    # companies=['AAPL','MSFT','GOOGL']

    for company in companies:
        URL = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={company}&interval=1min&apikey={api_key}"
        response = requests.get(URL)

        if response.status_code == 200:
            data = response.json()
            producer.send(topic='StockData', value=data)
            print(f'Produced data for {company}')

        time.sleep(8)

    producer.close()
    # stop_event.set()

    print('Producer has stop sending data')
kafkaProducer()