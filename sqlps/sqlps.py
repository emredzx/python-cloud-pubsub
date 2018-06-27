from google.cloud import pubsub
import time
import os
import mysql.connector
import datetime

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'key.json'
proj_name = 'project name'
sub_name = 'sub name'
config = {
    'user': 'root',
    'password': 'password',
    'host': '127.0.0.1',
    'database': 'sqlps',
    'charset': 'utf8',
    'raise_on_warnings': True,
}


def callback(message):
    if message.data:
        cnx = mysql.connector.connect(**config)
        cursor = cnx.cursor()
        try:
            data = message.data.decode("UTF-8")
            time = datetime.datetime.now()
            tuple = (data,time,)
            sql = ("INSERT INTO psmessage(message,timestamp) VALUES (%s,%s)")
            cursor.execute(sql, tuple)
            cnx.commit()
            print("{} - {} - Message sent to database".format(datetime.datetime.now(), data))
        except Exception as e:
            print(e)
        finally:
            cursor.close()
            cnx.close()
    message.ack()


def sub_pull():
    subscriber = pubsub.SubscriberClient()
    subscription_path = subscriber.subscription_path(
        proj_name, sub_name)
    subscriber.subscribe(subscription_path, callback=callback)
    print('Listening for messages on: {}'.format(subscription_path))
    while True:
        time.sleep(5)


if __name__ == "__main__":
    sub_pull()
