from google.cloud import pubsub
import time
import os
import mysql.connector
import datetime
from mysql.connector import pooling

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'key.json'
proj_name = 'project-name'
sub_name = 'subname'
config = {
    'user': 'root',
    'password': '12345',
    'host': '127.0.0.1',
    'database': 'sqlps',
    'charset': 'utf8',
    'raise_on_warnings': True,
}
cnxpool = mysql.connector.pooling.MySQLConnectionPool(pool_name=None,
                                                      pool_size=15,
                                                      pool_reset_session=True,
                                                      **config)


def callback(message):
    if message.data:
        cnx = cnxpool.get_connection()
        cursor = cnx.cursor()
        try:
            data = message.data.decode("UTF-8")
            time = datetime.datetime.now()
            tuple = (data, time,)
            sql = ("INSERT INTO psmessage(message,timestamp) VALUES (%s,%s)")
            cursor.execute(sql, tuple)
            cnx.commit()
            print("{} - {} - Message sent to database".format(datetime.datetime.now(), data))
            cursor.close()
            cnx.close()
            message.ack()
        except Exception as e:
            print(e)


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
