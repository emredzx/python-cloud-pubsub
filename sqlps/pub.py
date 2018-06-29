from flask import Flask, request, jsonify, abort
from google.cloud import pubsub
import os
import json

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'key.json'

app = Flask(__name__)

project_name = 'projectname'
topic_name = 'topicname'

batch_settings = pubsub.types.BatchSettings(
    max_bytes=1024,  # One kilobyte
    max_latency=1,  # One second
)

publisher = pubsub.PublisherClient(batch_settings)
topic_path = publisher.topic_path(project_name, topic_name)


@app.route('/pubsub/topic', methods=['POST'])
def pushtotopic():
    if not request.json or not 'data' in request.json:
        abort(400)
    data = request.data
    data = data.decode("UTF-8")
    json_obj = json.loads(data)
    print("Message : {}".format(json_obj["data"]))
    message = bytes(json_obj["data"].encode("UTF-8"))
    publisher.publish(topic_path, data=message)
    return jsonify({'result': 'OK'}), 200


if __name__ == '__main__':
    app.run(port='6666', threaded=True, debug=True)
