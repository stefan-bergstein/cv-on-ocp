import datetime
import os
from flask import Flask, Response
from kafka import KafkaConsumer
import json
import msgpack
import numpy as np
import cv2

        
# Set the consumer in a Flask App
app = Flask(__name__)

@app.route('/video', methods=['GET'])
def video():
    """
    This is the heart of our video display. Notice we set the mimetype to 
    multipart/x-mixed-replace. This tells Flask to replace any old images with 
    new values streaming through the pipeline.
    """
    return Response(
        get_video_stream(), 
        mimetype='multipart/x-mixed-replace; boundary=frame')

def get_video_stream():
    """
    Here is where we recieve streamed images from the Kafka Server and convert 
    them to a Flask-readable format.
    """
    for msg in consumer:
        d = json.loads(msg.value)
        frame = d['frame']
        frame= np.array(d['frame'],dtype=np.uint8)
        print(d['time'] + " " + str(frame.shape))
        # Convert image to png
        ret, buffer = cv2.imencode('.jpg', frame)
        yield (b'--frame\r\n'
               b'Content-Type: image/jpg\r\n\r\n' + buffer.tobytes() + b'\r\n\r\n')

if __name__ == "__main__":

    topic = os.getenv("TOPIC", default="distributed-video1")
    bootstrap_servers = os.getenv("BOOTSTRAP_SERVER", default="localhost:9092")
    security_protocol = os.getenv("SECURITY_PROTOCOL", default="PLAINTEXT")
    ssl_check_hostname = bool(os.getenv("SSL_CHECK_HOSTNAME", default="FALSE"))
    ssl_cafile = os.getenv("SSL_CAFILE", default="./ca.crt")

    consumer = KafkaConsumer(topic, 
        value_deserializer=msgpack.unpackb,
        bootstrap_servers=[bootstrap_servers],
        ssl_check_hostname=ssl_check_hostname,
        ssl_cafile=ssl_cafile)

    print("Consume ...")
    print("BOOTSTRAP_SERVER:" + bootstrap_servers)
    print("TOPIC:" + topic)

    app.run(host='0.0.0.0', debug=True)

