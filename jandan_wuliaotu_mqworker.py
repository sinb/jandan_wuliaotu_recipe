# encoding=utf-8
import json
import os
import datetime
import pika
from tqdm import tqdm
import requests

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost',
                                                               port=5672, virtual_host='/', connection_attempts=10,
                                                               retry_delay=60,
                                                               heartbeat_interval=500))

channel = connection.channel()
mq_name = "jandan_wuliaotu"
channel.queue_declare(queue=mq_name, durable=True)

download_dir = "jandan_wuliaotu"
if not os.path.exists(download_dir):
    os.makedirs(download_dir)
print(' [*] Waiting for messages. To exit press CTRL+C')


def callback(ch, method, properties, body):
    print(" [x] Received \n%r" % body)
    print(" [x] Done")
    download_tool(download_dir, body)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def write_meta(content, filename):
    with open(filename, 'w') as f:
        try:
            f.write(content.encode('utf-8'))
        except:
            f.write(content)


def download_one_request(dir, name, url):
    try:
        response = requests.get(url, stream=True, timeout=27)
        with open(os.path.join(dir, name), "wb") as handle:
            for data in tqdm(response.iter_content()):
                handle.write(data)
    except Exception as e:
        print("time out!, reconnect")
        try:
            response = requests.get(url, stream=True,  timeout=27)
            with open(os.path.join(dir, name), "wb") as handle:
                for data in tqdm(response.iter_content()):
                    handle.write(data)
        except Exception as e:
            print("give up")
            return

def download_tool(root_dir, message_body):
    body = json.loads(message_body)
    folder = os.path.join(root_dir, body['folder_name'])
    if not os.path.exists(folder):
        os.makedirs(folder)

    sub_folder = os.path.join(folder, body['sub_folder_name'])
    if not os.path.exists(sub_folder):
        os.makedirs(sub_folder)

    if body['text']:
        write_meta(body['text'], os.path.join(sub_folder, "meta.txt"))
    for idx, img_url in enumerate(body['img_list']):
        extension = img_url.split('.')[-1]
        download_one_request(sub_folder, str(idx) + "." + extension, img_url)
        print(datetime.datetime.now())


channel.basic_qos(prefetch_count=1)
channel.basic_consume(callback,
                      queue=mq_name)

channel.start_consuming()
