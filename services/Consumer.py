from datetime import datetime, timedelta

from kafka import KafkaConsumer, TopicPartition
import pandas as pd
import json
import yaml

df_all = pd.DataFrame()
json_data = []

with open("../config/kafkaConfig.yml", "r") as ymlfile:
    cfg = yaml.load(ymlfile)


def consumer_conf():
    partition = TopicPartition(cfg["consumer"]["topic"], 0)
    start = 155
    end = 134
    consumer = KafkaConsumer(  # 'test',
        group_id=cfg["consumer"]["group_id"],
        bootstrap_servers=cfg["consumer"]["bootstrap_servers"],
        auto_offset_reset=cfg["consumer"]["auto_offset_reset"],
        enable_auto_commit=cfg["consumer"]["enable_auto_commit"],
        # fetch_min_bytes =1,
        # fetch_max_wait_ms=300,
        consumer_timeout_ms=cfg["consumer"]["consumer_timeout_ms"],
        value_deserializer=lambda m: json.loads(m.decode('ascii')))

    consumer.assign([partition])
    consumer.seek(partition, start)
    return consumer

def read_data(consumer):
    for message in consumer:
        testdict = {
            "topic" : message.topic,
            "key" : message.key,
            "partition" : message.partition,
            "offset" : message.offset
        }
        updict = {"Metadata": testdict}
        messages = message.value
        updict.update(messages)
        json_data.append(updict)
        # print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
        #                                       message.offset, message.key,
        #                                       message.value))
    return json_data

def save_to_csv(df_all):
    if (len(df_all) > 0):
        drive_letter = r'E:\\kafka_2.12-2.6.0\\data\\kafka\\'
        topic_name = r'test'
        folder_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        folder_to_save_files = drive_letter + topic_name + "_" + folder_time + ".csv"
        print(folder_to_save_files)
        df_all.to_csv(folder_to_save_files, index=False)

def commit_offset(consumer):
    consumer.commit()
    consumer.close()

if __name__ == '__main__':
    consumer = consumer_conf()
    json_data = read_data(consumer)
    df_all = pd.DataFrame.from_records(json_data)
    print(df_all.to_string())
    save_to_csv(df_all)
    commit_offset(consumer)