from kafka import KafkaProducer
from atproto import FirehoseSubscribeReposClient, parse_subscribe_repos_message
from atproto import CAR, models
import sys
import json 
import random


producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
    

)



def process_post_record(record):
    
        text = record['text']


        return {
            'text': text,

        }

def isascii(s):
    return all(ord(c) < 128 for c in s)


def random_sentences():
     with open('randomsentences.json', 'r') as file:

        data = json.load(file)
        random_sentence = random.choice(data['sentences'])
        return {
            'text': random_sentence
        }
     


def on_message_handler(message):
     parsed_message = parse_subscribe_repos_message(message)

     if isinstance(parsed_message, models.ComAtprotoSyncSubscribeRepos.Commit):
  
            car = CAR.from_bytes(parsed_message.blocks)
            

            ops = parsed_message.ops
            
            for op in ops:
                if op.action == 'create' and op.path.startswith('app.bsky.feed.post'):

                    record = car.blocks.get(op.cid)
                    if record:
                        post_data = process_post_record(record)
                        if post_data:
                            if isascii(post_data['text']):
                                producer.send('test', value=post_data)
                            else:
                                producer.send('test', value=random_sentences())

                    
                    else:
                        producer.send('test', value=random_sentences())

                    

            
def main(): 
    print("Starting Kafka Producer")
    client = FirehoseSubscribeReposClient()
    try:
         client.start(on_message_handler)
    except KeyboardInterrupt:
        print("Exiting Kafka Producer")
        sys.exit(0)

if __name__ == '__main__':
    main()
