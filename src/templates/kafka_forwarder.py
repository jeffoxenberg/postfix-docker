import email
import json
import logging
import os
import sys

from kafka import KafkaProducer


def parse_email(email_msg) -> dict:
    return {'date': email_msg['Date'],
            'subject': email_msg['Subject'],
            'from_address': email_msg['From'],
            'to_address': email_msg['To'],
            'body': email_msg.get_payload()}


def send_kafka_message(json_msg):
    producer = KafkaProducer('${KAFKA_BROKER_URL}')
    producer.send('in', bytes(json.dumps(json_msg), 'utf-8'))


if __name__ == '__main__':
    # invoke with:
    # echo "testing message1" | mail -s "test subject1" -r "testsender1@hpe-solutions.com" testsender2@hpe-solutions.com
    logging.basicConfig(filename='/echolog/echokafka.log', level=logging.INFO,
                        format='%(asctime)s = %(levelname)s - %(message)s')

    email_msg = email.message_from_file(sys.stdin)
    parsed_email = parse_email(email_msg)
    logging.info(f'Parsed email: {parsed_email}')
    try:
        send_kafka_message(parsed_email)
    except Exception as e:
        logging.error(e)
