import email
import json
import logging
import re
import sys

from kafka import KafkaProducer


def parse_email(email_msg) -> dict:
    message = {'subject': email_msg['Subject'],
               'message': email_msg.get_payload()}
    regex = r'^(.*?)(?:\s*<)?([^<>]+@[^<>]+)(?:>)?$'
    parsed_sender = re.match(regex, email_msg['From'])
    parsed_receiver = re.match(regex, email_msg['To'])

    if parsed_sender:
        message['sender'] = parsed_sender.group(1).strip() if parsed_sender.group(1) else 'None'
        message['sender_email'] = parsed_sender.group(2).strip() if parsed_sender else 'None'
    if parsed_receiver:
        message['recipient'] = parsed_receiver.group(1).strip() if parsed_receiver.group(1) else 'None'
        message['recipient_email'] = parsed_receiver.group(2).strip() if parsed_receiver.group(2) else 'None'

    message['policy'] = 'Finance' if message['recipient_email'] == 'finance@hpe-solutions.com' else 'Medical'
    message['status'] = 'Processing'
    message['id'] = 1
    return message


def send_kafka_message(json_msg):
    producer = KafkaProducer(bootstrap_servers='${KAFKA_BROKER_URL}')
    if json_msg['policy'] == 'Finance':
        topic = 'input.email.dlp.Finance'
    else:
        topic = 'input.email.dlp.Medical'
    producer.send(topic, bytes(json.dumps(json_msg), 'utf-8'))


if __name__ == '__main__':
    # invoke manually with below from the postfix container:
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
