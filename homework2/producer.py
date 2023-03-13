from faker import Faker
import pika

from mongoengine import connect, Document, StringField, BooleanField


NUMBER_CONTACTS = 12
fake = Faker()

connect(host="mongodb+srv://NickGoit:Dp_ua_1989@clusterlearn.5mbivax.mongodb.net/HomeWork2", ssl=True)


class Contact(Document):
    fullname = StringField(required=True, max_length=50)
    email = StringField(required=True, max_length=50)
    done = BooleanField(default=False)


def contact_seeds():
    for _ in range(NUMBER_CONTACTS):
        contact = Contact(
            fullname=fake.name(),
            email=fake.email()
        ).save()


def contact_id():
    contacts = Contact.objects()
    for contact in contacts:
        print(f'Id:{contact.id}, name: {contact.fullname}')


credentials = pika.PlainCredentials('guest', 'guest')
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost',
                                                               port=5672,
                                                               credentials=credentials))
channel = connection.channel()

channel.exchange_declare(exchange='email_task', exchange_type='direct')
channel.queue_declare(queue='email_queue', durable=True)
channel.queue_bind(exchange='email_task', queue='email_queue')


def main():
    contacts = Contact.objects()
    for contact in contacts:
        message = f'Id:{contact.id},name:{contact.fullname}'

        channel.basic_publish(
            exchange='email_task',
            routing_key='email_queue',
            body=message.encode(),
            properties=pika.BasicProperties(
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
            ))
        print(" [x] Sent %r" % message)
    connection.close()


def update_bool(text):
    id_ = text.split(',')[0].split(':')[1]
    contact = Contact.objects(pk=id_)
    contact.update(done=False)


if __name__ == "__main__":
    contact_seeds()
    main()
    # update_bool('Id:640f782b7adf7f4e485ba25b,name:Anthony Craig')






