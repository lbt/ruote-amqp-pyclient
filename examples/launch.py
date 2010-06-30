from amqplib import client_0_8 as amqp
import simplejson as json
import sys

if len(sys.argv) != 2:
    print "Usage: python launch.py <version>"
    sys.exit(1)

# Specify a process definition
pdef = {
    "definition": """
        Ruote.process_definition :name => 'test' do
          sequence do
            developer
            builder
          end
        end
      """,
    "fields" : {
        "version" : sys.argv[1]
        }
    }

# Connect to the amqp server
conn = amqp.Connection(host="amqpvm", userid="ruote",
                       password="ruote", virtual_host="ruote-test", insist=False)
chan = conn.channel()

# Encode the message as json
msg = amqp.Message(json.dumps(pdef))
# delivery_mode=2 is persistent
msg.properties["delivery_mode"] = 2 

# Publish the message.

# Notice that this is sent to the anonymous/'' exchange (which is
# different to 'amq.direct') with a routing_key for the queue
chan.basic_publish(msg, exchange='', routing_key='ruote_workitems')

# and wrap up.
chan.close()
conn.close()
