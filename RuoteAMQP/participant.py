#!/usr/bin/python2.6
#~ Copyright (C) 2010 Nokia Corporation and/or its subsidiary(-ies).
#~ Contact: David Greaves <ext-david.greaves@nokia.com>
#~ This program is free software: you can redistribute it and/or modify
#~ it under the terms of the GNU General Public License as published by
#~ the Free Software Foundation, either version 3 of the License, or
#~ (at your option) any later version.

#~ This program is distributed in the hope that it will be useful,
#~ but WITHOUT ANY WARRANTY; without even the implied warranty of
#~ MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#~ GNU General Public License for more details.

#~ You should have received a copy of the GNU General Public License
#~ along with this program.  If not, see <http://www.gnu.org/licenses/>.

import sys, traceback
from amqplib import client_0_8 as amqp
from RuoteAMQP.workitem import Workitem
try:
    import json
except ImportError:
    import simplejson as json

class Participant(object):
    """
    A Participant will do work in a Ruote process. Participant is
    essentially abstract and must be subclassed to provide a useful
    consume() method.

    Workitems arrive via AMQP, are processed and returned to the Ruote engine.

    Cancel is not yet implemented.
    """

    def __init__(self, ruote_queue,
               amqp_host = "localhost", amqp_user = "ruote",
               amqp_pass = "ruote", amqp_vhost = "ruote"):

        self.workitem = None
        self._running = None

        self._conn = amqp.Connection(host=amqp_host, userid=amqp_user,
                                password=amqp_pass, virtual_host=amqp_vhost,
                                insist=False)

        self._chan = self._conn.channel()

        # Declare a shareable queue for the participant
        self._chan.queue_declare(queue=ruote_queue, durable=True,
                            exclusive=False, auto_delete=False)

        # Currently ruote-amqp uses the anonymous direct exchange
        self._chan.exchange_declare(exchange="", type="direct", durable=True,
                               auto_delete=False)

        # bind our queue using a routing key of our queue name
        self._chan.queue_bind(queue=ruote_queue, exchange="",
                          routing_key=ruote_queue)

        # and set a callback for workitems
        self._chan.basic_consume(queue=ruote_queue, no_ack=True,
                            callback=self.workitem_callback)

    def workitem_callback(self, msg):
        "This is where a workitem message is handled"

        try:
            self.workitem = Workitem(msg.body)
        except ValueError, exobj:
            print "Exception decoding incoming json"
            print '-'*60
            print msg.body
            print '-'*60
            print "Note: Now re-raising exception"
            raise exobj

        try:
            self.consume()
        except Exception, exobj:
            # This should be configureable:
            print "Exception"
            print '-'*60
            traceback.print_exc(file=sys.stderr)
            print '-'*60
            print "Note: for information only. Workitem returning with "\
                    "result=false"
            # And this should be the 'standardised' way of passing
            # errors back via a workitem
            # wi.set_error(e)
            self.workitem.Exception = "%s" % exobj
            self.workitem.result = False

        if not self.workitem.forget:
            self.reply_to_engine()

    def consume(self):
        """
        Override the consume() method in a subclass to do useful work.
        The workitem attribute contains a Workitem.
        """
        pass

    def run(self):
        """
        Currently an infinite loop waiting for messages on the AMQP channel.
        """
        self._running = True
        while self._running:
            self._chan.wait()
        self._chan.basic_cancel()
        self._chan.close()
        self._conn.close()


    def finish(self):
        "Closes channel and connection"
        self._running = False


    def reply_to_engine(self, workitem=None):
        """
        When the job is complete the workitem is passed back to the
        ruote engine.  The consume() method should set the
        workitem.result() if required.
        """
        if not workitem:
            workitem = self.workitem
        msg = amqp.Message(json.dumps(workitem.to_h()))
        # delivery_mode=2 is persistent
        msg.properties["delivery_mode"] = 2

        # Publish the message.
        # Notice that this is sent to the anonymous/'' exchange (which is
        # different to 'amq.direct') with a routing_key for the queue
        self._chan.basic_publish(msg, exchange='',
                routing_key='ruote_workitems')

    def register(self, name, options):
        """
        Relies on the engine supporting the "engine_command"
        participant.
        """
        if 'position' not in options:
            options['position'] = -2
        command = {
            "register": "RuoteAMQP::Participant",
            "name" : name,
            "options" : options
            }
        # Encode the message as json
        msg = amqp.Message(json.dumps(command))
        # delivery_mode=2 is persistent
        msg.properties["delivery_mode"] = 2
        self._chan.basic_publish(msg, exchange='',
                routing_key='ruote_workitems')
