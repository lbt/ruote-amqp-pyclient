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

""" Abstract participant class """

from __future__ import with_statement
import sys, traceback
from threading import Thread
from urllib2 import HTTPError
from amqplib import client_0_8 as amqp
from RuoteAMQP.workitem import Workitem

try:
    import json
except ImportError:
    import simplejson as json

def format_ruby_backtrace(trace):
    """Formats a python traceback so that a ruby Exception accepts it 
       as a backtrace."""
    return ["%s:%d: in `%s %s'" % (item[0], item[1], item[2], item[3]) \
            for item in trace]

def format_exception(exc):
    """Formats exception to more informative string based on exception type."""
    if isinstance(exc, HTTPError):
        # Python bug, HTTPError does not always have url attribute and geturl()
        # fails
        exc_str = "HTTPError: %d %s" % (exc.getcode(), exc.filename)
    # Catching EnvironmentError means we cover IOError, OSError, URLError
    # http://docs.python.org/library/exceptions.html#exceptions.EnvironmentError
    elif isinstance(exc, EnvironmentError):
        if exc.filename:
            exc_str = "{0}({1}): {2} {3}".format(exc.__class__.__name__, \
                                                 exc.errno, exc.filename, \
                                                 exc.strerror)
        elif exc.errno and exc.strerror:
            exc_str = "{0}({1}): {2}".format(exc.__class__.__name__, \
                                                 exc.errno, exc.strerror)
        else:
            exc_str = "{0}: {1}".format(exc.__class__.__name__, str(exc))
    # osc exceptions don't set args and message correctly so str(exc) contains
    # only the exception class name. However it has a msg attribute which has 
    # sensible contents so use that
    elif hasattr(exc, "msg"):
        exc_str = "{0}: {1}".format(exc.__class__.__name__, exc.msg)
    else:
        exc_str = "{0}: {1}".format(exc.__class__.__name__, \
                                    str(exc))
    return exc_str

def print_block(msg):
    """Print message in a block with separator lines at begining and end."""
    print "-" * 78
    print msg
    print "-" * 78

class ConsumerThread(Thread):
    """Thread for running the Participant.consume()"""
    def __init__(self, participant):
        super(ConsumerThread, self).__init__()
        self.__participant = participant
        self.exception = None
        self.trace = None

    def run(self):
        try:
            self.__participant.consume()
        except Exception, exobj:
            # This should be configureable:
            print "Exception in participant %s" % \
                    (self.__participant.workitem.participant_name)
            print "while handling instance %s of process %s " % \
                    (self.__participant.workitem.wfid,
                     self.__participant.workitem.wf_name)

            print_block(traceback.format_exc())
            print "Note: for information only. Participant remains functional."\
                  "\n      Error is being signalled to the workflow (unless" \
                  "\n      this workitem is 'forgotten')."
            self.exception = exobj
            self.trace = traceback.extract_tb(sys.exc_traceback)

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

        self._conn_params = dict(
                host=amqp_host, userid=amqp_user, password=amqp_pass,
                virtual_host=amqp_vhost, insist=False)
        self._chan = None
        self._queue = ruote_queue
        self._consumer_tag = None
        self._running = False
        self.workitem = None

    def _open_channel(self, connection):
        """Open and initialize the amqp channel."""
        if self._chan is None or not self._chan.is_open:
            self._chan = connection.channel()
            # set qos option on this channel with prefetch count 1 whole message
            # of any size
            self._chan.basic_qos(0, 1, False)
            # Declare a shareable queue for the participant
            self._chan.queue_declare(
                    queue=self._queue, durable=True, exclusive=False,
                    auto_delete=False)
            # Currently ruote-amqp uses the anonymous direct exchange
            #self._chan.exchange_declare(
            #       exchange="", type="direct", durable=True, auto_delete=False)
            # Bind our queue using a routing key of our queue name
            #self._chan.queue_bind(
            #        queue=self._queue, exchange="", routing_key=self._queue)
            # and set a callback for workitems
            self._consumer_tag = self._chan.basic_consume(
                    queue=self._queue, no_ack=False,
                    callback=self.workitem_callback)
        return self._chan

    def workitem_callback(self, msg):
        """
        This is where a workitem message is handled
        """
        tag = msg.delivery_info["delivery_tag"]
        try:
            self.workitem = Workitem(msg.body)
        except ValueError, exobj:
            # Reject and don't requeue the message
            self._chan.basic_reject(tag, False)
            print "Exception decoding incoming json"
            print_block(msg.body)
            print "Note: Now re-raising exception"
            raise exobj

        # Launch consume() in separate thread so it doesn't get interrupted by
        # signals
        consumer = ConsumerThread(self)
        consumer.start()
        consumer.join()
        if consumer.exception:
            # Note: the mechanism below is different than the one
            # ruote-beanstalk uses. That sends the message in an array
            # where the first element indicates the message type
            # (workitem or error)

            self.workitem.error = format_exception(consumer.exception)
            self.workitem.trace = format_ruby_backtrace(consumer.trace)

        # Acknowledge the message as received
        self._chan.basic_ack(tag)

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
        if self._running:
            raise RuntimeError("Participant already running")
        try:
            with amqp.Connection(**self._conn_params) as conn:
                with self._open_channel(conn):
                    self._running = True
                    while self._running:
                        self._chan.wait()
        except:
            self._running = False
            raise

    def finish(self):
        """
        Closes channel and connection
        """
        if self._chan and self._chan.is_open:
            # Cancel the consumer so that we don't receive more messages
            self._chan.basic_cancel(self._consumer_tag)
        self._running = False


    def reply_to_engine(self, workitem=None):
        """
        When the job is complete the workitem is passed back to the
        ruote engine.  The consume() method should set the
        workitem.result() if required.
        """
        if not (self._chan and self._chan.is_open):
            raise RuntimeError("AMQP channel not open")
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
