# Copyright (C) 2010 Nokia Corporation and/or its subsidiary(-ies).
# Contact: David Greaves <ext-david.greaves@nokia.com>
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from amqplib import client_0_8 as amqp

try:
    import json
except ImportError:
    import simplejson as json


class Launcher(object):
    """
    A Launcher will launch a Ruote process.

    Workitems arrive via AMQP, are processed and returned to the Ruote engine.

    Cancel is not yet implemented.
    """

    def __init__(self,
                 amqp_host="localhost", amqp_user="boss",
                 amqp_pass="boss", amqp_vhost="boss",
                 conn=None):
        if conn is not None:
            self.conn = conn
        else:
            self.host = amqp_host
            self.user = amqp_user
            self.pw = amqp_pass
            self.vhost = amqp_vhost
            self.conn = amqp.Connection(host=self.host,
                                        userid=self.user,
                                        password=self.pw,
                                        virtual_host=self.vhost,
                                        insist=False)
        if self.conn is None:
            raise Exception("No connection")
        self.chan = self.conn.channel()
        if self.chan is None:
            raise Exception("No channel")

#        # Currently ruote-amqp uses the anonymous direct exchange
#        self.chan.exchange_declare(exchange="", type="direct", durable=True,
#                              auto_delete=False)

    def launch(self, process, fields=None, variables=None):
        """
        Launch a process definition
        """
        if fields and not isinstance(fields, dict):
            raise TypeError("fields should be type dict")
        if variables and not isinstance(variables, dict):
            raise TypeError("variables should be type dict")
        pdef = {
            "definition": process,
            "fields" : fields,
            "variables" : variables
            }
        # Encode the message as json
        msg = amqp.Message(json.dumps(pdef))
        # delivery_mode=2 is persistent
        msg.properties["delivery_mode"] = 2

        # Publish the message.
        self.chan.basic_publish(msg, exchange='',
                                routing_key='ruote_workitems')
