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
from workitem import Workitem
import AIR

try:
     import json
except ImportError:
     import simplejson as json

import threading

class Launcher(AIR.AMQPServer):
     """
     A Launcher will launch a Ruote process.

     Workitems arrive via AMQP, are processed and returned to the Ruote engine.

     Cancel is not yet implemented.
     """

     def __init__(self,*args, **kwargs):
          super(Launcher, self).__init__(*args, **kwargs)

#          # Currently ruote-amqp uses the anonymous direct exchange
#          self.chan.exchange_declare(exchange="", type="direct", durable=True,
#                                     auto_delete=False)

     def launch(self, process, fields=None, variables=None):
          """
          Launch a process definition
          """
          # FIXME : Raise exception if fields not dict
          pdef = {
               "definition": process,
               "fields" : fields
               "variables" : variables
               }
          # Encode the message as json
          msg = amqp.Message(json.dumps(pdef))
          # delivery_mode=2 is persistent
          msg.properties["delivery_mode"] = 2 
          
          # Publish the message.
          self.chan.basic_publish(msg, exchange='',
                                  routing_key='ruote_workitems')
