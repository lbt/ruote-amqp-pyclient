#!/usr/bin/python
import sys
import os
import random

from  RuoteAMQP.workitem import Workitem
from  RuoteAMQP.participant import Participant

import simplejson as json

class MyPart(Participant):

    # Note that *all* Exceptions thrown here are caught by the
    # framework and reported to the Engine.
    def consume(self):
        # We have a workitem passed to us
        wi = self.workitem
        print "Got a workitem:"
        print json.dumps(wi.to_h(), indent=4)

        # we do something:
        size=random.randint(500,1000)
        print "\nSize is %s" % size

        # Write it into the workitem
        wi.set_field("image.size", size)
        wi.set_result(True)

        # That's it :)


# Now to start a participant (this could be in a 
print "Started a python participant"
p = MyPart(ruote_queue="sizer", amqp_host="amqpvm", amqp_vhost="ruote-test")
p.register("sizer", {'queue':'sizer'})
p.run()
