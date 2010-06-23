import simplejson as json
from copy import deepcopy

class FlowExpressionId(object):
    """
    The FlowExpressionId (fei for short) is an process expression identifier.
    Each expression when instantiated gets a unique fei.
    
    Feis are also used in workitems, where the fei is the fei of the
    [participant] expression that emitted the workitem.
    
    Feis can thus indicate the position of a workitem in a process tree.
    
    Feis contain four pieces of information :
  
    * wfid : workflow instance id, the identifier for the process instance
    * sub_wfid : the identifier for the sub process within the main instance
    * expid : the expression id, where in the process tree
    * engine_id : only relevant in multi engine scenarii (defaults to 'engine')
    """

    CHILD_SEP = '_'

    def __init__(self, h):
        self._h = deepcopy(h)

    def __getitem__(self, key):
        return self._h[key]

    def expid(self):
        return self._h['expid']

    def wfid(self):
        return self._h['wfid']

    def sub_wfid(self):
        return self._h['sub_wfid']

    def engine_id(self):
        return self._h['engine_id']

    def expid(self):
        return self._h['expid']

    def to_storage_id(self):
        return "%s!%s!%s" % (self._h['expid'], self._h['sub_wfid'], self._h['wfid'])

    def child_id(self):
        """
        Returns the last number in the expid. For instance, if the expid is
        '0_5_7', the child_id will be '7'.
        """
        try:
            return int(self._h.expid.split(CHILD_SEP)[-1])
        except ValueError:
            return None       

    def direct_child(self, other_fei):
        
        for k in  [ "sub_wfid", "wfid", "engine_id" ] :
            if self._h[k] != other_fei[k] : return False 

        pei = join(CHILD_SEP, reverse(split(CHILD_SEP, other_fei['expid'])))
        if pei == self._h['expid']: return True
        return False

class Workitem(object):
    """
    A workitem can be thought of an "execution token", but with a payload
    (fields).

    The payload/fields MUST be JSONifiable.
    """

    def __init__(self, msg):
        self._h = json.loads(msg)
        self._fei = FlowExpressionId(self._h['fei'])

    def to_h(self):
        "Returns the underlying Hash instance."
        return self._h

    def sid(self):
        """
        The string id for this workitem (something like "0_0!!20100507-wagamama").
        Not implemented.
        """
        return self._fei.to_storage_id()


    def wfid(self):
        """
        Returns the "workflow instance id" (unique process instance id) of
        the process instance which issued this workitem.
        """
        return self._fei.wfid

    def fei(self):
        "Returns a Ruote::FlowExpressionId instance."
        return FlowExpressionId(self._h['fei'])


    def dup(self):
        """Returns a complete copy of this workitem."""
        return Workitem(self._h)

    def participant_name(self):
        """
        The participant for which this item is destined. Will be nil when
        the workitem is transiting inside of its process instance (as opposed
        to when it's being delivered outside of the engine).
        """
        return self._h['participant_name']

    def fields(self):
        "Returns the payload, ie the fields hash."
        return self._h['fields']

    def set_fields(self, fields):
        """
        Sets all the fields in one sweep.
        Remember : the fields must be a JSONifiable hash.
        """
        self._h['fields'] = fields

    def result(self):
        """
        A shortcut to the value in the field named __result__

        This field is used by the if expression for instance to determine
        if it should branch to its 'then' or its 'else'.
        """
        return self.fields()['__result__']

    def set_result(self, r):
        "Sets the value of the 'special' field __result__"
        self.fields()['__result__'] = r

    def dispatch_at(self):
        "When was this workitem dispatched ?"
        return self.fields()['dispatched_at']

    def forget(self):
        "Is this workitem forgotten? If so no reply is expected."
        return self.fields()['params']['forget']

    def __eq__ (self, other):
        "Warning : equality is based on fei and not on payload !"
        if isinstance(other, type(self)): return false
        return self._h['fei'] == other.h['fei']

    def __ne__ (self, other):
        if (self == other): return True
        return False 

    def hash(self):
        "Warning : hash is fei's hash."
        return hash(self._h['fei'])


    def lookup(self, key, container_lookup=False):
        """
        For a simple key
           workitem.lookup('toto')
        is equivalent to
           workitem.fields['toto']
        but for a complex key
           workitem.lookup('toto.address')
        is equivalent to
           workitem.fields['toto']['address']
        """
        ref=self._h['fields']
        for k in key.split("."):
            if not key in ref:
                return None
            ref = ref[key]
        return ref

    def lf(self, key, container_lookup=False):
        "'lf' for 'lookup field'"
        return self.lookup(key, container_lookup)

    def set_field(self, key, value):
        """
        Like #lookup allows for nested lookups, #set_field can be used
        to set sub fields directly.

        workitem.set_field('customer.address.city', 'Pleasantville')

        Warning : if the customer and address field and subfield are not present
        or are not hashes, set_field will simply create a "customer.address.city"
        field and set its value to "Pleasantville".
        """

        ref=self._h['fields']
        ks = key.split(".")
        last = ks.pop()
        for k in ks:
            if not k in ref:
                ref[k]={}
            ref = ref[k]
        ref[last] = value


    def timed_out(self):
        "Shortcut for wi.fields['__timed_out__']"
        return self._h['fields']['__timed_out__']

    def error(self):
        "Shortcut for wi.fields['__error__']"
        return self._h['fields']['__error__']

    def params(self):
        """
        Shortcut for wi.fields['params']
        When a participant is invoked in the process definition as
           participant_name :ref => 'toto', :task => 'x"
        then when the participant's consume() is executed
           workitem.params
        contains
           { 'ref' => 'toto', 'task' => 'x' }
        """
        return self._h['fields']['params']
