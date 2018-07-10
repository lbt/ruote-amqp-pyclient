#!/usr/bin/python

try:
    import json
except ImportError:
    import simplejson as json
from copy import deepcopy


class DictAttrProxy(object):
    """
    This allows a dict object to be accessed in a pretty way.
    Given :
      wi = { 'ev': { 'val': 5, 'items': [1,2,3], 'nest': { 'in': 'In', 'out': 'Out' } } }
    then
      wip = DictAttrProxy(wi)
      wip.ev.val => 5
      wip.ev.items[1]=5
      wi => {'ev': {'items': [1, 5, 3], 'val': 5}}

    It is iterable and nested dicts will be proxied too
    """
    def __init__(self, d):
        # This:
        #   self._d = d
        # won't work as we can't set a local attribute as we override
        # __setattr__ so instead we poke straight into the __dict__ :
        self.__dict__['_d'] = d

    # Any attempt to get an attr looks it up in the proxied dict
    # any nested dict items are also proxied
    def __getattr__(self, attr):
        r = self._d.get(attr, None)
        if type(r) is dict:
            return DictAttrProxy(r)
        return r

    # Note that writing into an entry creates it.
    def __setattr__(self, attr, value):
        self._d[attr] = value

    # Passthru to the dict __iter__() and /__next__()
    def __iter__(self):
        return self._d.__iter__()

    def __next__(self):
        r = self._d.next()
        if type(r) is dict:
            return DictAttrProxy(r)
        return r

    # and if we want to use this syntax to get at a nested dict:
    def as_dict(self):
        return self._d


class FlowExpressionId(object):
    """
    The FlowExpressionId (fei for short) is an process expression identifier.
    Each expression when instantiated gets a unique fei.

    Feis are also used in workitems, where the fei is the fei of the
    [participant] expression that emitted the workitem.

    Feis can thus indicate the position of a workitem in a process tree.

    Feis contain four pieces of information :

    * wfid : workflow instance id, the identifier for the process instance
    * subid : the identifier for the sub process within the main instance
    * expid : the expression id, where in the process tree
    * engine_id : only relevant in multi engine scenarii (defaults to 'engine')
    """

    CHILD_SEP = '_'

    def __init__(self, h):
        self._h = deepcopy(h)

    def __getitem__(self, key):
        return self._h[key]

    @property
    def expid(self): return self._h['expid']

    @property
    def wfid(self): return self._h['wfid']

    @property
    def subid(self): return self._h['subid']

    @property
    def engine_id(self): return self._h['engine_id']

    def to_storage_id(self):
        return "%s!%s!%s" % (
            self._h['expid'],
            self._h['subid'] if self._h['subid'] else self._h['sub_wfid'],
            self._h['wfid'])

    @property
    def child_id(self):
        """
        Returns the last number in the expid. For instance, if the expid is
        '0_5_7', the child_id will be '7'.
        """
        try:
            return int(self._h.expid.split(self.CHILD_SEP)[-1])
        except ValueError:
            return None

    def direct_child(self, other_fei):

        for k in ["sub_wfid", "wfid", "engine_id"]:
            if self._h[k] != other_fei[k]:
                return False

        pei = self.CHILD_SEP.join(list(reversed(
            other_fei['expid'].split(self.CHILD_SEP))))
        if pei == self._h['expid']:
            return True
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

    @property
    def is_cancel(self):
        "Is this a 'cancel' workitem?"
        if "cancel" in self._h and self._h["cancel"]:
            return True
        return False

    @property
    def sid(self):
        """
        The string id for this workitem
        (something like "0_0!!20100507-wagamama").
        """
        return self._fei.to_storage_id()

    @property
    def wf_name(self):
        """
        Returns the "workflow name" (generic workflow name) of
        the process which issued this workitem.
        """
        try:
            return self._h['wf_name']
        except:
            return None

    @property
    def wfid(self):
        """
        Returns the "workflow instance id" (unique process instance id) of
        the process instance which issued this workitem.
        """
        return self._fei.wfid

    @property
    def fei(self):
        "Returns a Ruote::FlowExpressionId instance."
        return FlowExpressionId(self._h['fei'])

    def dup(self):
        """Returns a complete copy of this workitem."""
        return Workitem(json.dumps(self._h))

    @property
    def participant_name(self):
        """
        The participant for which this item is destined. Will be nil when
        the workitem is transiting inside of its process instance (as opposed
        to when it's being delivered outside of the engine).
        """
        try:
            return self._h['participant_name']
        except:
            return None

    @property
    def fields(self):
        "Returns the payload, ie the fields hash."
        try:
            return DictAttrProxy(self._h['fields'])
        except:
            return DictAttrProxy({})

    @fields.setter
    def fields(self, fields):
        """
        Sets all the fields in one sweep.
        Remember : the fields must be a JSONifiable hash.
        """
        self._h['fields'] = fields

    @property
    def result(self):
        """
        A shortcut to the value in the field named __result__

        This field is used by the if expression for instance to determine
        if it should branch to its 'then' or its 'else'.
        """
        return self.fields.__result__

    @result.setter
    def result(self, r):
        "Sets the value of the 'special' field __result__"
        self.fields.__result__ = r

    @property
    def dispatched_at(self):
        "When was this workitem dispatched ?"
        return self.fields.dispatched_at

    @property
    def forget(self):
        "Is this workitem forgotten? If so no reply is expected."
        try:
            if self.params.forget:
                return True
        except:
            pass
        return False

    @forget.setter
    def forget(self, value):
        assert isinstance(value, bool)
        self.params.forget = value

    def __eq__(self, other):
        "Warning : equality is based on fei and not on payload !"
        if isinstance(other, type(self)):
            return False
        return self._h['fei'] == other.h['fei']

    def __ne__(self, other):
        if (self == other):
            return True
        return False

    def hash(self):
        "Warning : hash is fei's hash."
        return hash(self._h['fei'])

    def lookup(self, key, container_lookup=False):
        """
        Not needed : use
           workitem.fields.toto.address

        For a simple key
           workitem.lookup('toto')
        is equivalent to
           workitem.fields['toto']
        but for a complex key
           workitem.lookup('toto.address')
        is equivalent to
           workitem.fields['toto']['address']
        """
        ref = self._h['fields']
        for k in key.split("."):
            if k not in ref:
                return None
            ref = ref[k]
        return ref

    lf = lookup

    def set_field(self, key, value):
        """Like #lookup allows for nested lookups, #set_field can be used
        to set sub fields directly.

        workitem.set_field('customer.address.city', 'Pleasantville')

        Warning : if the customer and address field and subfield are
        not present or are not hashes, set_field will simply create a
        "customer.address.city" field and set its value to
        "Pleasantville".

        """

        ref = self._h['fields']
        ks = key.split(".")
        last = ks.pop()
        for k in ks:
            if k not in ref:
                ref[k] = {}
            ref = ref[k]
        ref[last] = value

    @property
    def timed_out(self):
        "Shortcut for wi.fields['__timed_out__']"
        return self._h['fields']['__timed_out__']

    # Note this is different to the ruote internal workitem which
    # accesses the private wi.fields.__error__
    # Here we use a specific ruote-amqp value in the workitem.
    @property
    def error(self):
        """
        Reads any previously set value.

        Accesses the ruote-AMQP specific wi.['__error__']
        """
        return self._h['error']

    @error.setter
    def error(self, err):
        """
        Cause a process level error if the Workitem is returned.

        Shortcut for the ruote-AMQP specific wi.['__error__']
        """
        self._h['error'] = err

    # ruote-amqp trace
    @property
    def trace(self):
        """
        Reads any previously set value.

        Accesses the ruote-AMQP specific wi.['trace']
        """
        return self._h['trace']

    @trace.setter
    def trace(self, trace):
        """
        Sets a backtrace.

        Shortcut for the ruote-AMQP specific wi.['trace']
        """
        self._h['trace'] = trace

    @property
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
        try:
            return DictAttrProxy(self._h['fields']['params'])
        except:
            return DictAttrProxy({})

    def dump(self):
        "A useful and consistent dump format"
        return json.dumps(self._h, sort_keys=True, indent=4)
