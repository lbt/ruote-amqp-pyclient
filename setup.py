from distutils.core import setup
from setuptools import setup

setup(name='route-amqp-pyclient',
      version='1.0',
      description='Python Ruote/AMQP client',
      author='David Greaves',
      author_email='david@dgreaves.com',
      url='http://github.com/lbt/ruote-amqp-pyclient',
      packages=['RuoteAMQP',],
      requires=['amqplib', 'json']
     )
