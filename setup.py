from distutils.core import setup

setup(name='ruote-amqp',
      version='1.1',
      description='Python Ruote/AMQP client',
      author='David Greaves',
      author_email='david@dgreaves.com',
      url='http://github.com/lbt/ruote-amqp-pyclient',
      packages=['RuoteAMQP',],
      requires=['amqplib', 'json']
     )
