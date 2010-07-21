#!/usr/bin/python
import RuoteAMQP

# Specify a process definition
process = """
        Ruote.process_definition :name => 'test' do
          sequence do
            developer
            builder
          end
        end
      """
fields = {
    "version" : "0.5.12"
    }

# Specify the amqp server
launcher = RuoteAMQP.Launcher(amqp_host="amqpvm", amqp_user="ruote",
                              amqp_pass="ruote", amqp_vhost="ruote-test")

launcher.launch(process, fields)
