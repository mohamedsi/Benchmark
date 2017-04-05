# Benchmark

`./gradlew assemble`

command line parameters:

Implmentation		  SolaceClient
NumProducers		  2
NumConsumers		  3
NumbOfEndpoints		1
EndpointType		  Topic
DeliveryMode		  NON_PERSISTENT
PayLoadSize(Kb)		    1
Duration		      100000000
NumOfMessages		  100000000
BrokerIP		      10.9.220.40
BrokerPort		    55555
Producer / Consumer Producer


Using the native solace driver: SolaceClient 2 3 1 Topic NON_PERSISTENT 1 100000000 100000000 10.9.220.40 55555 Producer

Using the jms implmentation: Solace 2 3 1 Topic NON_PERSISTENT 1 100000000 100000000 10.9.220.40 55555 Producer
