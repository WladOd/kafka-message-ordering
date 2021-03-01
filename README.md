# kafka-message-ordering
This code is based on https://kafka-tutorials.confluent.io/message-ordering/kafka.html
tutorial but with golang **kafka-go** client. The main purpose is to learn **kafka-go** 
client behavior for different groupID and partition which are initialized as command 
line flags. 

## cases
groupID is blank 
: client consumes massages from given partition number only once

groupID is not blank (partition should always be 0)
: only one group client may consume single massage from any partition

ordering of messages is preserved on a per partition basis