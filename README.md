amqputil
========

Very simple helper to work with rabbit and minimize what you need to know about queues, exchanges, and AMQP in general. Supports: 
work queues (each subscriber gets one message round-robin style)
fanout queues (all subscribers get every message)
