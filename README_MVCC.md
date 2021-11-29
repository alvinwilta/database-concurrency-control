## Multiversion Timestamp Ordering Concurrency Control

Multiversion
The basic idea in this scheme is to assign transactions timestamps when they are started, which are used to order these transactions. If two transactions access data items in an order that is inconsistent with their time stamps, then one of them is aborted.
