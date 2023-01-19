# KataDB is a simple durable log based KV store. 

!!! THIS IS WORK IN PROGRESS !!!

* KataDB is a backend for KataSearch full text search index. 
* KataDB is also a backend for KataSQL an embeddable AnsiSQL engine for simple storage.  
* KataDB is also a backend for columnar storage KataCol

## Rationale 

I've always wanted to write a database. So here it is. 
This database prioritizes ease of understanding. 

The point of this project first to learn, then to teach. 

I should strive to write code in such a way, that anyone
could read this project during single evening and understand 
like 80% of everything that is going on. 

There should be nothing obscure, complex, hard to understand. 
I want this database to be like a React TODO app. 

Everyone should be able to understand how and why everything
works. 

The idea is, the easier something to understand the easier it 
is to run it in production. 

Hence, the DB will have multiple limitations. The purpose 
of limitations is to keep code simple. 

## How

Will write first iterations in Clojure. When 
design becomes apparent, will rewrite in Go. 


I've played a bit with concepts from DDD book. 

I immensely enjoy log based approaches, so I wanted
to model my database exclusively using queues logs and 
concurrency based on message passing. 

I've built first toy versions using Clojure. Current 
version uses Go because I enjoy writing Go code. It 
is easy to write and result is usually fast. 

We start with a durable message queue that looks much 
like Kafka from API perspective.

Then we use multiple workers that transform,
index and recycle. 

Then we will divide Read and Write. We will have 
unlimited readers and one master writer. 

Readers will be stateless and there should 
be possibility to run multiple readers from 
CLI. 
