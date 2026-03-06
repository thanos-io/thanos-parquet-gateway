# Scalable converters

Currently, the design of the converter is very simple - you get some files in
and some files out. It totally makes sense since we are still in the infancy of
this project. However, as it is getting adopted more and more, there is a need
to do the conversion more quickly. For that, we should be able to spread the load
across multiple servers.

Indeed, it is possible right now to use external labels to separate converters
and make a converter for each stream separately but that involves either some
automation that each user will need to do or, even worse, it is a manual job.

Let’s say we would need to convert 800TiB of data with a 10Gbps network card.
According to this napkin math, it would take 177 hours just to download all of
that data.

Another thing is that doing the planning in each service is wasteful - one has
to load all meta files, filter out what is not needed (if using filtering based
on external labels, for example), and do the planning step. For a long time
we've been talking about having a client/server architecture inside of Thanos
Compactor. Since we are still in the beginning, it seems like a good idea to
separate these two concerns since the start.

This also prevents doing changes en masse. Let’s say Parquet evolves and we want
to enable some flashy new features on all the data. Or if we want to experiment
with setting a bigger page size on a subset of block streams. Having a
client/server architecture place from the get go would allow us to easily do
some modifications again.

## Separation

Internally, let's separate the planner and the converter into two separate
subsystems. Keep the same ability to run the planner and converter in one
process if the user so requires. Hence, let's add new and optional command line
flags to control the behaviour if separation is requested.

This also begs the question: how to pass the "work items" from the planner to
the converter? For that, we will need to have some kind of queue from which the
converters will consume.

A very traditional way of doing so would be to use a "heavy-weight" system like
Kafka or RabbitMQ. However, we would like to avoid adding such big dependencies
as per the development philosophy of Thanos - keep everything as simple as
possible.

Also, note that all conversions (if the parameters do not change) are idempotent
so we can do the same conversion many times and it does not impact the system
negatively except for extraneous remote object storage operations that could be
avoided.

## Implementation

Inside of Cortex, they convert block and put data inside of the same directory
so it is possible to reuse the same sharding mechanism i.e. the same that the
Compactor component has. Inside of this project, we decided to use the day of
the data inside of a block as a top-level key so it doesn't work for us. Cortex
also supports rings everywhere so a user of Cortex already needs to have some
extra system which could maintain the state of the ring.

There are two separate concerns that we need to take care of: how to pass down
the units of work to converters and how the planner component could know whether
progress is being done, and when to pass the units of work again.

- For passing down the unit of work this part can be abstracted and any modicum
  can be used - Redis, memcached, the same remote object storage, etc., in
  general any key/value storage. All such systems are called "storage" from here
  on. The "queue" can look like the following objects:

```raw
/converter/<converter queue's name in the config file>/<work ulid>_0.json
/converter/<converter queue's name in the config file>/<work ulid>_1.json
/converter/<converter queue's name in the config file>/<work ulid>_2.json
...
```

We will need to identify each planner and converter with some string so it will
also reflect in the object path. Having this name will allow users to run
multiple converter/planner pairs on the same storage.

On each planner loop, generate a ULID that will uniquely identify that work
stream.

At the moment, the only work that can be done is "conversion" so it will contain
data about which blocks to download and convert.

After uploading all the work, the planner will ping through gRPC the workers
that need work is available and that they need to reset their internal history.

After that, the workers will start downloading the files and doing the work.
Deleting a object will "consume" it and act as a signal that the worker for
which the deletion succeeded, can do the work.

The workers must through gRPC expose what they are doing (or what has been
done). This will act as a guardrail and also it will allow us to have nice logs
inside of the planner.

Periodically, the planner shall check if the work has been completed and whether
it can try to do the planning again.

Let's use DNS to discover all workers.

## States

1. At the start, the worker has no history and does nothing.
2. If it receives a ping through gRPC from the planner, it shall mark that it in
   its state and start trying to do work. In other words, it should list objects
   and try to consume them.
3. Periodiocally check state to see if no work is queued anymore. If not then do
   the planning step again.
4. What if some file disappears while consuming is happening? It's not an issue
   because we will just do the planning again. We can repeat all operations
   because conversion is idempotent.
5. If a worker dies in the middle then it's not an issue and we can always retry
   again due to idempotency.

The planner should maintain state of what jobs have been previously queued so
that it could know if some job is being done over and over again to inform the
operator about this.

In practice, we could move the keeping of the state completely to object
storage but that would mean extra writes and reads, and doing it
synchronously through gRPC means that users are more protected against
accidental errors.

We should use `If-None-Match` (or equivalent) to ensure that objects are
only written once as a defensive measure.
