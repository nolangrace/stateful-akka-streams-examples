# Stateful Akka Streams Examples

Welcome to the Stateful Akka Streams Examples project.  This is a scala project build for 
a presentation at Scale by the Bay 2021.

> Project Structure

### Top-level directory layout

    .
    ├── ...
    ├── src                # Source files.  Everything good is in here
    │   └── scala     
    │        ├── entity    # Typed Akka Actor use in example 2 and 3
    │        ├── examples    
    │        │      ├── Example1MapConcat         # Stateful Akka Stream using statefulMapConcat
    │        │      ├── Example2ClusteredActor    # Stateful Akka Stream using Sharded Akka Typed Actors
    │        │      └── Example3KafkaSharding     # Stateful Akka Stream using Akka External Kafka Sharding
    │        ├── helper    # Contains the Kafak helper used to generate sources and sinks
    │        ├── model     # Basic MarketData/ Quotes model for generating data and messages
    │        └── runner    # Contains the code used to start embedded kafka and sample data
    └── ...