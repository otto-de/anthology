# Configure Anthology

## Environment variables

## Command-line arguments

## The main application config file
Most of the configuration is carried out via the main application configuration file. This is a YAML file with the structure described below. 

To give a basic idea of the file structure, a minimal configuration (taken from the end-to-end test) is shown here:
```yaml
name: Anthology-E2E

domain:
  channels:
    - name: Media 
      kafka:
        cluster: test-cluster
        topic: media
        consumer-group: e2e
      message-formats:
      - name: Book
    - name: Authors 
      kafka:
        cluster: test-cluster
        topic: authors
        consumer-group: e2e
      message-formats:
      - name: Author
  relations:
    - type: many-to-one
      rel-from: "Media/Book"
      rel-to: "Authors/Author"
      ref-from-many-to-one-path: "$.authorId"

codomain:
    deduplication:
      batch-size: 1
      batching-duration: 1 second
    kafka:
      cluster: test-cluster
      topic: rich-books

kafka-clusters:
  - name: test-cluster
    bootstrap-servers: localhost:6001
```

### The top level
- attribute `name`
  - Name of your  Anthology deployment. It is used for descriptive purposes, e.g. in log messages. 
  - required
  - type: string
- subsection [`domain`](#domain)
  - In principle, Anthology maps a domain to a codomain. The domain section therefore explains how to configure the data retrieval and how the source data are related to one another. 
  - required
  - type: single object
- subsection [`codomain`](#codomain)
  - The codomain section specifies how data is processed once it has been aggregated. 
  - required
  - type: single object
- subsection [`kafka-clusters`](#kafka-clusters)
  - An Antholoy instance can communicate with multiple Kafka clusters, both to consume and to produce messages. Each of these clusters must be declared here. 
  - required
  - type: array of objects
  - Must contain at least one element
- subsection [`rocks-db`](#rocks-db)
  - Configuring certain settings for RocksDB, which is used as embedded database. 
  - optional
  - type: single object

### `domain`
- subsection [`channels`](#domainchannels)  
  - Channels are the origin of the data to be aggregated. At present, and for as long as Anthology is limited to Kafka, a channel corresponds to a Kafka topic. 
  - required
  - type: array of objects
  - Must contain at least one element. 
- subsection [`relations`](#domainrelations)
  - This subsection specifies how the domain data are related to one another, which defines the actual aggregation.  
  - required
  - type: array of objects
  - Must contain at least one element
- attribute `log-throughput`
  - If activated, a simple per-minute throughput logging of the inbound message streams is activated. 
  - optional
  - type: boolean
  - default: false

### `domain.channels`
- attribute `name`
  - A freely selectable name for the channel, which is used to refer to this channel (for example, when defining the domain relations). 
  - required
  - type: string
  - Must be unique across all domain channels. 
- subsection [`kafka`](#domainchannelskafka)
  - How and by which Kafka topic data is consumed
  - required
  - type: single object
- subsection [`message-formats`](#domainchannelsmessage-formats)
  - As Anthology can handle different message formats for each channel, this section sets out how each message format should be processed. 
  - required
  - type: array of objects
  - Must contain at least one element. In total, at least two message formats must be declared in order to perform an aggregation; that is, either two channels, each with one message format, or one channel with two message formats. 

### `domain.channels.kafka`
- attribute `cluster`
  - Reference to the Kafka cluster on which the channel is located. 
  - required
  - type: string
  - In [`kafka-clusters`](#kafka-clusters), there must be an element whose `name` matches this value. 
- attribute `topic`
  - Name of the Kafka topic that implements this channel. 
  - required
  - type: string
- attribute `consumer-group`
  - Name of the consumer group that is to consume this topic. 
  - required
  - type: string
- subsection [`additional-consumer-properties`](#domainchannelskafkaadditional-consumer-properties)
  - Additional attributes that are passed directly to the Kafka consumer. 
  - optional
  - type: array of objects

### `domain.channels.kafka.additional-consumer-properties`
- attribute `name`
  - Name of the Kafka consumer property. See [Kafka reference](https://kafka.apache.org/41/configuration/consumer-configs). 
  - required
  - type: string
- attribute `value`
  - Value of the Kafka consumer property. 
  - required
  - type: string

### `domain.channels.message-formats`
- attribute `name`
  - A freely selectable name for the message format, which is used to refer to this message format (for example, when defining the domain relations). 
  - required
  - type: string
  - Must be unique across all message formats of the same domain channel. 
- attribute `recognition-path`
  - JSONPath which is used to determine whether a received message corresponds to this message format. A message is recognised as belonging to this format if the query on the message using the JSONPath returns at least one result. 
  - optional
  - type: string (JSONPath)
  - If there is more than one message format in this channel, a recognition path must be specified. 
  - Unrecognised messages are silently ignored. 
- attribute `id-extraction-path`
  - By default, the Kafka record key (which may need to be [transformed](#domainchannelsmessage-formatsid-transformation)) is used as the message ID. However, if it is to be extracted from the _message content_, the relevant JSONPath can be specified here. 
  - optional
  - type: string (JSONPath)
- subsection [`filtering`](#domainchannelsmessage-formatsfiltering)
  - Filtering of the received messages. It can be used to filter out or allow the entire message to pass through, or to extract a part of the message. 
  - optional
  - type: single object
- subsection [`id-transformation`](#domainchannelsmessage-formatsid-transformation)
- subsection [`transformation`](#domainchannelsmessage-formatstransformation)
- attribute `log-received-messages`
  - If activated, every received message will be printed to the log. 
  - optional
  - type: boolean
  - default: false

### `domain.channels.message-formats.filtering`
- attribute `filter-paths`
  - The result of the JSONPath-based filtering is what the query returns using the JSONPath on the message. This can be either the complete message (the filter is passed), nothing (filtered out) or a part of the input message. The elements of this array form a filter chain. The result of element n is passed to element n+1 as the input. The result of the last element is the final result. 
  - required
  - type: array of string (JSONNode)

### `domain.channels.message-formats.id-transformation`
- attribute `pattern`

### `domain.channels.message-formats.transformation`
- attribute `spec-file`

### `domain.relations`
- attribute `type`
- attribute `rel-from`
- attribute `rel-to`
- attribute `ref-from-many-to-one-path`
- attribute `omit-trigger-codomain`
  - This attribute can only be set for relations of type 'many-to-one'. If activated, this ensures that triggering the codomain computation is omitted. That means, changes in the 'rel-to' messages do not trigger the computation of a codomain message. It may be important to use this option if the relation is _highly_ asymmetrical (with a _very large_ number of messages on the ‘many’ side). 
  - optional
  - type: boolean
  - default: false

### `codomain`
- subsection [`deduplication`](#codomaindeduplication)
- subsection [`filtering`](#codomainfiltering)
- subsection [`transformation`](#codomaintransformation)
- subsection [`header-propagation`](#codomainheader-propagation)
- subsection [`kafka`](#codomainkafka)
- attribute `log-sent-messages`
- attribute `log-throughput`
  - If activated, a simple per-minute throughput logging of the outbound message stream is activated. 
  - optional
  - type: boolean
  - default: false

### `codomain.deduplication`
- attribute `batch-size`
- attribute `batching-duration`

### `codomain.filtering`
- attribute `filter-paths`

### `codomain.transformation`
- attribute `spec-file`

### `codomain.header-propagation`
- attribute `type`
- attribute `name`
- attribute `value`

### `codomain.kafka`
- attribute `cluster`
- attribute `topic`
- subsection [`additional-producer-properties`](#codomainkafkaadditional-producer-properties)
  - Additional attributes that are passed directly to the Kafka producer. 
  - optional
  - type: array of objects

### `codomain.kafka.additional-producer-properties`
- attribute `name`
  - Name of the Kafka producer property. See [Kafka reference](https://kafka.apache.org/41/configuration/producer-configs/). 
  - required
  - type: string
- attribute `value`
  - Value of the Kafka producer property. 
  - required
  - type: string

### `kafka-clusters`
- attribute `name`
- attribute `bootstrap-servers`

### `rocks-db`
- attribute `cache-size-mb`
- attribute `write-buffer-size-mb`
- attribute `best-efforts-recovery`