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
  - Name of your Anthology deployment. It is used for descriptive purposes, e.g. in log messages. 
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
  - Transformation of the IDs of the received messages. Useful when only part of the Kafka record key represents the actual ID and this part needs to be extracted. 
  - optional
  - type: single object
- subsection [`transformation`](#domainchannelsmessage-formatstransformation)
  - Transformation of the received message. 
  - optional
  - type: single object
- attribute `log-received-messages`
  - If activated, every received message will be printed to the log. 
  - optional
  - type: boolean
  - default: false

### `domain.channels.message-formats.filtering`
- attribute `filter-paths`
  - The result of the JSONPath-based filtering is what the query returns using the JSONPath on the message. This can be either the complete message (the filter is passed), nothing (filtered out) or a part of the input message. The elements of this array form a filter chain. The result of element n is passed to element n+1 as the input. The result of the last element is the final result. 
  - required
  - type: array of strings (JSONPath)

### `domain.channels.message-formats.id-transformation`
- attribute `pattern`
  - Regex pattern. Exactly one match is expected. If this match consists of several groups, they are concatenated using an underscore. 
  - required
  - type: string (Regex)

### `domain.channels.message-formats.transformation`
- attribute `spec-file`
  - File path to a [JOLT specification file](https://jolt-community.github.io/jolt-community/#specification). 
  - required
  - type: string (File path)

### `domain.relations`
- attribute `type`
  - The type of a relation can be either 'many-to-one' or 'one-to-many'. The relevant direction here is from top to bottom, when viewed from the aggregated structure. 
  - required
  - type: string
- attribute `rel-from`
  - A tuple consisting of a channel and a message format, specifying where the relationship begins.  
  - required
  - type: string
  - example: 'MyChannel/MyMessageFormat'
- attribute `rel-to`
  - A tuple consisting of a channel and a message format, specifying where the relationship ends.  
  - required
  - type: string
  - example: 'MyChannel/MyMessageFormat'
- attribute `ref-from-many-to-one-path`
  - It must be specified how the ID of the 'one' side is to be extracted from the message on the 'many' side. 
  - required
  - type: string (JSONPath)
- attribute `omit-trigger-codomain`
  - This attribute can only be set for relations of type 'many-to-one'. If activated, this ensures that triggering the codomain computation is omitted. That means, changes in the 'rel-to' messages do not trigger the computation of a codomain message. It may be important to use this option if the relation is _highly_ asymmetrical (with a _very large_ number of messages on the ‘many’ side). 
  - optional
  - type: boolean
  - default: false

### `codomain`
- subsection [`deduplication`](#codomaindeduplication)
  - To reduce the amount of codomain message updates, they can be deduplicated in batches. 
  - optional
  - type: single object
- subsection [`filtering`](#codomainfiltering)
  - Filtering of the aggregated messages. It works the same way as for [filtering of received messages](#domainchannelsmessage-formatsfiltering).  
  - optional
  - type: single object
- subsection [`transformation`](#codomaintransformation)
  - Transformation of the aggregated messages. It works the same way as for [transformation of received messages](#domainchannelsmessage-formatstransformation). 
  - optional
  - type: single object
- subsection [`header-propagation`](#codomainheader-propagation)
  - Headers can be added to the aggregated messages before they are sent. 
  - optional
  - type: array of objects
  - each entry specifies one header to add. 
- subsection [`kafka`](#codomainkafka)
  - How and by which Kafka topic data is produced
  - required
  - type: single object
- attribute `log-sent-messages`
  - If activated, every sent message will be printed to the log. 
  - optional
  - type: boolean
  - default: false
- attribute `log-throughput`
  - If activated, a simple per-minute throughput logging of the outbound message stream is activated. 
  - optional
  - type: boolean
  - default: false

### `codomain.deduplication`
- attribute `batch-size`
  - maximum size of the deduplication batch
  - required
  - type: number
- attribute `batching-duration`
  - Maximum time before a batch is deduplicated. 
  - required
  - type: string (duration, e.g. '20 seconds')

### `codomain.filtering`
- attribute `filter-paths`
  - The result of the JSONPath-based filtering is what the query returns using the JSONPath on the message. This can be either the complete message (the filter is passed), nothing (filtered out) or a part of the input message. The elements of this array form a filter chain. The result of element n is passed to element n+1 as the input. The result of the last element is the final result. 
  - required
  - type: array of strings (JSONPath)

### `codomain.transformation`
- attribute `spec-file`
  - File path to a [JOLT specification file](https://jolt-community.github.io/jolt-community/#specification). 
  - required
  - type: string (File path)

### `codomain.header-propagation`
- attribute `type`
  - Method for generating the actual header value. Can be one of 'generate-constant', 'generate-uuid' 'generate-timestamp'. 
  - required
  - type: string
- attribute `name`
  - Name of the header. 
  - required
  - type: string
- attribute `value`
  - Value of the header if its type is 'generate-constant'. 
  - required (for type 'generate-constant')
  - type: string

### `codomain.kafka`
- attribute `cluster`
  - Reference to the Kafka cluster on which the codomain topic is located. 
  - required
  - type: string
  - In [`kafka-clusters`](#kafka-clusters), there must be an element whose `name` matches this value. 
- attribute `topic`
  - Name of the codomain Kafka topic to which the aggregated messages are to be sent 
  - required
  - type: string
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
  - Name of the Kafka cluster, used for reference within the Anthology configuration. 
  - required
  - type: string
- attribute `bootstrap-servers`
  - Comma-separated list of the Kafka cluster's bootstrap server URLs. 
  - required
  - type: string

### `rocks-db`
- attribute `cache-size-mb`
  - Off-heap memory RocksDB is allowed to use for caching in megabytes. 
  - optional
  - type: number
  - default: 0 
- attribute `write-buffer-size-mb`
  - Memory RocksDB is allowed to use for (the sum of all) its write buffers. It reduces the cache size, so it must not be greater than `cache-size-mb`. 
  - optional
  - type: number
  - default: 0 
- attribute `best-efforts-recovery`
  - If activated, RocksDB tries to automatically recover from corrupted data files on startup. 
  - optional
  - type: boolean
  - default: true