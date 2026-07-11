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
  - Configuring certain settings for RocksDB, which is used as embedded database
  - optional
  - type: single object
- attribute `parallelism`
  - The level of concurrency at which the Anthology microservice is to run. 
  - optional
  - type: integer
  - default: 1

### `domain`
- subsection [`channels`](#domainchannels)  
  - Channels are the origin of the data to be aggregated. At present, and for as long as Anthology is limited to Kafka, a channel corresponds to a Kafka topic. 
  - required
  - type: array of objects
  - Must contain at least one element
- subsection [`relations`](#domainrelations)
  - This subsection specifies how the domain data are related to one another, which defines the actual aggregation.  
  - required
  - type: array of objects
  - Must contain at least one element

### `domain.channels`
- attribute `name`
  - A freely selectable name for the channel, which is used to refer to this channel (for example, when defining the domain relations). 
  - required
  - type: string
- subsection [`kafka`](#domainchannelskafka)
  - How and by which Kafka topic data is consumed
  - required
  - type: single object
- subsection [`message-formats`](#domainchannelsmessage-formats)
  - As Anthology can handle different message formats for each channel, this section sets out how each message format should be processed
  - required
  - type: array of objects
  - Must contain at least one element. In total, at least two message formats must be declared in order to perform an aggregation; that is, either two channels, each with one message format, or one channel with two message formats. 

### `domain.channels.kafka`
- attribute `cluster`
- attribute `topic`
- attribute `consumer-group`

### `domain.channels.message-formats`
- attribute `name`
- attribute `recognition-path`
- subsection [`filtering`](#domainchannelsmessage-formatsfiltering)
- subsection [`id-transformation`](#domainchannelsmessage-formatsid-transformation)
- subsection [`transformation`](#domainchannelsmessage-formatstransformation)

### `domain.channels.message-formats.filtering`
- attribute `filter-paths`

### `domain.channels.message-formats.id-transformation`
- attribute `pattern`

### `domain.channels.message-formats.transformation`
- attribute `spec-file`

### `domain.relations`
- attribute `type`
- attribute `rel-from`
- attribute `rel-to`
- attribute `ref-from-many-to-one-path`

### `codomain`
- subsection [`deduplication`](#codomaindeduplication)
- subsection [`filtering`](#codomainfiltering)
- subsection [`transformation`](#codomaintransformation)
- subsection [`header-propagation`](#codomainheader-propagation)
- subsection [`kafka`](#codomainkafka)

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

### `kafka-clusters`
- attribute `name`
- attribute `bootstrap-servers`

### `rocks-db`
- attribute `cache-size-mb`
- attribute `write-buffer-size-mb`