# Configure Anthology

## The main application config file
Most of the configuration is carried out via the main application configuration file. This is a YAML file with the structure described below. 

### The top level
- attribute `name`
  - required
  - type: string
  - Name of your  Anthology deployment. It is used for descriptive purposes, e.g. in log messages. 
- attribute `parallelism`
  - optional
  - type: integer
  - default: 1
  - The level of concurrency at which the Anthology microservice is to run. 
- subsection [`domain`](#domain)
  - required
  - type: single object
- subsection [`codomain`](#codomain)
  - required
  - type: single object
- subsection [`kafka-clusters`](#kafka-clusters)
  - required
  - type: single object

### `domain`
In principle, Anthology maps a domain to a codomain. The domain section therefore explains how to configure the data retrieval and how the source data are related to one another. 
- subsection [`channels`](#domainchannels)
  - required
  - type: array of objects
  - Must contain at least one element
- subsection [`relations`](#domainrelations)
  - required
  - type: array of objects
  - Must contain at least one element

### `domain.channels`
- attribute `name`
- subsection [`kafka`](#domainchannelskafka)
- subsection [`message-formats`](#domainchannelsmessage-formats)

### `domain.channels.kafka`

### `domain.channels.message-formats`

### `domain.relations`

### `codomain`

### `kafka-clusters`
