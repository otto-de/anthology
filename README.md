# _Anthology_

## Status

![OSS Lifecycle](https://img.shields.io/osslifecycle?file_url=https%3A%2F%2Fraw.githubusercontent.com%2Fotto-de%2Fanthology%2Fmain%2FOSSMETADATA)

## About

Anthology is an implementation of the [Aggregator Pattern](https://www.enterpriseintegrationpatterns.com/patterns/messaging/Aggregator.html) as a standards-based microservice. It is designed to make the task of aggregating data from multiple sources within a distributed system landscape as straightforward and effortless as possible. 

```mermaid
flowchart LR
    DTA@{ shape: h-cyl, label: "Domain Topic A" }
    DTB@{ shape: h-cyl, label: "Domain Topic B" }
    DTC@{ shape: h-cyl, label: "Domain Topic C" }
    DTA --> DA
    DTB --> DB
    DTC --> DC
    subgraph SERVICE["Anthology Service Instance"]
    DA(Domain A)
    DB(Domain B)
    DC(Domain C)
    FDA(Domain A Filtering)
    FDB(Domain B Filtering)
    FDC(Domain C Filtering)
    TDA(Domain A Transformation)
    TDB(Domain B Transformation)
    TDC(Domain C Transformation)
    AGG(Aggregation)
    FCO(Codomain Filtering)
    TCO(Codomain Transformation)
    PUB(Codomain)
    DA --> FDA
    DB --> FDB
    DC --> FDC
    FDA --> TDA
    FDB --> TDB
    FDC --> TDC
    TDA --> AGG
    TDB --> AGG
    TDC --> AGG
    AGG --> FCO
    FCO --> TCO
    TCO --> PUB
    end
    TA@{ shape: h-cyl, label: "Codomain Topic" }
    STR@{ shape: cyl, label: "State Store" }
    SERVICE --> STR
    PUB --> TA
```
