Topologies:
Sub-topology: 0
Source: KSTREAM-SOURCE-0000000000 (topics: [pageviews])
--> KSTREAM-KEY-SELECT-0000000001
Processor: KSTREAM-KEY-SELECT-0000000001 (stores: [])
--> KSTREAM-FILTER-0000000006
<-- KSTREAM-SOURCE-0000000000
Processor: KSTREAM-FILTER-0000000006 (stores: [])
--> KSTREAM-SINK-0000000005
<-- KSTREAM-KEY-SELECT-0000000001
Sink: KSTREAM-SINK-0000000005 (topic: KSTREAM-KEY-SELECT-0000000001-repartition)
<-- KSTREAM-FILTER-0000000006

Sub-topology: 1
Source: KSTREAM-SOURCE-0000000007 (topics: [KSTREAM-KEY-SELECT-0000000001-repartition])
--> KSTREAM-LEFTJOIN-0000000008
Processor: KSTREAM-LEFTJOIN-0000000008 (stores: [users-STATE-STORE-0000000002])
--> KSTREAM-MAP-0000000009
<-- KSTREAM-SOURCE-0000000007
Processor: KSTREAM-MAP-0000000009 (stores: [])
--> KSTREAM-FILTER-0000000013
<-- KSTREAM-LEFTJOIN-0000000008
Processor: KSTREAM-FILTER-0000000013 (stores: [])
--> KSTREAM-SINK-0000000012
<-- KSTREAM-MAP-0000000009
Source: KSTREAM-SOURCE-0000000003 (topics: [users])
--> KTABLE-SOURCE-0000000004
Sink: KSTREAM-SINK-0000000012 (topic: KSTREAM-AGGREGATE-STATE-STORE-0000000010-repartition)
<-- KSTREAM-FILTER-0000000013
Processor: KTABLE-SOURCE-0000000004 (stores: [users-STATE-STORE-0000000002])
--> none
<-- KSTREAM-SOURCE-0000000003

Sub-topology: 2
Source: KSTREAM-SOURCE-0000000014 (topics: [KSTREAM-AGGREGATE-STATE-STORE-0000000010-repartition])
--> KSTREAM-AGGREGATE-0000000011
Processor: KSTREAM-AGGREGATE-0000000011 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000010])
--> KTABLE-TOSTREAM-0000000015
<-- KSTREAM-SOURCE-0000000014
Processor: KTABLE-TOSTREAM-0000000015 (stores: [])
--> KSTREAM-MAPVALUES-0000000016
<-- KSTREAM-AGGREGATE-0000000011
Processor: KSTREAM-MAPVALUES-0000000016 (stores: [])
--> KSTREAM-SINK-0000000017
<-- KTABLE-TOSTREAM-0000000015
Sink: KSTREAM-SINK-0000000017 (topic: streams-pageviewstats-typed-output)
<-- KSTREAM-MAPVALUES-0000000016