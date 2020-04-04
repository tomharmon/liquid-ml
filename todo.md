- Random Forest LUL
- seven degrees of Linus
- application + dataframe layer documentation/update memo
- better distributed dataframe functionality? (possibly make a `DataFrame`
  trait and a `DistributedDataFrame` struct that implements that trait. 
  `DistributedDataFrame` would have a `KVStore` and a list of `Key`s associated
  with all the chunks of the `DistributedDataFrame`. Most of the `Application`
  code for `pmap`/`pfilter` could then be moved to the `DistributedDataFrame`)
- Application level `pfilter`
- Every node has the entire SOR file, possibly add support for distributing
  chunks automatically?
- Testing (esp would be good around chunking to ensure no off by one errors)
- investigate if we should use `tokio::spawn_blocking` anywhere




For the distributed DF

 - Create a stream from sor that can batch SoR parse results
 - redo application to make sense with distributed DFs
    - Only node 1 gets a DDF
    - run only runs on node 1
    - all fields are private, run is the only way to access the application
    - Low Level and High level applications? This breaks the milestone 1 demo,
        so we need a workaround to get direct access to the kv in all nodes
 - constructors for a DDF
 -
