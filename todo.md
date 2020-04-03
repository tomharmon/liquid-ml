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
