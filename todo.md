- Random Forest LUL
- application + dataframe layer documentation/update memo
- add back function that builds from sor and assumes file is on every node
- Testing (esp would be good around chunking to ensure no off by one errors)
- investigate if we should use `tokio::spawn_blocking` anywhere
- can we get rid of `RwLock` on `KVStore` completely? we never use `.write`
  on the `RwLock`, and all the fields of the `KVStore` that we mutate has a 
  `RwLock` or `Mutex`


