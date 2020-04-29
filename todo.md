- clean up
- add back function that builds from sor, assuming file is on every node
- `.sor` -> `.csv`
- replace copy+pasted code (due to type checking of primitive types) with macro
- can remove `mpsc` channels and do `futures::select()` on a `vec<future>` in 
  message processing loops
- Testing (esp would be good around chunking to ensure no off by one errors)
- investigate if we should use `tokio::spawn_blocking` anywhere

