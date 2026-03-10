This test validates that when nullable columns are missing intirely from the parquet file, such as:

- eventInboxRoot
- consensusParametersVersion
- stateTransitionBytecodeVersion
- messageOutboxRoot

The output will include these columnds with null values
