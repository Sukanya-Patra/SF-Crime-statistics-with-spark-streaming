How did changing values on the SparkSession property parameters affect the throughput and latency of the data?

When we increase maxOffsetsPerTrigger and repartition the data to become a multiple of number of cores it positively impacts the throughput and latency.

Q2: What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?

- Increasing the maxOffsetPerTrigger led to increase in number of rows processed per second.
- Increasing number of cores led improved the speed of execution. 
