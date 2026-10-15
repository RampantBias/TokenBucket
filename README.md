# Overview

This is a simple token bucket exercise written in Golang. The Token Bucket acts as an algorithm typically used in rate limiting. Here, I've implemented a rudimentary form of the algorithm in-memory.

You can read more about this algorithm here: https://en.wikipedia.org/wiki/Token_bucket

## Why this Exists

I just recently completed the Design a Distributed Rate Limiter exercise, and did not natively know about this algorithm. Thus, I felt compelled to write it.

## Assumptions and Decisions

- Fail-fast behavior is implemented on request workers
- Refill worker succeeds as long as it can add tokens, so if a refill rate is specified as 2 tokens/sec, max bucket is 5, and current count is 4, this will succeed but cap tokens at 5.
- Workers aim to finish together within a minute, except if they've already entered a request to update the bucket count and are awaiting the lock.

## Future

This is a closed project left open to the public. No future changes are expected.