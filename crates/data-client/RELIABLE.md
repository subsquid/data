# Reliable data service client

The goal is to combine two data clients into a one, that is more reliable.

## Expected context

* Forks are rare, especially in low-latency and high-frequency chains.

## Streaming algo

We begin by sending two stream requests concurrently and waiting for the first stream, 
that receives the first block (or ends, or reports a fork). 

Let's name the first stream as `A`, the second one as `B` and outline the possible outcomes:

`A` can be in one of the following states:

1. got fork
2. got first block
3. ended
4. errored

and `B`:

1. got headers (finalized head is known)
2. got nothing.

We react as follows.

#### A[1]

In case of fork, we wait up to `500 ms (configurable)` for `B`. 

If `B` reports a fork, we return the result with the highest priority defined as:

1. Highest finalized head
2. Longest chain of the previous blocks
3. `A`.

If `B` does not error and does not report a fork, we delegate to it all further processing.

#### A[2], B[1]

We take `MAX(A.finalized_head, B.finalized_head)` as finalized head of the result 
and invoke the race procedure - `race(A, B, MAX(A.finalized_head, B.finalized_head))`.

#### A[2], B[2]

We take `A.finalized_head` as finalized head of the result 
and invoke the race procedure - `race(A, B, A.finalized_head)`.

#### A[3]

If `A` has ended with no blocks, we return it's finalized head and end the processing.

#### A[4]

In case of error, we delegate all the rest to `B`.

### Race procedure (stream_A, stream_B, reported_finalized_head)

During the race procedure both streams are polled in attempt to grow the resulting chain.

A stream `S` is eligible to submit the next block if either of the following holds:

1. `S.next_block <= S.finalized_head`
2. `S.next_block > reported_finalized_head`
3. `S.next_block.hash == reported_finalized_head.hash`.

When stream is not eligible, it still can be polled, 
but its blocks are ignored until it becomes eligible again.

The stream becomes excluded from the race, when it either errors or produces a fork.

There is no need to be super precise about fork detection, in particular, to hold the whole chain
of returned heads. It is enough to just check the growth attempts.

The race procedure terminates when either:

1. The last stream, that successfully submitted a new block ends
2. No eligible stream is longer available.
