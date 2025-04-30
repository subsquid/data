use crate::types::{DataEvent, DataSource};
use anyhow::Context;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{FutureExt, Stream, StreamExt};
use sqd_data_client::{BlockStreamRequest, BlockStreamResponse, DataClient};
use sqd_primitives::{Block, BlockNumber, BlockRef};
use std::future::Future;
use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;
use tokio::time::Sleep;
use tracing::{error, warn};


struct Endpoint<C: DataClient> {
    client: C,
    state: EndpointState<C::Block>,
    error_counter: usize,
    last_committed_block: Option<BlockNumber>
}


enum EndpointState<B> {
    Ready,
    Req {
        req: BlockStreamRequest,
        future: BoxFuture<'static, anyhow::Result<BlockStreamResponse<B>>>
    },
    Stream {
        finalized_head: BlockNumber,
        blocks: BoxStream<'static, anyhow::Result<B>>
    },
    Fork {
        req: BlockStreamRequest,
        prev_blocks: Vec<BlockRef>
    },
    Backoff(Pin<Box<Sleep>>)
}


pub struct StandardDataSource<C: DataClient, F> {
    endpoints: Vec<Endpoint<C>>,
    state: DataSourceState<F>
}


struct DataSourceState<F> {
    parse: F,
    finalized_head: Option<BlockRef>,
    position: BlockStreamRequest,
    position_is_canonical: bool,
    max_seen_finalized_block: BlockNumber,
    forks: usize
}


impl<F> DataSourceState<F> {
    fn poll_endpoint<B, C>(
        &mut self,
        ep: &mut Endpoint<C>,
        cx: &mut std::task::Context<'_>
    ) -> Poll<DataEvent<B>>
    where
        B: Block,
        C: DataClient,
        F: Fn(C::Block) -> anyhow::Result<B>
    {
        loop {
            match &mut ep.state {
                EndpointState::Ready => {
                    ep.state = EndpointState::Req {
                        req: self.position.clone(),
                        future: ep.client.stream(self.position.clone())
                    }
                },
                EndpointState::Req { req, future } => {
                    match future.poll_unpin(cx) {
                        Poll::Ready(Ok(BlockStreamResponse::Stream {
                           finalized_head,
                           blocks
                        })) => {
                            let finalized_head_updated = self.on_new_finalized_head(
                                finalized_head.as_ref()
                            );
                            
                            ep.error_counter = 0;
                            ep.state = EndpointState::Stream {
                                finalized_head: finalized_head.as_ref().map_or(0, |b| b.number),
                                blocks
                            };

                            if finalized_head_updated {
                                return Poll::Ready(DataEvent::FinalizedHead(
                                    finalized_head.unwrap()
                                ))
                            }
                        },
                        Poll::Ready(Ok(BlockStreamResponse::Fork(prev_blocks))) => {
                            ep.error_counter = 0;
                            ep.state = EndpointState::Fork {
                                req: req.clone(),
                                prev_blocks
                            };
                        },
                        Poll::Ready(Err(err)) => ep.on_error(err),
                        Poll::Pending => return Poll::Pending
                    }
                },
                EndpointState::Stream {
                    finalized_head,
                    blocks
                } => {
                    match blocks.poll_next_unpin(cx) {
                        Poll::Ready(None) => {
                            ep.error_counter = 0;
                            ep.state = EndpointState::Ready;
                            let prev_block = self.position.first_block.saturating_sub(1);
                            if prev_block >= self.max_seen_finalized_block && ep.last_committed_block == Some(prev_block)  {
                                return Poll::Ready(DataEvent::MaybeOnHead)
                            }
                        },
                        Poll::Ready(Some(Ok(new_block))) => {
                            match (self.parse)(new_block).context("failed to parse a block") {
                                Ok(block) => {
                                    ep.error_counter = 0;
                                    if block.number() >= self.position.first_block {
                                        let is_final = *finalized_head >= block.number();
                                        if self.accept_new_block(&block, is_final) {
                                            ep.last_committed_block = Some(block.number());
                                            return Poll::Ready(DataEvent::Block {
                                                block,
                                                is_final
                                            })
                                        } else {
                                            ep.state = EndpointState::Ready;
                                        }
                                    }
                                },
                                Err(err) => ep.on_error(err),
                            }
                        },
                        Poll::Ready(Some(Err(err))) => ep.on_error(err),
                        Poll::Pending => return Poll::Pending
                    }
                },
                EndpointState::Fork { req, .. } => {
                    if req == &self.position {
                        self.forks += 1;
                        return Poll::Pending
                    } else {
                        ep.state = EndpointState::Ready;
                    }
                },
                EndpointState::Backoff(sleep) => {
                    match sleep.as_mut().poll(cx) {
                        Poll::Ready(_) => ep.state = EndpointState::Ready,
                        Poll::Pending => return Poll::Pending
                    }
                }
            }
        }
    }

    fn accept_new_block(&mut self, block: &impl Block, is_final: bool) -> bool {
        assert!(self.position.first_block <= block.number());

        if let Some(parent_hash) = self.position.parent_block_hash.as_mut() {
            if block.parent_hash() != parent_hash {
                return false
            }
            parent_hash.clear();
            parent_hash.push_str(block.hash());
        } else {
            self.position.parent_block_hash = Some(block.hash().to_string());
        }
        self.position.first_block = block.number() + 1;
        self.position_is_canonical = true;

        if is_final {
            set_head(&mut self.finalized_head, block.number(), block.hash());
        }

        true
    }

    fn on_new_finalized_head(&mut self, new_head: Option<&BlockRef>) -> bool {
        let Some(new_head) = new_head else {
            return false
        };

        self.max_seen_finalized_block = std::cmp::max(
            self.max_seen_finalized_block,
            new_head.number
        );

        if self.position.first_block == 0 {
            return false
        }

        let Some(current_parent_hash) = self.position.parent_block_hash.as_ref() else {
            return false
        };

        let is_behind = self.finalized_head
            .as_ref()
            .map_or(false, |c| c.number >= new_head.number);

        if is_behind {
            return false
        }

        let mut new_number = new_head.number;
        let mut new_hash = &new_head.hash;

        if new_head.number >= self.position.first_block {
            if !self.position_is_canonical {
                return false
            }
            new_number = self.position.first_block - 1;
            new_hash = current_parent_hash;
        }

        set_head(&mut self.finalized_head, new_number, new_hash);

        true
    }
}


fn set_head(head: &mut Option<BlockRef>, number: BlockNumber, hash: &str) {
    if let Some(current) = head.as_mut() {
        current.number = number;
        current.hash.clear();
        current.hash.push_str(hash);
    } else {
        *head = Some(BlockRef {
            number,
            hash: hash.to_string()
        })
    }
}


impl<C: DataClient> Endpoint<C> {
    fn on_error(&mut self, error: anyhow::Error) {
        let backoff = [0, 100, 200, 500, 1000, 2000, 5000, 10000];
        let pause = backoff[std::cmp::min(self.error_counter, backoff.len() - 1)];
        if pause > 0 {
            warn!(
                error =? error,
                data_source =? self.client,
                "data ingestion error, will disable the data source for {} ms",
                pause
            )
        } else {
            warn!(
                error =? error,
                data_source =? self.client,
                "data ingestion error",
            )
        }
        self.state = if pause > 0 {
            let sleep = tokio::time::sleep(Duration::from_millis(pause));
            EndpointState::Backoff(Box::pin(sleep))
        } else {
            EndpointState::Ready
        };
        self.error_counter += 1;
    }
}


impl<B, C, F> StandardDataSource<C, F>
where
    B: Block,
    C: DataClient,
    F: Fn(C::Block) -> anyhow::Result<B>
{
    pub fn new(clients: Vec<C>, parse: F) -> Self {
        let endpoints = clients.into_iter().map(|client| {
            Endpoint {
                client,
                error_counter: 0,
                state: EndpointState::Ready,
                last_committed_block: None
            }
        }).collect();

        let state = DataSourceState {
            parse,
            finalized_head: None,
            position: BlockStreamRequest {
                first_block: 0,
                parent_block_hash: None
            },
            position_is_canonical: false,
            max_seen_finalized_block: 0,
            forks: 0
        };

        Self {
            endpoints,
            state
        }
    }

    fn poll_next_event(&mut self, cx: &mut std::task::Context<'_>) -> Poll<DataEvent<B>> {
        self.state.forks = 0;

        for ep in self.endpoints.iter_mut() {
            let event = self.state.poll_endpoint(ep, cx);
            if event.is_ready() {
                return event
            }
        }

        if self.state.forks > 0 {
            if self.state.forks > self.endpoints.len() / 2 || self.state.forks == self.active_endpoints() {
                return Poll::Ready(DataEvent::Fork(self.extract_fork()))
            }
        }

        Poll::Pending
    }

    fn active_endpoints(&self) -> usize {
        self.endpoints.iter().map(|ep| {
            if let EndpointState::Backoff(_) = &ep.state {
                0
            } else {
                1
            }
        }).sum()
    }

    fn extract_fork(&mut self) -> Vec<BlockRef> {
        let mut chain = Vec::new();
        for ep in self.endpoints.iter_mut() {
            match std::mem::replace(&mut ep.state, EndpointState::Ready) {
                EndpointState::Fork { prev_blocks, .. } => {
                    if prev_blocks.len() > chain.len() {
                        chain = prev_blocks
                    }
                }
                _ => {}
            }
        }
        assert!(!chain.is_empty());
        chain
    }
}


impl<B, C, F> Stream for StandardDataSource<C, F>
where
    B: Block,
    C: DataClient,
    F: Fn(C::Block) -> anyhow::Result<B> + Unpin
{
    type Item = DataEvent<B>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().poll_next_event(cx).map(Some)
    }
}


impl<B, C, F> DataSource for StandardDataSource<C, F>
where
    B: Block,
    C: DataClient,
    F: Fn(C::Block) -> anyhow::Result<B> + Unpin
{
    type Block = B;

    fn set_position(&mut self, next_block: BlockNumber, parent_block_hash: Option<String>) {
        self.state.position.first_block = next_block;
        self.state.position.parent_block_hash = parent_block_hash;
        self.state.position_is_canonical = false;
        self.state.finalized_head = None;
        for ep in self.endpoints.iter_mut() {
            ep.state = EndpointState::Ready;
            ep.last_committed_block = None;
        }
    }
}