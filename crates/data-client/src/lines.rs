use crate::client::is_retryable;
use crate::stream::BodyStreamBox;
use bytes::{Buf, Bytes, BytesMut};
use futures_core::Stream;
use std::pin::Pin;
use std::task::Poll;


pub struct LineStream {
    inner: Option<BodyStreamBox>,
    line: BytesMut,
    unchecked_pos: usize,
}


impl LineStream {
    pub fn new(inner: BodyStreamBox) -> Self {
        Self {
            inner: Some(inner),
            line: BytesMut::new(),
            unchecked_pos: 0
        }
    }
    
    fn check_line(&mut self) -> Option<Bytes> {
        if let Some(pos) = self.line.as_ref()[self.unchecked_pos..].iter().position(|b| *b == b'\n') {
            let line = self.line.split_to(pos).freeze();
            self.line.advance(if self.line.get(2).copied() == Some(b'\r') {
                2
            } else {
                1
            });
            self.unchecked_pos = 0;
            Some(line)
        } else {
            self.unchecked_pos = self.line.len();
            None
        }
    }
    
    fn take_final_line(&mut self) -> Option<Bytes> {
        let line = std::mem::take(&mut self.line);
        if line.is_empty() {
            None
        } else {
            Some(line.freeze())
        }
    }
}


impl Stream for LineStream {
    type Item = reqwest::Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        
        loop {
            if let Some(line) = this.check_line() {
                return Poll::Ready(Some(Ok(line)))
            }
            
            if let Some(inner) = this.inner.as_mut() {
                match Pin::new(inner).poll_next(cx) {
                    Poll::Ready(None) => {
                        this.inner = None;
                        return Poll::Ready(
                            Ok(this.take_final_line()).transpose()
                        )
                    },
                    Poll::Ready(Some(Ok(bytes))) => {
                        this.line.extend_from_slice(&bytes)
                    },
                    Poll::Ready(Some(Err(err))) => {
                        this.inner = None;
                        this.line = BytesMut::new();
                        return if is_retryable(&err) {
                            Poll::Ready(None)
                        } else {
                            Poll::Ready(Some(Err(err)))
                        }
                    },
                    Poll::Pending => return Poll::Pending,
                }   
            } else {
                return Poll::Ready(None)
            }
        }
    }
}