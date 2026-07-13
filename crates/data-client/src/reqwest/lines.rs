use std::{pin::Pin, task::Poll};

use bytes::{Buf, Bytes, BytesMut};
use futures::Stream;

pub struct LineStream<Body> {
    inner: Option<Body>,
    line: BytesMut,
    unchecked_pos: usize
}

impl<Body> LineStream<Body> {
    pub fn new(body: Body) -> Self {
        Self {
            inner: Some(body),
            line: BytesMut::new(),
            unchecked_pos: 0
        }
    }

    fn check_line(&mut self) -> Option<Bytes> {
        if let Some(pos) = self.line.as_ref()[self.unchecked_pos..]
            .iter()
            .position(|b| *b == b'\n')
        {
            let line = self.line.split_to(self.unchecked_pos + pos).freeze();
            self.line
                .advance(if self.line.get(1).copied() == Some(b'\r') { 2 } else { 1 });
            self.unchecked_pos = 0;
            Some(line)
        } else {
            self.unchecked_pos = self.line.len();
            None
        }
    }

    fn take_final_line(&mut self) -> Option<Bytes> {
        // The buffer is emptied here, so the scan position must go with it: a body whose last
        // line is unterminated would otherwise leave `unchecked_pos` past the end of an empty
        // buffer, and the next poll would index out of bounds.
        self.unchecked_pos = 0;
        let line = std::mem::take(&mut self.line);
        if line.is_empty() {
            None
        } else {
            Some(line.freeze())
        }
    }
}

impl<Body, E> Stream for LineStream<Body>
where
    Body: Stream<Item = Result<Bytes, E>> + Unpin,
    E: Into<anyhow::Error>
{
    type Item = anyhow::Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(line) = self.check_line() {
                return Poll::Ready(Some(Ok(line)));
            }

            let Some(inner) = self.inner.as_mut() else {
                return Poll::Ready(None);
            };

            match Pin::new(inner).poll_next(cx) {
                Poll::Ready(None) => {
                    self.inner = None;
                    return Poll::Ready(Ok(self.take_final_line()).transpose());
                }
                Poll::Ready(Some(Ok(bytes))) => self.line.extend_from_slice(&bytes),
                Poll::Ready(Some(Err(err))) => {
                    self.inner = None;
                    self.line = BytesMut::new();
                    self.unchecked_pos = 0;
                    return Poll::Ready(Some(Err(err.into())));
                }
                Poll::Pending => return Poll::Pending
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::{executor::block_on, stream, StreamExt};

    use super::*;

    fn lines(chunks: &[&'static str]) -> Vec<String> {
        let body = stream::iter(
            chunks
                .iter()
                .map(|chunk| Ok::<_, std::io::Error>(Bytes::from_static(chunk.as_bytes())))
                .collect::<Vec<_>>()
        );
        block_on(
            LineStream::new(body)
                .map(|line| String::from_utf8(line.unwrap().to_vec()).unwrap())
                .collect()
        )
    }

    #[test]
    fn splits_lines_across_chunk_boundaries() {
        assert_eq!(lines(&["{\"a\":1}\n{\"a\"", ":2}\n"]), ["{\"a\":1}", "{\"a\":2}"]);
    }

    #[test]
    fn an_unterminated_final_line_is_yielded_and_the_stream_ends() {
        // The stream is polled once more after the final line: with the scan position left
        // pointing past the emptied buffer, that poll used to index out of bounds and panic,
        // taking the ingest task with it.
        assert_eq!(lines(&["{\"a\":1}\n{\"a\":2}"]), ["{\"a\":1}", "{\"a\":2}"]);
    }

    #[test]
    fn an_empty_body_yields_nothing() {
        assert!(lines(&[]).is_empty());
        assert!(lines(&[""]).is_empty());
    }
}
