use std::time::{Instant, Duration};
use std::num::NonZeroUsize;


#[derive(Clone, Debug)]
struct ProgressUnit {
    value: u64,
    time: Instant,
}


pub struct Progress {
    window: Vec<ProgressUnit>,
    tail: usize,
    size: usize,
    granularity: Duration,
    has_news: bool,
}

impl Progress {
    pub fn new(window_size: NonZeroUsize, window_granularity: Duration) -> Self {
        assert!(!window_granularity.is_zero());
        Self {
            window: Vec::with_capacity(window_size.get() + 1),
            tail: 0,
            size: window_size.get() + 1,
            granularity: window_granularity,
            has_news: false,
        }
    }

    pub fn set_current_value(&mut self, value: u64) {
        let time = Instant::now();

        if self.window.is_empty() {
            self.window.push(ProgressUnit { value, time });
            self.tail = 1;
            return;
        }

        let last = self.last();
        let value = value.max(last.value);

        if self.window.len() > 1 && time <= last.time + self.granularity {
            self.last_mut().value = value;
        } else {
            let unit = ProgressUnit { value, time };
            if self.tail < self.window.len() {
                self.window[self.tail] = unit;
            } else {
                self.window.push(unit);
            }
            self.tail = (self.tail + 1) % self.size;
        }

        self.has_news = true;
    }

    pub fn get_current_value(&self) -> u64 {
        assert!(!self.window.is_empty(), "no current value available");
        self.last().value
    }

    pub fn has_news(&self) -> bool {
        self.has_news
    }

    pub fn speed(&mut self) -> f64 {
        self.has_news = false;

        if self.window.len() < 2 {
            return 0.0;
        }

        let beg = if self.window.len() < self.size {
            &self.window[0]
        } else {
            &self.window[self.tail]
        };

        let end = self.last();
        let duration = end.time.duration_since(beg.time).as_secs_f64();
        let inc = end.value - beg.value;

        inc as f64 / duration
    }

    fn last(&self) -> &ProgressUnit {
        assert!(!self.window.is_empty());
        &self.window[(self.size + self.tail - 1) % self.size]
    }

    fn last_mut(&mut self) -> &mut ProgressUnit {
        assert!(!self.window.is_empty());
        &mut self.window[(self.size + self.tail - 1) % self.size]
    }
}
