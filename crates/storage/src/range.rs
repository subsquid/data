

type Range = std::ops::Range<usize>;


pub struct RangeList {
    ranges: Vec<Range>
}


impl TryFrom<Vec<Range>> for RangeList {
    type Error = &'static str;

    fn try_from(ranges: Vec<Range>) -> Result<Self, Self::Error> {
        if ranges.iter().any(|r| r.is_empty()) {
            return Err("range list can only contain non-empty ranges")
        }
        for i in 1..ranges.len() {
            let current = &ranges[i];
            let prev = &ranges[i-1];
            if prev.end > current.start {
                return Err("found unordered or overlapping ranges")
            }
        }
        Ok(Self {
            ranges
        })
    }
}


impl RangeList {
    pub fn new(ranges: Vec<Range>) -> Self {
        Self::try_from(ranges).unwrap()
    }

    pub fn iter(&self) -> impl Iterator<Item=&Range> + '_ {
        self.ranges.iter()
    }
}