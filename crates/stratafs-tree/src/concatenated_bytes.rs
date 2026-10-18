use std::cmp::Ordering;

// ConcatenatedBytes provides a zero copy view of 2 buffers
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct ConcatenatedBytes<'a> {
    first: &'a [u8],
    second: &'a [u8],
}

impl<'a> ConcatenatedBytes<'a> {
    pub(crate) fn new(first: &'a [u8], second: &'a [u8]) -> Self {
        Self { first, second }
    }
}

impl<'a> From<&'a [u8]> for ConcatenatedBytes<'a> {
    fn from(bytes: &'a [u8]) -> Self {
        Self::new(bytes, &[])
    }
}

impl Ord for ConcatenatedBytes<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        let mut left = [self.first, self.second];
        let mut right = [other.first, other.second];
        let (mut left_part, mut right_part) = (0, 0);
        loop {
            while left_part < 2 && left[left_part].is_empty() {
                left_part += 1;
            }
            while right_part < 2 && right[right_part].is_empty() {
                right_part += 1;
            }
            match (left_part < 2, right_part < 2) {
                (false, false) => return Ordering::Equal,
                (false, true) => return Ordering::Less,
                (true, false) => return Ordering::Greater,
                (true, true) => {}
            }
            let length = left[left_part].len().min(right[right_part].len());
            let (left_head, left_rest) = left[left_part].split_at(length);
            let (right_head, right_rest) = right[right_part].split_at(length);
            match left_head.cmp(right_head) {
                Ordering::Equal => {
                    left[left_part] = left_rest;
                    right[right_part] = right_rest;
                }
                ordering => return ordering,
            }
        }
    }
}

impl PartialOrd for ConcatenatedBytes<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for ConcatenatedBytes<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for ConcatenatedBytes<'_> {}

#[cfg(test)]
mod tests;
