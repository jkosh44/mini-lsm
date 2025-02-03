#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use anyhow::Result;

use super::StorageIterator;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ActiveIter {
    A,
    B,
}

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    // Add fields as need
    active_iter: ActiveIter,
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > TwoMergeIterator<A, B>
{
    pub fn create(a: A, b: B) -> Result<Self> {
        let active_iter = active_iter(&a, &b);
        Ok(Self { a, b, active_iter })
    }
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        match self.active_iter {
            ActiveIter::A => self.a.key(),
            ActiveIter::B => self.b.key(),
        }
    }

    fn value(&self) -> &[u8] {
        match self.active_iter {
            ActiveIter::A => self.a.value(),
            ActiveIter::B => self.b.value(),
        }
    }

    fn is_valid(&self) -> bool {
        self.a.is_valid() || self.b.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        match self.active_iter {
            ActiveIter::A
                if self.a.is_valid() && self.b.is_valid() && self.a.key() == self.b.key() =>
            {
                self.a.next()?;
                self.b.next()?;
            }
            ActiveIter::A if self.a.is_valid() => self.a.next()?,
            ActiveIter::B if self.b.is_valid() => self.b.next()?,
            _ => {}
        }

        self.active_iter = active_iter(&self.a, &self.b);

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.a.num_active_iterators() + self.b.num_active_iterators()
    }
}

fn active_iter<
    A: 'static + StorageIterator,
    B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
>(
    a: &A,
    b: &B,
) -> ActiveIter {
    match (a.is_valid(), b.is_valid()) {
        (true, true) => {
            if a.key() <= b.key() {
                ActiveIter::A
            } else {
                ActiveIter::B
            }
        }
        (true, false) => ActiveIter::A,
        (false, true) => ActiveIter::B,
        // Doesn't really matter what we return here.
        (false, false) => ActiveIter::A,
    }
}
