#[derive(Debug, Clone)]
struct LruEntry<K, T> {
    key: K,
    value: T,
    next_lru: u32,
    previous_lru: u32
}

#[derive(Debug, Clone)]
pub struct Lru<K: Eq,  T> {
    entries: Vec<LruEntry<K, T>>,
    head_lru: u32,
}

impl<K: Eq, T> Lru<K, T> {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            head_lru: 0,
        }
    }

    pub fn insert(&mut self, key: K, value: T) -> Option<T>
        where K: std::fmt::Debug
    {
        tracing::debug!(?key, "insert");
        match self.lookup(&key) {
            Some(old_value) => {
                Some(std::mem::replace(old_value, value))
            },
            None => {
                tracing::debug!(?key, "pushed");
                self.push_entry(key, value);
                None
            },
        }
    }

    fn push_entry(&mut self, key: K, value: T) -> u32 {
        let tail_lru = if self.entries.len() > 0  {
            self.entries[self.head_lru as usize].previous_lru
        } else {
            0
        };

        self.entries.push(LruEntry {
            key,
            value,
            next_lru: self.head_lru,
            previous_lru: tail_lru
        });

        let inserted_idx = self.entries.len() - 1;
        self.entries[self.head_lru as usize].previous_lru = inserted_idx as u32;
        self.entries[tail_lru as usize].next_lru = inserted_idx as u32;
        self.head_lru = inserted_idx as u32;


        inserted_idx as u32
    }

    pub fn lookup(&mut self, key: &K) -> Option<&mut T>
        where K: std::fmt::Debug
    {
        let entries: Vec<_> = self.entries.iter().map(|e| (&e.key, e.previous_lru, e.next_lru)).collect();
        tracing::debug!(?key, ?entries, head = self.head_lru, "lookup");
        match self.lookup_idx(key) {
            Some(entry_idx) if entry_idx == self.head_lru => Some(&mut self.entries[entry_idx as usize].value),
            Some(entry_idx) => {
                let (entry_previous_lru, entry_next_lru) = {
                    let entry = &self.entries[entry_idx as usize];
                    (entry.previous_lru, entry.next_lru)
                };

                /* Detach the current entry */
                self.entries[entry_previous_lru as usize].next_lru = entry_next_lru;
                self.entries[entry_next_lru as usize].previous_lru = entry_previous_lru;

                let tail_lru = self.entries[self.head_lru as usize].previous_lru;

                /* Put the entry at the start of the circular lru list. */
                self.entries[self.head_lru as usize].previous_lru = entry_idx as u32;
                self.entries[entry_idx as usize].next_lru = self.head_lru;
                self.entries[entry_idx as usize].previous_lru = tail_lru;
                self.entries[tail_lru as usize].next_lru = entry_idx as u32;
                self.head_lru = entry_idx as u32;

                Some(&mut self.entries[entry_idx as usize].value)
            },
            None => None,
        }
    }

    fn lookup_idx(&self, key: &K) -> Option<u32> {
        if self.entries.len() > 0 && &self.entries[self.head_lru as usize].key == key {
            return Some(self.head_lru)
        }

        self.entries.iter().position(|p| &p.key == key).map(|i| i as u32)
    }

    pub fn evict(&mut self) -> Option<(K, T)>
        where K: std::fmt::Debug
    {
        match self.entries.len() {
            0 => None,
            1 => {
                self.head_lru = 0;
                self.entries.pop().map(|p| {
                    tracing::debug!(key = ?p.key, "evicted");
                    (p.key, p.value)
                })
            }
            _ => {
                let evicted_idx = self.entries[self.head_lru as usize].previous_lru;
                let last_idx = self.entries.len() - 1;

                let (evicted_previous_lru, evicted_next_lru) = {
                    let evicted = &self.entries[evicted_idx as usize];
                    (evicted.previous_lru, evicted.next_lru)
                };
                self.entries[evicted_previous_lru as usize].next_lru = evicted_next_lru;
                self.entries[evicted_next_lru as usize].previous_lru = evicted_previous_lru;

                /* The removal moved the last element, we need to update references to him. */
                if self.head_lru == last_idx as u32 {
                    self.head_lru = evicted_idx;
                }

                if last_idx as u32 != evicted_idx  {
                    let (swapped_previous_lru, swapped_next_lru) = {
                        let last = &self.entries[last_idx];
                        (last.previous_lru, last.next_lru)
                    };

                    self.entries[swapped_previous_lru as usize].next_lru = evicted_idx;
                    self.entries[swapped_next_lru as usize].previous_lru = evicted_idx;
                }

                let evicted = self.entries.swap_remove(evicted_idx as usize);
                let entries: Vec<_> = self.entries.iter().map(|e| (&e.key, e.previous_lru, e.next_lru)).collect();
                tracing::debug!(key = ?evicted.key, ?entries, head = self.head_lru, "evicted");
                Some((evicted.key, evicted.value))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Lru;

    #[test]
    fn insert_lookup() {
        let mut lru = Lru::new();

        let key = "key";
        let mut value = 0;
        assert_eq!(lru.insert(key, value), None);
        assert_eq!(lru.lookup(&key), Some(&mut value));

        /* With one entry, the evicted entry should reset the lru to an empty state */
        assert_eq!(lru.evict(), Some((key, value)));
        assert_eq!(lru.lookup(&key), None);

        assert_eq!(lru.entries.len(), 0);

        /* Inserting the same entry is fine but the entry has been evicted */
        assert_eq!(lru.insert(key, value), None);

        /* Inserting it again returns the old value */
        assert_eq!(lru.insert(key, value), Some(0));
    }

    #[test]
    fn lru_reorders() {
        let mut lru = Lru::new();

        for x in 0..3 {
            assert_eq!(lru.insert(x, ()), None);
        }

        /* Inserts updates the lru order */
        assert_eq!(lru.evict(), Some((0, ())));

        /* However, eviction does not */
        assert_eq!(lru.evict(), Some((1, ())));

        /* Inserting doesn't not place the new value as the lru. */
        assert_eq!(lru.insert(3, ()), None);
        assert_eq!(lru.evict(), Some((2, ())));

        /* Lookups brings back the current lru as the mru. */
        assert_eq!(lru.insert(4, ()), None);
        assert_eq!(lru.lookup(&3), Some(&mut ()));
        assert_eq!(lru.evict(), Some((4, ())));

        /* Looking up the only entry doesn't break the list. */
        assert_eq!(lru.lookup(&3), Some(&mut ()));
        assert_eq!(lru.evict(), Some((3, ())));
        assert_eq!(lru.lookup(&3), None);
        assert_eq!(lru.entries.len(), 0);
    }

    #[test]
    fn lru_lookup_backward() {
        let mut lru = Lru::new();

        for x in 0..5 {
            assert_eq!(lru.insert(x, ()), None);
        }

        for x in 0..5 {
            assert_eq!(lru.lookup(&x), Some(&mut ()));
        }

        assert_eq!(lru.evict(), Some((0, ())));
    }

    #[test]
    fn lru_multiple_inserts_and_lookup() {
        let mut lru = Lru::new();

        for x in 0..3 {
            assert_eq!(lru.insert(x, ()), None);
            assert_eq!(lru.insert(x, ()), Some(()));
        }

        assert_eq!(lru.lookup(&0), Some(&mut ()));
        assert_eq!(lru.evict(), Some((1, ())));
    }
}
