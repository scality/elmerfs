use crate::driver::{Config, Result, ENOENT};
use crate::model::dentries;
use crate::model::inode::{self, Ino};
use crate::time;
use crate::view::View;
use crate::view::{Name, NameRef};
use antidotec::{reads, updates, Transaction};
use nix::unistd;
use std::borrow::Cow;
use std::collections::HashMap;
use std::str;
use std::sync::Arc;

#[derive(Debug)]
pub(super) struct DirDriver {
    config: Arc<Config>,
}

impl DirDriver {
    pub(super) fn new(config: Arc<Config>) -> Self {
        Self { config }
    }

    pub(super) async fn decode_view(
        &self,
        view: View,
        tx: &mut Transaction<'_>,
        ino: Ino,
        reply: &mut antidotec::ReadReply,
        index: usize,
    ) -> Result<DirView> {
        let entries = dentries::decode(reply, index).unwrap_or_default();
        self.view(view, tx, entries, ino).await
    }

    pub(super) fn lookup(
        &self,
        view: View,
        reply: &mut antidotec::ReadReply,
        index: usize,
        name: &NameRef,
    ) -> Option<Ino> {
        let entries = dentries::decode(reply, index).unwrap_or_default();
        let (effective_view, prefix) = match name {
            NameRef::Exact(name) => (name.view, &name.prefix),
            NameRef::Partial(prefix) => (view, prefix),
        };

        let mut candidate = None;
        let mut must_match_exact = false;
        for entry in entries {
            // We may need to render this a bit more up to date. e.g check if
            // the looked up entry is a directory, and if so, check its parent.
            if prefix == &entry.name.prefix {
                if effective_view == entry.name.view {
                    return Some(entry.ino);
                }

                // If it already has a candidate, the entry is in conflict,
                // and the lookup must only succeed if the view match. 
                if candidate.is_some() {
                    must_match_exact = true;
                    candidate = None;
                }

                if candidate.is_none() && !must_match_exact {
                    candidate = Some(entry.ino);
                }
            }
        }

        candidate
    }

    #[tracing::instrument(skip(self, tx, entries))]
    async fn view(
        &self,
        view: View,
        tx: &mut Transaction<'_>,
        mut entries: Vec<dentries::Entry>,
        ino: Ino,
    ) -> Result<DirView> {
        entries.sort();

        /* When loading a directory, we have a series of cleanup to do:

        1. If there are multiple directory entry referring to
           the same directory, we need to delete duplicate entries.

        2. If we stumble upon a directory whom parent is different from
           the one being loaded, the entry is stale, it needs to be deleted

        For 2. we need to query all the attributes and those must come
        from the same transaction. */
        let mut entries_to_purge = Vec::new();
        self.collect_directory_duplicates(&entries[..], &mut entries_to_purge);

        self.check_directory_parent_ino(tx, ino, &entries[..], &mut entries_to_purge)
            .await?;

        entries_to_purge.sort();
        entries_to_purge.dedup_by(|lhs, rhs| lhs.idx == rhs.idx);

        tracing::debug!(?entries_to_purge);
        let view = if !entries_to_purge.is_empty() {
            let entries_to_purge = entries_to_purge;

            let cleaned_entries = self.clean_loaded_entries(&entries[..], &entries_to_purge[..]);

            let mut reply = tx
                .read(self.config.bucket(), reads!(inode::read(ino)))
                .await?;
            let inode = inode::decode(ino, &mut reply, 0).ok_or(ENOENT)?;

            self.purge(tx, ino, &inode, &entries[..], entries_to_purge)
                .await?;

            self.load_dir_view(view, cleaned_entries)
        } else {
            self.load_dir_view(view, entries)
        };

        Ok(view)
    }

    fn collect_directory_duplicates(
        &self,
        sorted_entries: &[dentries::Entry],
        duplicates: &mut Vec<PurgeEntry>,
    ) {
        let mut previous_ino = None; /* 0 is an ino number that cannot appear. */

        let directories = sorted_entries.iter().enumerate().filter(|(_, e)| {
            inode::kind(e.ino) == inode::Directory && !Self::is_dot(&e.name.prefix)
        });

        for (idx, entry) in directories {
            if Some(entry.ino) == previous_ino {
                duplicates.push(PurgeEntry {
                    idx,
                    ino: entry.ino,
                });
            } else {
                previous_ino = Some(entry.ino);
            }
        }
    }

    async fn check_directory_parent_ino(
        &self,
        tx: &mut Transaction<'_>,
        parent_ino: Ino,
        sorted_entries: &[dentries::Entry],
        invalid: &mut Vec<PurgeEntry>,
    ) -> Result<()> {
        let directories = sorted_entries.iter().enumerate().filter(|(_, e)| {
            inode::kind(e.ino) == inode::Directory && !Self::is_dot(&e.name.prefix)
        });

        let mut children = tx
            .read(
                self.config.bucket(),
                directories.clone().map(|(_, d)| inode::read(d.ino)),
            )
            .await?;

        for (query_idx, (idx, entry)) in directories.enumerate() {
            let child_inode = inode::decode(entry.ino, &mut children, query_idx).ok_or(ENOENT)?;

            if child_inode.dotdot != Some(parent_ino) {
                tracing::debug!(?child_inode, ?parent_ino, "invalid parent_ino");

                invalid.push(PurgeEntry {
                    idx,
                    ino: entry.ino,
                });
            }
        }

        Ok(())
    }

    fn clean_loaded_entries(
        &self,
        entries: &[dentries::Entry],
        purged: &[PurgeEntry],
    ) -> Vec<dentries::Entry> {
        let mut purged = purged.iter().peekable();

        entries
            .iter()
            .cloned()
            .enumerate()
            .filter_map(|(idx, e)| {
                if inode::kind(e.ino) != inode::Directory {
                    return Some(e);
                }

                match purged.peek() {
                    Some(purged_entry) if purged_entry.idx == idx => {
                        purged.next();
                        None
                    }
                    _ => Some(e),
                }
            })
            .collect()
    }

    #[tracing::instrument(skip(tx))]
    async fn purge(
        &self,
        tx: &mut Transaction<'_>,
        parent_ino: Ino,
        parent_inode: &inode::Inode,
        entries: &[dentries::Entry],
        purged: Vec<PurgeEntry>,
    ) -> Result<()> {
        let ts = time::now();

        for purged in purged {
            let entry = &entries[purged.idx];
            let old_link = parent_inode.links.find(purged.ino, &entry.name).cloned();

            if old_link.is_none() {
                tracing::warn!(
                    ?parent_inode,
                    ?entry,
                    "found an entry that wasn't referenced in parent inode."
                );
            } else {
                tx.update(
                    self.config.bucket(),
                    updates!(
                        inode::remove_link(ts, entry.ino, old_link.unwrap()),
                        dentries::remove_entry(parent_ino, entry)
                    ),
                )
                .await?;
            }
        }

        Ok(())
    }

    fn load_dir_view(&self, view: View, sorted_entries: Vec<dentries::Entry>) -> DirView {
        use std::collections::hash_map::Entry as HashEntry;

        let mut by_name: HashMap<_, EntryList> = HashMap::with_capacity(sorted_entries.len());
        let mut entries: Vec<_> = sorted_entries
            .into_iter()
            .map(|entry| EntryView {
                ino: entry.ino,
                prefix: Arc::from(entry.name.prefix),
                view: entry.name.view,
                next: None,
            })
            .collect();

        for idx in 0..entries.len() {
            let prefix = entries[idx].prefix.clone();

            match by_name.entry(prefix) {
                HashEntry::Occupied(mut entry) => {
                    let entry_list = entry.get_mut();
                    entries[entry_list.tail].next = Some(idx);
                    entry_list.tail = idx;
                }
                HashEntry::Vacant(entry) => {
                    entry.insert(EntryList {
                        head: idx,
                        tail: idx,
                    });
                }
            }
        }

        DirView {
            view,
            entries,
            by_name,
        }
    }

    pub fn is_dot(prefix: &str) -> bool {
        prefix == "." || prefix == ".."
    }
}

#[derive(Debug, PartialEq, Eq, Ord, PartialOrd)]
pub struct PurgeEntry {
    idx: usize,
    ino: Ino,
}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct EntryView {
    pub ino: Ino,
    pub view: View,
    pub prefix: Arc<str>,
    next: Option<usize>,
}

impl EntryView {
    pub fn into_dentry(&self) -> dentries::Entry {
        dentries::Entry {
            ino: self.ino,
            name: Name {
                prefix: String::from(&*self.prefix as &str),
                view: self.view,
            },
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
struct EntryList {
    head: usize,
    tail: usize,
}

#[derive(Debug)]
pub struct DirView {
    view: View,
    entries: Vec<EntryView>,
    by_name: HashMap<Arc<str>, EntryList>,
}

impl DirView {
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn get(&self, name: &NameRef) -> Option<&EntryView> {
        self.position(name).map(|idx| &self.entries[idx])
    }

    pub(crate) fn contains_key(&self, name: &NameRef) -> bool {
        self.get(name).is_some()
    }

    pub(crate) fn iter_from(
        &self,
        listing_flavor: ListingFlavor,
        offset: usize,
    ) -> impl Iterator<Item = Result<EntryRef<'_>>> {
        let start = offset.min(self.entries.len());
        Iter {
            listing_flavor,
            entries: self.entries[start..].iter(),
            by_name: &self.by_name,
            view: self.view,
            user_mapping: HashMap::default(),
        }
    }

    fn position(&self, name: &NameRef) -> Option<usize> {
        match name {
            NameRef::Exact(name) => {
                let entry_list = self.by_name.get(&name.prefix as &str)?;
                self.resolve_by_view(&entry_list, name.view)
            }
            NameRef::Partial(prefix) => {
                /* This is simple algorithm to resolve conflicts (multiple entry with
                the same prefix). If there is only one entry for a given prefix
                there is no conflict so we can simply entry. Otherwise, try to
                fetch the exact entry by using our current view */

                let entry_list = self.by_name.get(prefix as &str)?;
                if entry_list.head == entry_list.tail {
                    return Some(entry_list.head);
                }

                self.resolve_by_view(&entry_list, self.view)
            }
        }
    }

    fn resolve_by_view(&self, entry_list: &EntryList, view: View) -> Option<usize> {
        let mut current = Some(entry_list.head);
        while let Some(idx) = current {
            let entry = &self.entries[idx];
            if entry.view == view {
                return Some(idx);
            }

            current = entry.next;
        }

        None
    }
}

#[derive(Debug)]
pub struct EntryRef<'a> {
    pub name: Cow<'a, str>,
    pub ino: Ino,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ListingFlavor {
    FullyQualified,
    Partial,
}
#[derive(Debug, thiserror::Error)]
#[error("invalid listing flavor name")]
pub struct ListingParseError;

impl str::FromStr for ListingFlavor {
    type Err = ListingParseError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "full" => Ok(Self::FullyQualified),
            "partial" => Ok(Self::Partial),
            _ => Err(ListingParseError),
        }
    }
}

pub(crate) struct Iter<'a> {
    listing_flavor: ListingFlavor,
    entries: std::slice::Iter<'a, EntryView>,
    by_name: &'a HashMap<Arc<str>, EntryList>,
    user_mapping: HashMap<u32, String>,
    view: View,
}

impl<'a> Iterator for Iter<'a> {
    type Item = Result<EntryRef<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        use crate::view::REF_SEP;

        let entry = self.entries.next()?;
        let entry_list = self.by_name[&entry.prefix];

        let show_alias = (entry_list.head == entry_list.tail || entry.view == self.view)
            && self.listing_flavor == ListingFlavor::Partial;

        let entry = if show_alias {
            Ok(EntryRef {
                name: Cow::Borrowed(&*entry.prefix as &str),
                ino: entry.ino,
            })
        } else {
            let username = match cached_username(&mut self.user_mapping, entry.view.uid) {
                Err(error) => return Some(Err(error)),
                Ok(username) => username,
            };

            let fully_qualified = format!(
                "{prefix}{sep}{username}",
                prefix = entry.prefix,
                sep = REF_SEP,
                username = username
            );

            Ok(EntryRef {
                name: Cow::Owned(fully_qualified),
                ino: entry.ino,
            })
        };

        Some(entry)
    }
}

fn cached_username(cache: &mut HashMap<u32, String>, uid: u32) -> Result<&str> {
    use std::collections::hash_map::Entry;

    match cache.entry(uid) {
        Entry::Occupied(entry) => Ok(&*entry.into_mut()),
        Entry::Vacant(entry) => {
            let user = unistd::User::from_uid(unistd::Uid::from_raw(uid))?;

            let name = user.map(|u| u.name).unwrap_or_else(|| "UNKNOWN".into());
            Ok(&*entry.insert(name))
        }
    }
}
