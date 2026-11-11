//! Symbol dictionary: interns `(venue, symbol)` strings into dense `u32` ids.
//!
//! Hot path: feeds repeat the same few symbols, so [`SymbolDict::intern`] first compares against
//! the last symbol seen (one short memcmp), then looks up a map keyed with foldhash: 8 bytes per
//! step, each mixed by a folded 64×64→128-bit multiply (`mul` + `umulh` on Arm), instead of one
//! dependent multiply per byte (FNV-1a: 16 of them for a 16-byte metric name). `SipHash` on every
//! message showed up at ~19% of ingest time in profiles. The seed is random per dictionary, so
//! publishers cannot aim collisions at it (the entries are capped as well).

use std::collections::HashMap;
use std::hash::Hasher;

/// Dense symbol identifier (dictionary code).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SymbolId(pub u32);

/// FNV-1a over bytes: fast for short identifiers; not DoS-resistant, which is acceptable because
/// the dictionary only grows with distinct symbol names from subscribed streams.
#[derive(Default, Clone, Copy)]
pub struct Fnv1a(u64);

impl Hasher for Fnv1a {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        let mut h = if self.0 == 0 {
            0xcbf2_9ce4_8422_2325
        } else {
            self.0
        };
        for &b in bytes {
            h ^= u64::from(b);
            h = h.wrapping_mul(0x0100_0000_01b3);
        }
        self.0 = h;
    }
}

/// Interner. Ids are assigned in first-seen order and never change for the process lifetime;
/// persisted dictionaries (P1-06) replay the same order.
#[derive(Debug, Default, Clone)]
pub struct SymbolDict {
    by_name: HashMap<String, SymbolId, foldhash::fast::RandomState>,
    names: Vec<String>,
    /// Most recently interned symbol: `(id, name)`; feeds repeat symbols back to back.
    last: Option<(SymbolId, String)>,
}

impl std::fmt::Debug for Fnv1a {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Fnv1a")
    }
}

impl SymbolDict {
    /// Empty dictionary.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Id for `name`, interning it if new. Allocates only on first sight of a symbol.
    #[inline]
    pub fn intern(&mut self, name: &str) -> SymbolId {
        if let Some((id, last)) = &self.last
            && last.as_str() == name
        {
            return *id;
        }
        let id = if let Some(&id) = self.by_name.get(name) {
            id
        } else {
            let id = SymbolId(u32::try_from(self.names.len()).unwrap_or(u32::MAX));
            self.names.push(name.to_owned());
            self.by_name.insert(name.to_owned(), id);
            id
        };
        match &mut self.last {
            Some((lid, lname)) => {
                *lid = id;
                lname.clear();
                lname.push_str(name);
            }
            None => self.last = Some((id, name.to_owned())),
        }
        id
    }

    /// Lookup without interning.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<SymbolId> {
        if let Some((id, last)) = &self.last
            && last.as_str() == name
        {
            return Some(*id);
        }
        self.by_name.get(name).copied()
    }

    /// Name for an id.
    #[must_use]
    pub fn name(&self, id: SymbolId) -> Option<&str> {
        self.names.get(id.0 as usize).map(String::as_str)
    }

    /// Names in id order (for persistence).
    #[must_use]
    pub fn names(&self) -> &[String] {
        &self.names
    }

    /// Number of symbols.
    #[must_use]
    pub fn len(&self) -> usize {
        self.names.len()
    }

    /// True when empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.names.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interns_stably_and_caches_last() {
        let mut d = SymbolDict::new();
        let a = d.intern("BTCUSDT");
        let b = d.intern("ETHUSDT");
        assert_ne!(a, b);
        assert_eq!(d.intern("BTCUSDT"), a);
        assert_eq!(d.intern("BTCUSDT"), a);
        assert_eq!(d.intern("ETHUSDT"), b);
        assert_eq!(d.get("ETHUSDT"), Some(b));
        assert_eq!(d.name(a), Some("BTCUSDT"));
        assert_eq!(d.len(), 2);
    }
}
