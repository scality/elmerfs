use std::str::FromStr;
use std::str;
use nix::unistd;

pub const REF_SEP: &str = ":";

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct View {
    pub uid: u32,
}

#[derive(thiserror::Error, Debug)]
#[error("invalid view name")]
pub struct ParseViewError;

impl str::FromStr for View {
    type Err = ParseViewError;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        let user = unistd::User::from_name(name).map_err(|_| ParseViewError)?;

        match user {
            Some(user) => Ok(View { uid: user.uid.as_raw() as u32 }),
            None => Err(ParseViewError)
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Name {
    pub view: View,
    pub prefix: String,
}

impl Name {
    pub fn new(prefix: impl Into<String>, view: View) -> Self {
        Name {
            view,
            prefix: prefix.into(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum NameRef {
    Partial(String),
    Exact(Name),
}

impl NameRef {
    pub fn canonicalize(self, view: View) -> Name {
        match self {
            Self::Partial(prefix) => Name { prefix, view },
            Self::Exact(name) => name,
        }
    }
}

pub struct NameRefParseError;

impl FromStr for NameRef {
    type Err = NameRefParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut s = s.split(REF_SEP);

        let prefix = String::from(s.next().ok_or(NameRefParseError)?);
        let view = match s.next() {
            Some(view) => view,
            None => return Ok(Self::Partial(prefix)),
        };

        let view = view.parse().map_err(|_| NameRefParseError)?;
        Ok(Self::Exact(Name::new(prefix, view)))
    }
}
