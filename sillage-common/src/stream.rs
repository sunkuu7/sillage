use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Stream {
    Tx,
    Acct,
    Block,
}

impl Stream {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Tx => "tx",
            Self::Acct => "acct",
            Self::Block => "block",
        }
    }

    pub fn all() -> [Stream; 3] {
        [Self::Tx, Self::Acct, Self::Block]
    }
}

impl std::fmt::Display for Stream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_as_str() {
        assert_eq!(Stream::Tx.as_str(), "tx");
        assert_eq!(Stream::Acct.as_str(), "acct");
        assert_eq!(Stream::Block.as_str(), "block");
    }

    #[test]
    fn test_all() {
        let all = Stream::all();
        assert_eq!(all.len(), 3);
        assert!(all.contains(&Stream::Tx));
        assert!(all.contains(&Stream::Acct));
        assert!(all.contains(&Stream::Block));
    }

    #[test]
    fn test_display() {
        assert_eq!(format!("{}", Stream::Tx), "tx");
        assert_eq!(format!("{}", Stream::Acct), "acct");
        assert_eq!(format!("{}", Stream::Block), "block");
    }
}
