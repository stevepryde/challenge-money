use std::{
    fmt::Display,
    ops::{Add, AddAssign, Sub, SubAssign},
    str::FromStr,
};

use anyhow::Context;
use rust_decimal::Decimal;
use serde_with::{DeserializeFromStr, SerializeDisplay};

const DECIMAL_PLACES: u32 = 4;

#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    SerializeDisplay,
    DeserializeFromStr,
)]
pub struct Currency(Decimal);

impl Currency {
    pub fn is_negative(&self) -> bool {
        self.0 < Decimal::ZERO
    }
}

impl Display for Currency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.round_dp(DECIMAL_PLACES))
    }
}

impl FromStr for Currency {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(
            Decimal::from_str(s).context("failed to parse currency value: {s}")?,
        ))
    }
}

impl Add for Currency {
    type Output = Currency;

    fn add(self, rhs: Self) -> Self::Output {
        Self(self.0 + rhs.0)
    }
}

impl AddAssign for Currency {
    fn add_assign(&mut self, rhs: Self) {
        self.0 += rhs.0;
    }
}

impl Sub for Currency {
    type Output = Currency;

    fn sub(self, rhs: Self) -> Self::Output {
        Self(self.0 - rhs.0)
    }
}

impl SubAssign for Currency {
    fn sub_assign(&mut self, rhs: Self) {
        self.0 -= rhs.0;
    }
}

#[cfg(test)]
pub mod test_support {
    use super::*;

    use proptest::prelude::*;
    use rust_decimal::prelude::FromPrimitive;

    impl Currency {
        pub fn from_f64(value: f64) -> Self {
            Self(Decimal::from_f64(value).unwrap())
        }
    }

    impl Arbitrary for Currency {
        type Parameters = ();

        fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
            any::<f64>()
                .prop_map(|x| Self(Decimal::from_f64(x).unwrap_or_default()))
                .boxed()
        }

        type Strategy = BoxedStrategy<Self>;
    }
}
