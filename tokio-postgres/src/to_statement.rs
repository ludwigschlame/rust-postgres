use postgres_types::ProtocolEncodingFormat;

use crate::to_statement::private::{Sealed, ToStatementType};
use crate::Statement;

mod private {
    use postgres_types::ProtocolEncodingFormat;

    use crate::{Client, Error, Statement};

    pub trait Sealed {}

    pub enum ToStatementType<'a> {
        Statement(ProtocolEncodingFormat, &'a Statement),
        Query(ProtocolEncodingFormat, &'a str),
    }

    impl<'a> ToStatementType<'a> {
        pub async fn into_statement(self, client: &Client) -> Result<Statement, Error> {
            match self {
                ToStatementType::Statement(result_format, s) => {
                    if result_format != s.result_format() {
                        Err(Error::column(format!(
                            "the requested encoding '{:?}' does not mat the statements encoding '{:?}'",
                            result_format,
                            s.result_format()
                        )))
                    } else {
                        Ok(s.clone())
                    }
                }
                ToStatementType::Query(result_format, s) => {
                    client.prepare_with_result_format(s, result_format).await
                }
            }
        }
    }
}

/// A trait abstracting over prepared and unprepared statements.
///
/// Many methods are generic over this bound, so that they support both a raw query string as well as a statement which
/// was prepared previously.
///
/// This trait is "sealed" and cannot be implemented by anything outside this crate.
pub trait ToStatement: Sealed {
    #[doc(hidden)]
    fn __convert(&self, result_format: ProtocolEncodingFormat) -> ToStatementType<'_>;
}

impl ToStatement for Statement {
    fn __convert(&self, result_format: ProtocolEncodingFormat) -> ToStatementType<'_> {
        ToStatementType::Statement(result_format, self)
    }
}

impl Sealed for Statement {}

impl ToStatement for str {
    fn __convert(&self, result_format: ProtocolEncodingFormat) -> ToStatementType<'_> {
        ToStatementType::Query(result_format, self)
    }
}

impl Sealed for str {}

impl ToStatement for String {
    fn __convert(&self, result_format: ProtocolEncodingFormat) -> ToStatementType<'_> {
        ToStatementType::Query(result_format, self)
    }
}

impl Sealed for String {}
