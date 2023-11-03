use std::sync::Arc;

use nom::{bytes::complete::{tag_no_case, tag}, IResult, sequence::{preceded, delimited, separated_pair}, character::complete::{alpha1, alphanumeric1}, multi::separated_list1, branch::alt};

use crate::{buffer::tuple::{RowTable, DatumTypes, Table, Datum, TupleOps, Tuple, PageBuffer}, operator::{Select, SelectIter}, error::Error, storage::folder::Folder};

pub mod ast;
pub mod semantic;
pub mod generator;

pub fn parse_create_table(input: &str) -> IResult<&str, impl '_ + Fn(Arc<PageBuffer>, Arc<Folder>) -> Result<(), Error>> {
    let (input, name) = preceded(tag_no_case("CREATE TABLE "), alpha1)(input)?;
    let (input, schema) = delimited(tag("("), separated_list1(tag(","), separated_pair(alpha1, tag(" "), alpha1)), tag(")"))(input)?;
    
    Ok((input, move |_buf: Arc<PageBuffer>, f: Arc<Folder>| {
        let mut err = Ok(());
        let schema = schema.iter().scan(&mut err, |err, (col, typ)| {
            match DatumTypes::parse(typ) {
                Ok(v) => Some(((*col).to_string(), v)),
                Err(e) => {
                    **err = Err(e);
                    None
                }
            }
        }).collect();
        err?;
        RowTable::create(Arc::clone(&f), name.to_string(), schema)?;
        RowTable::new(Arc::clone(&f), name)?;
        Ok(())
    }))
}

pub fn parse_select(input: &str) -> IResult<&str, impl '_ + Fn(Arc<PageBuffer>, Arc<Folder>) -> Result<SelectIter, Error>> {
    let (input, _cols) = preceded(tag_no_case("SELECT "), separated_list1(tag(","), alt((tag("*"), alpha1))))(input)?;
    let (input, name) = preceded(tag_no_case(" FROM "), alpha1)(input)?;

    Ok((input, move |buf: Arc<PageBuffer>, f: Arc<Folder>| {
        let table = RowTable::new(Arc::clone(&f), name)?;
        Ok(Select::new(table, buf, |_| true).into_iter())
    }))
}

pub fn parse_insert(input: &str) -> IResult<&str, impl '_ + Fn(Arc<PageBuffer>, Arc<Folder>) -> Result<(), Error>>  {
    let (input, name) = preceded(tag_no_case("INSERT INTO "), alpha1)(input)?;
    let (input, values) = preceded(tag_no_case(" VALUES"), delimited(tag("("), separated_list1(tag(","), alphanumeric1), tag(")")))(input)?;
    
    Ok((input, move |buf: Arc<PageBuffer>, f: Arc<Folder>| {
        let mut table = RowTable::new(Arc::clone(&f), name)?;
        let schema = table.get_schema();
        let tup = schema.iter().zip(values.iter()).map(|((_, typ), inp)| {
            match typ {
                DatumTypes::Int => Datum::Int((*inp).parse::<i32>().unwrap()),
                DatumTypes::Float => Datum::Float((*inp).parse::<f32>().unwrap())
            }
        }).collect();
        table.add(buf, tup)
    }))    
}

pub fn parse(input: &str, buf: Arc<PageBuffer>, f: Arc<Folder>) -> Result<Option<Vec<Tuple>>, Error> {
    let res = parse_create_table(input);
    if res.is_ok() { (res.unwrap().1)(buf, Arc::clone(&f))?; return Ok(None); }
    let res = parse_insert(input);
    if res.is_ok() { (res.unwrap().1)(buf, Arc::clone(&f)).unwrap(); return Ok(None); }
    let res = parse_select(input);
    if res.is_ok() { return Ok(Some((res.unwrap().1)(buf, Arc::clone(&f))?.collect())); }
    return Err(Error::ParseError)
}

#[cfg(test)]
mod tests {
    use crate::compiler::parse_create_table;

    #[test]
    fn test_table_create() {
        let input = "create table Two(id INT,price INT)";
        assert!(parse_create_table(input).is_ok())
    }
}

