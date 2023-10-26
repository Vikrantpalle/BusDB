use nom::{bytes::complete::{tag_no_case, tag}, IResult, sequence::{preceded, delimited, separated_pair}, character::complete::{alpha1, alphanumeric1}, multi::separated_list1, branch::alt};

use crate::{buffer::{tuple::{Table, DatumTypes, File, Datum, TupleOps, Operator, Tuple}, ClockBuffer}, optree::Select, error::Error};

pub fn parse_create_table(input: &str) -> IResult<&str, impl '_ + Fn(&mut ClockBuffer) -> Result<(), Error>> {
    let (input, name) = preceded(tag_no_case("CREATE TABLE "), alpha1)(input)?;
    let (input, schema) = delimited(tag("("), separated_list1(tag(","), separated_pair(alpha1, tag(" "), alpha1)), tag(")"))(input)?;
    
    Ok((input, move |_buf: &mut ClockBuffer| {
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
        Table::create(name.to_string(),schema)?;
        Table::new(name.to_string())?;
        Ok(())
    }))
}

pub fn parse_select(input: &str) -> IResult<&str, Result<Select, Error>> {
    let (input, _cols) = preceded(tag_no_case("SELECT "), separated_list1(tag(","), alt((tag("*"), alpha1))))(input)?;
    let (input, name) = preceded(tag_no_case(" FROM "), alpha1)(input)?;

    let table = Table::new(name.to_string());
    match table {
        Ok(t) => Ok((input, Ok(Select::new(t, |_| true)))),
        Err(e) => Ok((input, Err(e)))
    }
}

pub fn parse_insert(input: &str) -> IResult<&str, impl '_ + Fn(&mut ClockBuffer) -> Result<(), Error>>  {
    let (input, name) = preceded(tag_no_case("INSERT INTO "), alpha1)(input)?;
    let (input, values) = preceded(tag_no_case(" VALUES"), delimited(tag("("), separated_list1(tag(","), alphanumeric1), tag(")")))(input)?;
    
    Ok((input, move |buf: &mut ClockBuffer| {
        let mut table = Table::new(name.to_string())?;
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

pub fn parse(input: &str, buf: &mut ClockBuffer) -> Result<Option<Vec<Tuple>>, Error> {
    let res = parse_create_table(input);
    if res.is_ok() { (res.unwrap().1)(buf)?; return Ok(None); }
    let res = parse_insert(input);
    if res.is_ok() { (res.unwrap().1)(buf).unwrap(); return Ok(None); }
    let res = parse_select(input);
    if res.is_ok() { return Ok(Some(res.unwrap().1?.collect(buf))); }
    return Err(Error::ParseError)
}

#[cfg(test)]
mod tests {
    use crate::parser::parse_create_table;

    #[test]
    fn test_table_create() {
        let input = "create table Two(id INT,price INT)";
        assert!(parse_create_table(input).is_ok())
    }
}

