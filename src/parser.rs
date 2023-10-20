use nom::{bytes::complete::{tag_no_case, tag}, IResult, sequence::{pair, preceded, delimited, separated_pair}, character::complete::alpha1, multi::separated_list1, combinator::map};

use crate::buffer::tuple::{Table, DatumTypes};

fn parse_table_schema(input: &str) -> IResult<&str, Vec<(&str, &str)>> {
    Ok(delimited(tag("("), separated_list1(tag(","), separated_pair(alpha1, tag(" "), alpha1)), tag(")"))(input)?)
}

pub fn parse_create_table(input: &str) -> IResult<&str, (&str, Vec<(&str, &str)>)> {
    Ok(pair(preceded(tag_no_case("CREATE TABLE "), alpha1), parse_table_schema)(input)?)
}

impl Table {
    pub fn parse(input: &str) -> IResult<&str, Self> {
        Ok(map(parse_create_table, |(_name, schema)| {
            Self::create(7, schema.iter().map(|(col, typ)| ((*col).to_string(), DatumTypes::parse(typ))).collect());
            Self::new(7)
        })(input)?)
    }
}

#[cfg(test)]
mod tests {
    use crate::buffer::tuple::{Table, DatumTypes};

    #[test]
    fn test_create_table() {
        assert_eq!(Table::parse("CREATE TABLE Users(id INT)"), Ok(("", Table {id: 7, num_blocks: 1, schema: vec![("id".into(), DatumTypes::Int)]})));
    } 
}