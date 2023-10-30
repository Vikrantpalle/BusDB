use nom::{IResult, sequence::{preceded, delimited, tuple, separated_pair}, character::complete::{space0, alpha1, space1}, bytes::complete::{tag_no_case, tag}};

use crate::operator::predicate::{Predicate, Equal, Field};

#[derive(Debug, PartialEq)]
pub struct Node {
    pub table: String,
    pub cols: Vec<String>,
    pub pred: Option<Predicate>,
    pub join: Option<Box<Node>>
}

pub fn parse_join(input: &str) -> IResult<&str, Option<Node>> {
    if input == "" { return Ok((input, None))}
    let (input, _op) = preceded(space1, tag_no_case("JOIN"))(input)?;
    let (input, name) = preceded(space1, alpha1)(input)?;
    let (input, (f1, _alop, f2)) = tuple((preceded(delimited(space1, tag_no_case("ON"), space1), separated_pair(alpha1, tag("."), alpha1)), delimited(space0, tag("="), space0), separated_pair(alpha1, tag("."), alpha1)))(input)?;
    Ok((input, Some(Node { table: name.into(), cols: vec![], pred: Some(Predicate::Equal(Equal { l: Field { table: f1.0.into(), col: f1.1.into() }, r: Field { table: f2.0.into(), col: f2.1.into() }})), join: parse_join(input)?.1.map(|n| Box::new(n))})))
}

pub fn parse_ast(input: &str) -> IResult<&str, Node> {
    let (input, l_name) = preceded(space0, alpha1)(input)?;
    let (input, r) = parse_join(input)?;
    Ok((input, Node { table: l_name.into(), cols: vec![], pred: None, join: r.map(|n| Box::new(n))}))
}

#[cfg(test)]
mod tests {
    use crate::{compiler::ast::{Field, Equal}, operator::predicate::Predicate};

    use super::{parse_ast, Node};

    #[test]
    fn test_ast() {
        let ast = parse_ast("a join b on a.id = b.id").unwrap().1;
        let b = Node { table: "b".into(), cols: vec![], pred: Some(Predicate::Equal(Equal { l: Field { table: "a".into(), col: "id".into() }, r: Field { table: "b".into(), col: "id".into() }})), join: None};
        let a = Node { table: "a".into(), cols: vec![], pred: None, join: Some(Box::new(b))};
        assert_eq!(ast, a)
    }

    #[test]
    fn test_ast_neg() {
        assert!(parse_ast("a join b jon c").is_err())
    }
}