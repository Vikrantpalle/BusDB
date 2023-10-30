use crate::{error::Error, buffer::tuple::{Table, File}, operator::predicate::{Predicate, Equal}};

use super::ast::Node;


pub trait TypeCheck {
    fn check(&self) -> Result<(), Error>;
}

impl TypeCheck for Node {
    fn check(&self) -> Result<(), Error> {
        let t = Table::new(&self.table)?;
        if !self.cols.iter().all(|inp| t.get_schema().iter().find(|(col, _)| col == inp).is_some()) {return Err(Error::ColumnDoesNotExist);}
        if self.pred.is_some() { self.pred.as_ref().unwrap().check()?; }
        if self.join.is_some() { self.join.as_ref().unwrap().check()?; }
        return Ok(())
    }
}


impl TypeCheck for Equal {
    fn check(&self) -> Result<(), Error> {
        let ty1 = self.l.get_type()?;
        let ty2 = self.r.get_type()?;
        if ty1 == ty2 { Ok(()) } else { Err(Error::TypeMismatch) }
    }
}

impl TypeCheck for Predicate {
    fn check(&self) -> Result<(), Error> {
        match self {
            Self::Equal(e) => e.check()
        }
    }
}


#[cfg(test)]
mod tests {
    use crate::{compiler::{ast::Node, semantic::TypeCheck}, buffer::tuple::{Table, DatumTypes}, storage::folder::Folder, operator::predicate::{Equal, Field, Predicate}};


    #[test]
    fn test_type_check() {
        let a = "a".to_string();
        let b = "b".to_string();
        Folder::create().unwrap();
        Table::create(a.clone(), vec![("id".into(), DatumTypes::Int)]).unwrap();
        Table::create(b.clone(), vec![("id".into(), DatumTypes::Int)]).unwrap();
        let b = Node { table: b.into(), cols: vec![], pred: Some(Predicate::Equal(Equal { l: Field { table: "a".into(), col: "id".into() }, r: Field { table: "b".into(), col: "id".into() }})), join: None};
        let a = Node { table: a.into(), cols: vec![], pred: None, join: Some(Box::new(b))};
        assert!(a.check().is_ok())
    }
}

