use std::sync::Arc;

use crate::{error::Error, buffer::tuple::{RowTable, Table}, operator::predicate::{Predicate, Equal}, storage::folder::Folder};

use super::ast::Node;


pub trait TypeCheck {
    fn check(&self, f: Arc<Folder>) -> Result<(), Error>;
}

impl TypeCheck for Node {
    fn check(&self, f: Arc<Folder>) -> Result<(), Error> {
        let t = RowTable::new(Arc::clone(&f), &self.table)?;
        if !self.cols.iter().all(|inp| t.get_schema().iter().find(|(col, _)| col == inp).is_some()) {return Err(Error::ColumnDoesNotExist);}
        if self.pred.is_some() { self.pred.as_ref().unwrap().check(Arc::clone(&f))?; }
        if self.join.is_some() { self.join.as_ref().unwrap().check(Arc::clone(&f))?; }
        return Ok(())
    }
}


impl TypeCheck for Equal {
    fn check(&self, f: Arc<Folder>) -> Result<(), Error> {
        let ty1 = self.l.get_type(Arc::clone(&f))?;
        let ty2 = self.r.get_type(Arc::clone(&f))?;
        if ty1 == ty2 { Ok(()) } else { Err(Error::TypeMismatch) }
    }
}

impl TypeCheck for Predicate {
    fn check(&self, f: Arc<Folder>) -> Result<(), Error> {
        match self {
            Self::Equal(e) => e.check(Arc::clone(&f))
        }
    }
}


#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{compiler::{ast::Node, semantic::TypeCheck}, buffer::tuple::{RowTable, DatumTypes}, storage::folder::Folder, operator::predicate::{Equal, Field, Predicate}};


    #[test]
    fn test_type_check() {
        Folder::create().unwrap();
        let a = "a".to_string();
        let b = "b".to_string();
        let f = Arc::new(Folder::new().unwrap());
        RowTable::create(Arc::clone(&f), a.clone(), vec![("id".into(), DatumTypes::Int)]).unwrap();
        RowTable::create(Arc::clone(&f), b.clone(), vec![("id".into(), DatumTypes::Int)]).unwrap();
        let b = Node { table: b.clone(), cols: vec![], pred: Some(Predicate::Equal(Equal { l: Field { table: a.to_string(), col: "id".into() }, r: Field { table: b.to_string(), col: "id".into() }})), join: None};
        let a = Node { table: a.clone(), cols: vec![], pred: None, join: Some(Box::new(b))};
        a.check(Arc::clone(&f)).unwrap();
        assert!(a.check(Arc::clone(&f)).is_ok());
    }
}

