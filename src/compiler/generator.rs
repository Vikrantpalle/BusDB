use crate::{operator::{Operator, Select, Project, Join}, buffer::{tuple::Table, ClockBuffer}, error::Error};

use super::ast::Node;

pub trait Generate {
    fn generate(&self, buf: &mut ClockBuffer) -> Result<Operator, Error>; 
}

impl Generate for Node {
    fn generate(&self, buf: &mut ClockBuffer) -> Result<Operator, Error> {
        let mut op = Operator::Select( Box::new(Select::new(Table::new(&self.table).unwrap(), |_| true ) ));
        if !self.cols.is_empty() { op = Operator::Project( Box::new(Project::new(op, self.cols.clone())));};
        match &self.join {
            Some(v) => {
                Ok(Operator::Join(Box::new(Join::new(op, v.generate(buf)?, buf, v.pred.as_ref().unwrap())?)))
            },
            None => Ok(op)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{storage::folder::Folder, buffer::{tuple::{Table, DatumTypes, Operate, TupleOps, Datum}, ClockBuffer, Buffer}, compiler::ast::Node, operator::predicate::{Equal, Field, Predicate}};

    use super::Generate;


    #[test]
    fn test_generate() {
        let a = "a".to_string();
        let b = "b".to_string();
        let mut buf = ClockBuffer::new(10);
        Folder::create().unwrap();
        Table::create(a.clone(), vec![("id".into(), DatumTypes::Int)]).unwrap();
        Table::create(b.clone(), vec![("id".into(), DatumTypes::Int)]).unwrap();
        let mut t1 = Table::new("a").unwrap();
        let mut t2 = Table::new("b").unwrap();
        t1.add(&mut buf, vec![Datum::Int(10)]).unwrap();
        t2.add(&mut buf, vec![Datum::Int(10)]).unwrap();
        t1.add(&mut buf, vec![Datum::Int(20)]).unwrap();
        t2.add(&mut buf, vec![Datum::Int(20)]).unwrap();
        let b = Node { table: b.into(), cols: vec![], pred: Some(Predicate::Equal(Equal { l: Field { table: "a".into(), col: "id".into() }, r: Field { table: "b".into(), col: "id".into() }})), join: None};
        let a = Node { table: a.into(), cols: vec![], pred: None, join: Some(Box::new(b))};
        let mut op = a.generate(&mut buf).unwrap();
        assert_eq!(op.collect(&mut buf), vec![vec![Datum::Int(10), Datum::Int(10)], vec![Datum::Int(20), Datum::Int(20)]])
    }
}