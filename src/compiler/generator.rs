use std::sync::Arc;

use crate::{operator::{Select, Project, Join}, buffer::tuple::{Table, Operator, ClockBuffer}, error::Error};

use super::ast::Node;

pub trait Generate {
    fn generate(&self, buf: Arc<ClockBuffer>) -> Result<Box<dyn Operator>, Error>; 
}

impl Generate for Node {
    fn generate(&self, buf: Arc<ClockBuffer>) -> Result<Box<dyn Operator>, Error> {
        if self.cols.is_empty() {
            let op = Select::new(Table::new(&self.table).unwrap(), Arc::clone(&buf), |_| true ).into_iter();
            if self.join.is_some() {
                let v = self.join.as_ref().unwrap();
                return Ok(Box::new(Join::new(Box::new(op), v.generate(Arc::clone(&buf))?, buf, v.pred.as_ref().unwrap().clone()).into_iter()));
            }
            return Ok(Box::new(op.into_iter()));
        } else {
            let op = Select::new(Table::new(&self.table).unwrap(), Arc::clone(&buf), |_| true ).into_iter();
            let op = Project::new(op, Arc::clone(&buf), self.cols.clone()).into_iter();
            match &self.join {
                Some(v) => {
                    Ok(Box::new(Join::new(Box::new(op), v.generate(Arc::clone(&buf))?, buf, v.pred.as_ref().unwrap().clone()).into_iter()))
                },
                None => Ok(Box::new(op.into_iter()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{storage::folder::Folder, buffer::{tuple::{Table, DatumTypes, TupleOps, Datum, Tuple}, Buffer}, compiler::ast::Node, operator::predicate::{Equal, Field, Predicate}};

    use super::Generate;


    #[test]
    fn test_generate() {
        let a = "a".to_string();
        let b = "b".to_string();
        let buf = Arc::new(Buffer::new(10));
        Folder::create().unwrap();
        Table::create(a.clone(), vec![("id".into(), DatumTypes::Int)]).unwrap();
        Table::create(b.clone(), vec![("id".into(), DatumTypes::Int)]).unwrap();
        let mut t1 = Table::new("a").unwrap();
        let mut t2 = Table::new("b").unwrap();
        t1.add(Arc::clone(&buf), vec![Datum::Int(10)]).unwrap();
        t2.add(Arc::clone(&buf), vec![Datum::Int(10)]).unwrap();
        t1.add(Arc::clone(&buf), vec![Datum::Int(20)]).unwrap();
        t2.add(Arc::clone(&buf), vec![Datum::Int(20)]).unwrap();
        let b = Node { table: b.into(), cols: vec![], pred: Some(Predicate::Equal(Equal { l: Field { table: "a".into(), col: "id".into() }, r: Field { table: "b".into(), col: "id".into() }})), join: None};
        let a = Node { table: a.into(), cols: vec![], pred: None, join: Some(Box::new(b))};
        let op = a.generate(buf).unwrap();
        assert_eq!(op.collect::<Vec<Tuple>>(), vec![vec![Datum::Int(10), Datum::Int(10)], vec![Datum::Int(20), Datum::Int(20)]])
    }
}