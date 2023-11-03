use std::sync::Arc;

use crate::{operator::{Select, Project, Join}, buffer::tuple::{RowTable, Operator, PageBuffer}, error::Error, storage::folder::Folder};

use super::ast::Node;

pub trait Generate {
    fn generate(&self, buf: Arc<PageBuffer>, f: Arc<Folder>) -> Result<Box<dyn Operator>, Error>; 
}

impl Generate for Node {
    fn generate(&self, buf: Arc<PageBuffer>, f: Arc<Folder>) -> Result<Box<dyn Operator>, Error> {
        if self.cols.is_empty() {
            let op = Select::new(RowTable::new(Arc::clone(&f), &self.table).unwrap(), Arc::clone(&buf), |_| true ).into_iter();
            if self.join.is_some() {
                let v = self.join.as_ref().unwrap();
                return Ok(Box::new(Join::new(Box::new(op), v.generate(Arc::clone(&buf), Arc::clone(&f))?, buf, Arc::clone(&f), v.pred.as_ref().unwrap().clone()).into_iter()));
            }
            return Ok(Box::new(op.into_iter()));
        } else {
            let op = Select::new(RowTable::new(Arc::clone(&f), &self.table).unwrap(), Arc::clone(&buf), |_| true ).into_iter();
            let op = Project::new(op, Arc::clone(&buf), self.cols.clone()).into_iter();
            match &self.join {
                Some(v) => {
                    Ok(Box::new(Join::new(Box::new(op), v.generate(Arc::clone(&buf), Arc::clone(&f))?, buf, Arc::clone(&f), v.pred.as_ref().unwrap().clone()).into_iter()))
                },
                None => Ok(Box::new(op.into_iter()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{storage::folder::Folder, buffer::tuple::{RowTable, DatumTypes, TupleOps, Datum, Tuple, PageBuffer}, compiler::ast::Node, operator::predicate::{Equal, Field, Predicate}};

    use super::Generate;


    #[test]
    fn test_generate() {
        let a = "a".to_string();
        let b = "b".to_string();
        let c = "c".to_string();
        let buf = Arc::new(PageBuffer::new(10));
        Folder::create().unwrap();
        let f = Arc::new(Folder::new().unwrap());
        let mut t1 = RowTable::create(Arc::clone(&f), a.to_string(), vec![("id".into(), DatumTypes::Int)]).unwrap();
        let mut t2 = RowTable::create(Arc::clone(&f),  b.to_string(), vec![("id".into(), DatumTypes::Int)]).unwrap();
        let mut t3 = RowTable::create(Arc::clone(&f), c.to_string(), vec![("id".into(), DatumTypes::Int)]).unwrap();
        t1.add(Arc::clone(&buf), vec![Datum::Int(10)]).unwrap();
        t1.add(Arc::clone(&buf), vec![Datum::Int(20)]).unwrap();
        t2.add(Arc::clone(&buf), vec![Datum::Int(10)]).unwrap();
        t2.add(Arc::clone(&buf), vec![Datum::Int(20)]).unwrap();
        t3.add(Arc::clone(&buf), vec![Datum::Int(10)]).unwrap();
        t3.add(Arc::clone(&buf), vec![Datum::Int(20)]).unwrap();
        let c = Node { table: c.into(), cols: vec![], pred: Some(Predicate::Equal(Equal { l: Field { table: "b".into(), col: "id".into() }, r: Field { table: "c".into(), col: "id".into() }})), join: None };
        let b = Node { table: b.into(), cols: vec![], pred: Some(Predicate::Equal(Equal { l: Field { table: "a".into(), col: "id".into() }, r: Field { table: "c".into(), col: "id".into() }})), join: Some(Box::new(c))};
        let a = Node { table: a.into(), cols: vec![], pred: None, join: Some(Box::new(b))};
        let op = a.generate(buf, Arc::clone(&f)).unwrap();
        assert_eq!(op.collect::<Vec<Tuple>>(), vec![vec![Datum::Int(10), Datum::Int(10), Datum::Int(10)], vec![Datum::Int(20), Datum::Int(20), Datum::Int(20)]])
    }
}