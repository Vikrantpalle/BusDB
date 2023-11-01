use std::ptr;
use std::sync::Arc;

use crate::buffer::tuple::{TableIter, Tuple, Table, Schema, Operator, File, ClockBuffer};

use crate::index::hash_table::{HashTable, Hash, HashIter};

use self::predicate::Predicate;

pub mod predicate;

pub struct Select {
    t: Table,
    buf: Arc<ClockBuffer>,
    pred: fn(&Tuple) -> bool
}

impl IntoIterator for Select {
    type Item = Tuple;
    type IntoIter = SelectIter;

    fn into_iter(self) -> Self::IntoIter {
        let schema = self.get_schema();
        let iter = self.t.iter(Arc::clone(&self.buf));
        SelectIter { schema, iter, pred: self.pred }
    }
}

pub struct SelectIter {
    schema: Schema,
    iter: TableIter<Table>,
    pred: fn(&Tuple) -> bool
}

impl Select {
    pub fn new(t: Table, buf: Arc<ClockBuffer>, pred: fn(&Tuple) -> bool) -> Self {
        Select { t, buf, pred }
    }

    fn get_schema(&self) -> Schema {
        self.t.get_schema()
    }
}

impl Operator for SelectIter {
    fn get_schema(&self) -> Schema {
        self.schema.clone()
    }
}

impl Iterator for SelectIter {

    type Item = Tuple;

    fn next(&mut self) -> Option<Self::Item> {
        let t = self.iter.next();
        match t {
            Some(t) => {
                if (self.pred)(&t) {Some(t)} else {self.next()}
            },
            None => None
        }
    }
}

pub struct Project {
    t: Box<dyn Operator>,
    _buf: Arc<ClockBuffer>,
    cols: Vec<String>
}

pub struct ProjectIter {
    schema: Schema,
    iter: Box<dyn Operator>,
    cols: Vec<usize>
}

impl IntoIterator for Project {
    
    type Item = Tuple;
    type IntoIter = ProjectIter;

    fn into_iter(self) -> Self::IntoIter {
        let schema = self.t.get_schema();
        let cols = schema.iter().enumerate().filter(|(_, (col, _))| {
            self.cols.iter().find(|x| *x == col).is_some()
        }).map(|(i, _)| i).collect();
        let schema = self.get_schema();
        let iter = self.t;
        ProjectIter { schema, iter, cols }
    }
}

impl Project {
    pub fn new(t: impl Operator + 'static, buf: Arc<ClockBuffer>, cols: Vec<String>) -> Self {
        Project { t: Box::new(t), _buf: buf, cols }
    }

    fn get_schema(&self) -> Schema {
        self.t.get_schema().into_iter().filter(|(col, _)| self.cols.iter().any(|x| x == col)).collect()
    }
}

impl Operator for ProjectIter {
    fn get_schema(&self) -> Schema {
        self.schema.clone()
    }
}


impl Iterator for ProjectIter {

    type Item = Tuple;

    fn next(&mut self) -> Option<Self::Item> {
        let tup = self.iter.next();
        match tup {
            Some(t) => {
                Some(self.cols.iter().map(|i| t[*i].clone()).collect())
            },
            None => None
        } 
    }
}

pub struct Join {
    l: Box<dyn Operator>,
    r: Box<dyn Operator>,
    buf: Arc<ClockBuffer>,
    pred: Predicate
}

pub struct JoinIter {
    schema: Schema,
    h: TableIter<HashTable>,
    cur_r: Option<Tuple>,
    r: Box<dyn Operator>,
    r_hash: Box<dyn Fn(&Tuple) -> u16>
}

impl Join {
    pub fn new(l: Box<dyn Operator>, r: Box<dyn Operator>, buf: Arc<ClockBuffer>, pred: Predicate) -> Self {
        Join { l, r, buf, pred }
    }

    fn get_schema(&self) -> Schema {
        let mut schema = self.l.get_schema();
        schema.append(&mut self.r.get_schema());
        schema
    }
}

impl IntoIterator for Join {
    
    type Item = Tuple;
    type IntoIter = JoinIter;

    fn into_iter(mut self) -> Self::IntoIter {
        let schema = self.get_schema();
        // ! remove hardcode
        let h_id = 10;
        HashTable::create(h_id, self.l.get_schema());
        let mut h = HashTable::new(h_id);
        let (l_hash, r_hash) = self.pred.generate_hashes().unwrap();
        while let Some(t) = self.l.next() {
            h.insert( l_hash(&t), t, Arc::clone(&self.buf)).unwrap();
        }
        JoinIter { schema, h: TableIter { block_num: None, buf: Arc::clone(&self.buf), tup_idx: 0, table: h, page: ptr::null_mut(), on_page_end: |i| {
            let page = unsafe { i.page.as_ref().unwrap().read().unwrap() };
            if !page.has_next() { return true;}
            i.block_num = Some(page.get_next().unwrap() as u64);
            drop(page);
            i.tup_idx = 0;
            false 
        }  }, cur_r: None, r: self.r, r_hash: Box::new(r_hash) }
    }
}

impl Operator for JoinIter {
    fn get_schema(&self) -> Schema {
        self.schema.clone()
    }
}

impl Iterator for JoinIter {
    
    type Item = Tuple;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cur_r == None { 
            self.cur_r = self.r.next(); 
            if self.cur_r == None {
                return None;
            } else {
                self.h.swap_key((self.r_hash)(self.cur_r.as_ref().unwrap()));
            }
        }
        let Some(mut cur_l) = self.h.next() else {
            self.cur_r = None;
            return self.next();
        };

        cur_l.extend_from_slice(&self.cur_r.as_ref().unwrap());
        Some(cur_l)
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use crate::{buffer::{tuple::{Table, DatumTypes, Tuple, Datum, TupleOps}, Buffer}, operator::{Project, predicate::{Predicate, Equal, Field}}};

    use super::{Select, Join};

    #[test]
    fn test_select() {
        let t_id = "test_select".to_string();
        Table::create(t_id.clone(), vec![("a".into(), DatumTypes::Int), ("b".into(), DatumTypes::Int)]).unwrap();
        let mut t = Table::new(&t_id).unwrap();
        let buf = Arc::new(Buffer::new(1));
        let mut tuple;
        let mut res: Vec<Tuple> = Vec::new();
        for i in 0..100 {
            tuple = vec![Datum::Int(i), Datum::Int(i+2)];
            t.add(Arc::clone(&buf), tuple.to_vec()).unwrap();
            res.push(tuple);
        }
        let s_op = Select::new(t, buf, |t| {
            match t[0] {
                Datum::Int(_) => true,
                _ => false
            }
        }).into_iter();
        assert_eq!(s_op.collect::<Vec<Vec<Datum>>>(), res);
    }

    #[test]
    fn test_project() {
        let t_id = "test_project".to_string();
        Table::create(t_id.clone(), vec![("a".into(), DatumTypes::Int), ("b".into(), DatumTypes::Int)]).unwrap();
        let mut t = Table::new(&t_id).unwrap();
        let buf = Arc::new(Buffer::new(1));
        let mut tuple;
        let mut res: Vec<Tuple> = Vec::new();
        for i in 0..100 {
            tuple = vec![Datum::Int(i), Datum::Int(i+2)];
            t.add(Arc::clone(&buf), tuple.to_vec()).unwrap();
            res.push(vec![tuple[0].clone()]);
        }
        let s_op = Select::new(t, Arc::clone(&buf), |t| {
            match t[0] {
                Datum::Int(_) => true,
                _ => false
            }
        }).into_iter();
        let proj = Project::new(s_op, buf, vec![t_id + "." + "a"]).into_iter();
        assert_eq!(proj.collect::<Vec<Vec<Datum>>>(), res);
    }

    #[test]
    fn test_join() {
        let t_id = "test_join".to_string();
        Table::create(t_id.clone(), vec![("a".into(), DatumTypes::Int), ("b".into(), DatumTypes::Int)]).unwrap();
        Table::create(t_id.clone()+"a", vec![("a".into(), DatumTypes::Int), ("b".into(), DatumTypes::Int)]).unwrap();
        let mut t = Table::new(&t_id).unwrap();
        let buf = Arc::new(Buffer::new(10));
        let mut tuple;
        for i in 0..1 {
            tuple = vec![Datum::Int(i), Datum::Int(i+1)];
            t.add(Arc::clone(&buf), tuple.to_vec()).unwrap();
        }

        let mut t2 = Table::new(&(t_id.clone()+"a")).unwrap();
        let mut tuple;
        for i in 0..1 {
            tuple = vec![Datum::Int(i), Datum::Int(i+1)];
            t2.add(Arc::clone(&buf), tuple.to_vec()).unwrap();
        }
        let s1 = Select::new(t, Arc::clone(&buf), |_| {true}).into_iter();
        let s_op = Join::new( 
            Box::new(s1),
            Box::new(Select::new(t2, Arc::clone(&buf), |_| {true}).into_iter()),
            buf,
            Predicate::Equal(Equal::new(Field::new(&t_id, "a"), Field::new(&(t_id.clone()+"a"), "a")))
        ).into_iter();
        assert_eq!(s_op.collect::<Vec<Vec<Datum>>>(), vec![vec![Datum::Int(0),Datum::Int(1),Datum::Int(0),Datum::Int(1)]]);
    }
}

