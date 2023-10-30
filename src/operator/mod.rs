use crate::{buffer::{tuple::{TableIter, Tuple, Table, Schema, Operate}, ClockBuffer}, error::Error};

use crate::index::hash_table::{HashTable, Hash, HashIter};

use self::predicate::Predicate;

pub mod predicate;

pub enum Operator {
    Select(Box<Select>),
    Project(Box<Project>),
    Join(Box<Join>)
}

// todo: write macro
impl Operate for Operator {

    type Item = Tuple;

    fn next(&mut self, p_buf: &mut ClockBuffer) -> Option<Self::Item> {
        match self {
            Self::Select(s) => s.next(p_buf),
            Self::Project(p) => p.next(p_buf),
            Self::Join(j) => j.next(p_buf)
        }
    }

    fn get_schema(&self) -> Schema {
        match self {
            Self::Select(s) => s.get_schema(),
            Self::Project(p) => p.get_schema(),
            Self::Join(j) => j.get_schema()
        }
    }
}

pub struct Select {
    t: TableIter<Table>,
    pred: fn(&Tuple) -> bool
}

impl Select {
    pub fn new(t: Table, pred: fn(&Tuple) -> bool) -> Self {
        Select { t: t.iter(), pred }
    }
}

impl Operate for Select {

    type Item = Tuple;

    fn next(&mut self, p_buf: &mut ClockBuffer) -> Option<Self::Item> {
        let t = self.t.next(p_buf);
        match t {
            Some(t) => {
                if (self.pred)(&t) {Some(t)} else {self.next(p_buf)}
            },
            None => None
        }
    }

    fn get_schema(&self) -> Schema {
        self.t.get_schema()
    }
}

pub struct Project {
    t: Operator,
    cols: Vec<usize>
}

impl Project {
    pub fn new(t: Operator, cols: Vec<String>) -> Self {
        let schema = t.get_schema();
        let cols = schema.iter().enumerate().filter(|(_, (col, _))| {
            cols.iter().find(|x| *x == col).is_some()
        }).map(|(i, _)| i).collect();
        Project { t, cols }
    }
}

impl Operate for Project {

    type Item = Tuple;

    fn next(&mut self, p_buf: &mut ClockBuffer) -> Option<Self::Item> {
        let tup = self.t.next(p_buf);
        match tup {
            Some(t) => {
                Some(self.cols.iter().map(|i| t[*i].clone()).collect())
            },
            None => None
        } 
    }

    fn get_schema(&self) -> Schema {
        self.cols.iter().map(|i| self.t.get_schema()[*i].clone()).collect()
    }
}

pub struct Join {
    h: TableIter<HashTable>,
    cur_r: Option<Tuple>,
    r: Operator,
    r_hash: Box<dyn Fn(&Tuple) -> u16>
}

impl Join {
    pub fn new(mut l: Operator, r: Operator, buf: &mut ClockBuffer, pred: &Predicate) -> Result<Self, Error> {
        // ! remove hardcode
        let h_id = 10;
        HashTable::create(h_id, l.get_schema());
        let mut h = HashTable::new(h_id);
        let (l_hash, r_hash) = pred.generate_hashes()?;
        while let Some(t) = l.next(buf) {
            h.insert(buf, l_hash(&t), t)?;
        }
        Ok(Join { h: TableIter { block_num: None, tup_idx: 0, table: h, page: None, on_page_end: |i| {
            if !i.page.as_ref().unwrap().read().unwrap().has_next() { return true;}
            i.block_num = Some(i.page.as_ref().unwrap().read().unwrap().get_next().unwrap() as u64);
            i.tup_idx = 0;
            false 
        }  }, cur_r: None, r, r_hash: Box::new(r_hash) })
    }
}

impl Operate for Join {
    
    type Item = Tuple;

    fn next(&mut self, p_buf: &mut ClockBuffer) -> Option<Self::Item> {
        if self.cur_r == None { 
            self.cur_r = self.r.next(p_buf); 
            if self.cur_r == None {
                return None;
            } else {
                self.h.swap_key((self.r_hash)(self.cur_r.as_ref().unwrap()));
            }
        }
        let Some(mut cur_l) = self.h.next(p_buf) else {
            self.cur_r = None;
            return self.next(p_buf);
        };

        cur_l.extend_from_slice(&self.cur_r.as_ref().unwrap());
        Some(cur_l)
    }

    fn get_schema(&self) -> Schema {
        let mut schema = self.h.get_schema();
        schema.append(&mut self.r.get_schema());
        schema
    }
}

#[cfg(test)]
mod tests {

    use crate::{buffer::{tuple::{Table, DatumTypes, Tuple, Datum, TupleOps, Operate}, ClockBuffer, Buffer}, operator::{Project, Operator, predicate::{Predicate, Equal, Field}}};

    use super::{Select, Join};

    #[test]
    fn test_select() {
        let t_id = "test_select".to_string();
        Table::create(t_id.clone(), vec![("a".into(), DatumTypes::Int), ("b".into(), DatumTypes::Int)]).unwrap();
        let mut t = Table::new(&t_id).unwrap();
        let mut buf = ClockBuffer::new(1);
        let mut tuple;
        let mut res: Vec<Tuple> = Vec::new();
        for i in 0..100 {
            tuple = vec![Datum::Int(i), Datum::Int(i+2)];
            t.add(&mut buf, tuple.to_vec()).unwrap();
            res.push(tuple);
        }
        let mut s_op = Select::new(t, |t| {
            match t[0] {
                Datum::Int(_) => true,
                _ => false
            }
        });
        assert_eq!(s_op.collect(&mut buf), res);
    }

    #[test]
    fn test_project() {
        let t_id = "test_project".to_string();
        Table::create(t_id.clone(), vec![("a".into(), DatumTypes::Int), ("b".into(), DatumTypes::Int)]).unwrap();
        let mut t = Table::new(&t_id).unwrap();
        let mut buf = ClockBuffer::new(1);
        let mut tuple;
        let mut res: Vec<Tuple> = Vec::new();
        for i in 0..100 {
            tuple = vec![Datum::Int(i), Datum::Int(i+2)];
            t.add(&mut buf, tuple.to_vec()).unwrap();
            res.push(vec![tuple[0].clone()]);
        }
        let s_op = Operator::Select(Box::new(Select::new(t, |t| {
            match t[0] {
                Datum::Int(_) => true,
                _ => false
            }
        })));
        let mut proj = Project::new(s_op, vec!["a".into()]);
        assert_eq!(proj.collect(&mut buf), res);
    }

    #[test]
    fn test_join() {
        let t_id = "test_join".to_string();
        Table::create(t_id.clone(), vec![("a".into(), DatumTypes::Int), ("b".into(), DatumTypes::Int)]).unwrap();
        Table::create(t_id.clone()+"a", vec![("a".into(), DatumTypes::Int), ("b".into(), DatumTypes::Int)]).unwrap();
        let mut t = Table::new(&t_id).unwrap();
        let mut buf = ClockBuffer::new(10);
        let mut tuple;
        for i in 0..1 {
            tuple = vec![Datum::Int(i), Datum::Int(i+1)];
            t.add(&mut buf, tuple.to_vec()).unwrap();
        }

        let mut t2 = Table::new(&(t_id.clone()+"a")).unwrap();
        let mut tuple;
        for i in 0..1 {
            tuple = vec![Datum::Int(i), Datum::Int(i+1)];
            t2.add(&mut buf, tuple.to_vec()).unwrap();
        }
        let s1 = Operator::Select(Box::new(Select::new(t, |_| {true})));
        let mut s_op = Join::new( 
            s1,
            Operator::Select(Box::new(Select::new(t2, |_| {true}))),
            &mut buf,
            &Predicate::Equal(Equal::new(Field::new(&t_id, "a"), Field::new(&(t_id.clone()+"a"), "a")))
        ).unwrap();
        assert_eq!(s_op.collect(&mut buf), vec![vec![Datum::Int(0),Datum::Int(1),Datum::Int(0),Datum::Int(1)]]);
    }
}

