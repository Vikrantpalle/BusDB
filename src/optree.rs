use crate::buffer::{tuple::{TableIter, Tuple, Table, Schema, Operator}, ClockBuffer};

use crate::index::hash_table::{HashTable, Hash, HashIter};

pub struct Select {
    t: TableIter<Table>,
    pred: fn(&Tuple) -> bool
}

impl Select {
    pub fn new(t: Table, pred: fn(&Tuple) -> bool) -> Self {
        Select { t: t.iter(), pred }
    }
}

impl Operator for Select {

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

pub struct Project<T: Operator<Item = Tuple>> {
    t: T,
    cols: Vec<usize>
}

impl<T: Operator<Item = Tuple>> Project<T> {
    pub fn new(t: T, cols: Vec<String>) -> Self {
        let schema = t.get_schema();
        let cols = schema.iter().enumerate().filter(|(_, (col, _))| {
            cols.iter().find(|x| *x == col).is_some()
        }).map(|(i, _)| i).collect();
        Project { t, cols }
    }
}

impl<T: Operator<Item = Tuple>> Operator for Project<T> {

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

pub struct Join<R: Operator> {
    h: TableIter<HashTable>,
    cur_r: Option<Tuple>,
    r: R,
    r_hash: fn(&Tuple) -> u16,
}

impl<R: Operator<Item = Tuple>> Join<R> {
    pub fn new<L: Operator<Item = Tuple>>(mut l: L, r: R, buf: &mut ClockBuffer, l_hash: fn(&Tuple) -> u16, r_hash: fn(&Tuple) -> u16) -> Self {
        let h_id = 0;
        HashTable::create(h_id, l.get_schema());
        let mut h = HashTable::new(h_id);
        while let Some(t) = l.next(buf) {
            h.insert(buf, l_hash(&t), t);
        }
        Join { h: TableIter { block_num: None, tup_idx: 0, table: h, page: None, on_page_end: |i| {
            if !i.page.as_ref().unwrap().read().unwrap().has_next() { return true;}
            i.block_num = Some(i.page.as_ref().unwrap().read().unwrap().get_next().unwrap() as u64);
            i.tup_idx = 0;
            false 
        }  }, cur_r: None, r, r_hash }
    }
}

impl<R: Operator<Item = Tuple>> Operator for Join<R> {
    
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

    use crate::{buffer::{tuple::{Table, DatumTypes, Tuple, Datum, TupleOps, Operator}, ClockBuffer, Buffer}, optree::Project};

    use super::{Select, Join};

    #[test]
    fn test_select() {
        let t_id = "test_select".to_string();
        Table::create(t_id.clone(), vec![("a".into(), DatumTypes::Int), ("b".into(), DatumTypes::Int)]);
        let mut t = Table::new(t_id);
        let mut buf = ClockBuffer::new(1);
        let mut tuple;
        let mut res: Vec<Tuple> = Vec::new();
        for i in 0..100 {
            tuple = vec![Datum::Int(i), Datum::Int(i+2)];
            t.add(&mut buf, tuple.to_vec());
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
        Table::create(t_id.clone(), vec![("a".into(), DatumTypes::Int), ("b".into(), DatumTypes::Int)]);
        let mut t = Table::new(t_id);
        let mut buf = ClockBuffer::new(1);
        let mut tuple;
        let mut res: Vec<Tuple> = Vec::new();
        for i in 0..100 {
            tuple = vec![Datum::Int(i), Datum::Int(i+2)];
            t.add(&mut buf, tuple.to_vec());
            res.push(vec![tuple[0].clone()]);
        }
        let s_op = Select::new(t, |t| {
            match t[0] {
                Datum::Int(_) => true,
                _ => false
            }
        });
        let mut proj = Project::new(s_op, vec!["a".into()]);
        assert_eq!(proj.collect(&mut buf), res);
    }

    #[test]
    fn test_join() {
        let t_id = "test_join".to_string();
        Table::create(t_id.clone(), vec![("a".into(), DatumTypes::Int), ("b".into(), DatumTypes::Int)]);
        Table::create(t_id.clone()+"a", vec![("a".into(), DatumTypes::Int), ("b".into(), DatumTypes::Int)]);
        let mut t = Table::new(t_id.clone());
        let mut buf = ClockBuffer::new(10);
        let mut tuple;
        for i in 0..1 {
            tuple = vec![Datum::Int(i), Datum::Int(i+1)];
            t.add(&mut buf, tuple.to_vec());
        }

        let mut t2 = Table::new(t_id+"a");
        let mut tuple;
        for i in 0..1 {
            tuple = vec![Datum::Int(i), Datum::Int(i+1)];
            t2.add(&mut buf, tuple.to_vec());
        }
        let s1 = Select::new(t, |_| {true});
        let mut s_op = Join::new( 
            s1,
            Select::new(t2, |_| {true}),
            &mut buf,
            |x| match x[0]{
                Datum::Int(i) => i as u16,
                _ => 0
            },
            |x| match x[0]{
                Datum::Int(i) => i as u16,
                _ => 0
            }
        );
        assert_eq!(s_op.collect(&mut buf), vec![vec![Datum::Int(0),Datum::Int(1),Datum::Int(0),Datum::Int(1)]]);
    }
}

