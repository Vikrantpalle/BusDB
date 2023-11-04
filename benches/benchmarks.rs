use std::{fs::remove_file, sync::Arc};

use criterion::{Criterion, criterion_group, criterion_main};
use rustDB::{storage::{utils::create_file, utils::append_block, disk_manager::read_block, folder::Folder}, buffer::tuple::{RowTable, DatumTypes, Datum, TupleOps, PageBuffer, Table}, operator::Select};



pub fn block_read_benchmark(c: &mut Criterion) {
    let file_id = 20;
    create_file(&file_id.to_string()).expect("Could not create file");
    for _ in 0..20 {
        append_block(&file_id.to_string()).expect("Could not append block to file");
    }
    c.bench_function("read_block", |b| b.iter(|| {
        for i in 0..20 {
            read_block((file_id << 32) | i & 0xFFFFFFFF);
        }
    }));
    remove_file("C:/Users/vikra/rustDB/".to_owned() + &file_id.to_string()).expect("Could not delete benchmark file");
}

pub fn seq_scan_benchmark(c: &mut Criterion) {
        let t_id = "test".to_string();
        let f = Arc::new(Folder::new().unwrap());
        let mut t = RowTable::create(f, &t_id, vec![("a".into(), DatumTypes::Int), ("b".into(), DatumTypes::Int)]).unwrap();
        let buf = Arc::new(PageBuffer::new(1001));
        let mut tuple = vec![Datum::Int(10), Datum::Int(20)];
        for _ in 0..100000 {
            t.add(Arc::clone(&buf), tuple).unwrap();
            tuple = vec![Datum::Int(10), Datum::Int(10), Datum::Int(10), Datum::Int(10), Datum::Int(10), Datum::Int(10)];
        }

        c.bench_function("seq_scan", |b| b.iter(|| {
            let s = Select::new(t.clone(), Arc::clone(&buf), |_| true).into_iter();
            s.for_each(drop);
        }));
    
}

criterion_group!(benches, block_read_benchmark, seq_scan_benchmark);
criterion_main!(benches);