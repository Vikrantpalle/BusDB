#![allow(non_snake_case)]

use std::{io::Result, sync::{Mutex, Arc}};

use actix_web::{get, Responder, HttpResponse, HttpServer, App, web};
use rustDB::{compiler::parse, buffer::tuple::PageBuffer, State, storage::folder::Folder};

#[get("/query")]
async fn query(data: web::Data<State>, query: String) -> impl Responder {
    let r = parse(&query, Arc::clone(&data.buf.lock().unwrap()), Arc::clone(&data.folder.lock().unwrap()));
    match r {
        Ok(Some(t)) => HttpResponse::Ok().json(t),
        Ok(None) => HttpResponse::Ok().body("Success"),
        Err(_) => HttpResponse::Ok().body("Failed")
    }
}

#[actix_web::main]
async fn main() -> Result<()> {
    let state = web::Data::new(State { folder: Mutex::new(Arc::new(Folder::new().unwrap())), buf: Mutex::new(Arc::new(PageBuffer::new(10)))});
    HttpServer::new(move || {
        App::new()
            .app_data(state.clone())
            .service(query)
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}



