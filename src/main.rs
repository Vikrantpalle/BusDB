#![allow(non_snake_case)]

use std::{io::Result, sync::Mutex};

use actix_web::{get, Responder, HttpResponse, HttpServer, App, web};
use rustDB::{parser::parse, buffer::{ClockBuffer, Buffer}};

struct AppState {
    buf: Mutex<ClockBuffer>
}

#[get("/query")]
async fn query(data: web::Data<AppState>, query: String) -> impl Responder {
    let r = parse(&query, &mut data.buf.lock().unwrap());
    match r {
        Ok(Some(t)) => HttpResponse::Ok().json(t),
        Ok(None) => HttpResponse::Ok().body("Success"),
        Err(_) => HttpResponse::Ok().body("Failed")
    }
}

#[actix_web::main]
async fn main() -> Result<()> {
    let state = web::Data::new(AppState { buf: Mutex::new(ClockBuffer::new(10))});
    HttpServer::new(move || {
        App::new()
            .app_data(state.clone())
            .service(query)
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}



