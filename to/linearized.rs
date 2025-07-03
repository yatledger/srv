fn lin () {
    let ret = app.raft.get_read_linearizer(ReadPolicy::ReadIndex).await;
    let mut nodes;
    match ret {
        Ok(linearizer) => {
            linearizer.await_ready(&app.raft).await.unwrap();

            let state_machine = app.state_machine.state_machine.lock().unwrap();
            nodes = state_machine.data.get_weights().clone();
            
        }
        Err(e) => {
            // Другие ошибки Raft
            tracing::error!("Unexpected Raft error: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(PoolResponse {
                    status: "error".to_string(),
                    nodes: vec![],
                    message: Some(format!("Raft error: {}", e)),
                }),
            )
        }
    };
}

#[axum_macros::debug_handler]
pub async fn unified_handler(
    State(mut app): State<App>,
    axum::extract::OriginalUri(uri): axum::extract::OriginalUri,
    body: Bytes,
) -> StatusCode {
    let path = uri.path().to_string();
    let payload = String::from_utf8_lossy(&body).to_string();
    info!("GET {} with {}", path.clone(), payload.clone());

    match path.as_str() {
        "/app/write" => api::write(&mut app, payload).await,
        "/app/read" => api::read(&mut app, payload).await,

        "/raft/append" => api::append(&mut app, payload).await,
        "/raft/snapshot" => api::snapshot(&mut app, payload).await,
        "/raft/vote" => api::vote(&mut app, payload).await,

        "/mng/change-membership" => api::change_membership(&mut app, payload).await,
        "/mng/init" => api::init(&mut app).await,
        "/mng/metrics" => api::metrics(&mut app).await,

        _ => return StatusCode::NOT_FOUND,
    };

    StatusCode::OK
}
