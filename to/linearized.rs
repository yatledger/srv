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