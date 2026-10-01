// A channel request that reaches lapin's internal RPC after the IO loop has stopped it is
// dropped with its resolver (`InternalRPCHandle::send` ignores the send failure, and a dropped
// `PromiseResolver` resolves nothing), so `create_channel` never returns.
//
// The window is between `ensure_connected` and the queuing of the command, a handful of
// instructions in a release. The `repro-create-channel-window` feature widens it with a sleep
// so a test can kill the connection inside it.

use lapin::{Connection, ConnectionProperties};
use std::{sync::Arc, time::Duration};
use tokio::net::{TcpListener, TcpStream};

type Forwarders = Arc<std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>>;

/// Forwards to the broker. Aborting the forwarders drops both of a connection's sockets
/// mid-stream, which is what the client sees as the connection dying.
async fn proxy(broker: String) -> (u16, Forwarders) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let forwarders: Forwarders = Default::default();
    let accepted_by = forwarders.clone();
    tokio::spawn(async move {
        while let Ok((mut downstream, _)) = listener.accept().await {
            let broker = broker.clone();
            let forwarder = tokio::spawn(async move {
                let Ok(mut upstream) = TcpStream::connect(broker).await else {
                    return;
                };
                let _ = tokio::io::copy_bidirectional(&mut downstream, &mut upstream).await;
            });
            accepted_by.lock().unwrap().push(forwarder);
        }
    });
    (port, forwarders)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_channel_request_whose_connection_dies_under_it_still_returns() {
    let broker = std::env::var("AMQP_PROXY_TARGET").unwrap_or_else(|_| "127.0.0.1:5679".into());
    let (port, forwarders) = proxy(broker).await;
    let uri = format!("amqp://guest:guest@127.0.0.1:{port}/%2f");
    let connection = Arc::new(
        Connection::connect(&uri, ConnectionProperties::default())
            .await
            .expect("the broker is reachable through the proxy"),
    );

    // A channel request that passes the status check, then sits in the widened window.
    let requester = {
        let connection = connection.clone();
        tokio::spawn(async move { connection.create_channel().await })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;

    // The connection dies while the request is in flight but before it is queued.
    for forwarder in forwarders.lock().unwrap().drain(..) {
        forwarder.abort();
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !connection.status().connected(),
        "the connection should be gone by now"
    );

    let channel = tokio::time::timeout(Duration::from_secs(5), requester)
        .await
        .expect("the request returned")
        .unwrap();
    assert!(
        channel.is_err(),
        "a channel cannot be opened on a dead connection"
    );
}

/// The control: the same teardown, after the request has been queued. The loop is still there
/// to answer it, so the wait ends.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_channel_request_queued_before_its_connection_dies_returns() {
    let broker = std::env::var("AMQP_PROXY_TARGET").unwrap_or_else(|_| "127.0.0.1:5679".into());
    let (port, forwarders) = proxy(broker).await;
    let uri = format!("amqp://guest:guest@127.0.0.1:{port}/%2f");
    let connection = Arc::new(
        Connection::connect(&uri, ConnectionProperties::default())
            .await
            .expect("the broker is reachable through the proxy"),
    );

    let requester = {
        let connection = connection.clone();
        tokio::spawn(async move { connection.create_channel().await })
    };
    // Past the widened window, so the command is queued while the loop is alive.
    tokio::time::sleep(Duration::from_millis(700)).await;

    for forwarder in forwarders.lock().unwrap().drain(..) {
        forwarder.abort();
    }

    tokio::time::timeout(Duration::from_secs(5), requester)
        .await
        .expect("the request returned")
        .unwrap()
        .expect("the channel was opened before the connection died");
}
