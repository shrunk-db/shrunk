//! A socket holds an admission permit until its HTTP connection is dropped.

use std::future::Future;
use std::pin::pin;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use hyper::server::conn::http1;
use hyper_util::rt::{TokioIo, TokioTimer};
use hyper_util::service::TowerToHyperService;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{watch, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tokio::time::{sleep, sleep_until, Instant};

use crate::admission::Metrics;

const HEADER_TIMEOUT: Duration = Duration::from_secs(10);
const CONNECTION_LIFETIME: Duration = Duration::from_secs(60);

pub(super) struct LimitedListener {
    listener: TcpListener,
    permits: Arc<Semaphore>,
    metrics: Arc<Metrics>,
}

impl LimitedListener {
    pub fn new(listener: TcpListener, maximum: usize, metrics: Arc<Metrics>) -> Self {
        Self {
            listener,
            permits: Arc::new(Semaphore::new(maximum)),
            metrics,
        }
    }

    pub async fn serve(self, router: Router, shutdown: impl Future<Output = ()>) {
        let http = http1_builder();
        let service = TowerToHyperService::new(router.with_state(()));
        let (stop, stopped) = watch::channel(());
        let mut connections = JoinSet::new();
        let mut shutdown = pin!(shutdown);

        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => break,
                _ = connections.join_next(), if !connections.is_empty() => {},
                (stream, permit) = self.accept() => {
                    let deadline = Instant::now() + CONNECTION_LIFETIME;
                    let connection = http.serve_connection(TokioIo::new(stream), service.clone());
                    let mut stopped = stopped.clone();
                    connections.spawn(async move {
                        let _permit = permit;
                        let mut connection = pin!(connection);
                        let mut expiry = pin!(sleep_until(deadline));
                        let mut draining = false;
                        loop {
                            tokio::select! {
                                biased;
                                // This deadline also bounds stalled bodies and writes, even
                                // while Hyper isn't waiting for the next request header.
                                _ = &mut expiry => break,
                                _ = stopped.changed(), if !draining => {
                                    draining = true;
                                    connection.as_mut().graceful_shutdown();
                                }
                                _ = &mut connection => break,
                            }
                        }
                    });
                }
            }
        }

        drop(self.listener);
        drop(stop);
        while connections.join_next().await.is_some() {}
    }

    async fn accept(&self) -> (TcpStream, ConnectionPermit) {
        loop {
            match self.listener.accept().await {
                Ok((stream, _)) => {
                    let Ok(permit) = self.permits.clone().try_acquire_owned() else {
                        self.metrics
                            .connection_rejections
                            .fetch_add(1, Ordering::Relaxed);
                        drop(stream);
                        continue;
                    };
                    self.metrics
                        .active_connections
                        .fetch_add(1, Ordering::Relaxed);
                    return (
                        stream,
                        ConnectionPermit {
                            _permit: permit,
                            metrics: self.metrics.clone(),
                        },
                    );
                }
                Err(error) => {
                    eprintln!("accept failed: {error}");
                    sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }
}

fn http1_builder() -> http1::Builder {
    let mut http = http1::Builder::new();
    http.timer(TokioTimer::new())
        .header_read_timeout(HEADER_TIMEOUT);
    http
}

struct ConnectionPermit {
    _permit: OwnedSemaphorePermit,
    metrics: Arc<Metrics>,
}

impl Drop for ConnectionPermit {
    fn drop(&mut self) {
        self.metrics
            .active_connections
            .fetch_sub(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::get;
    use std::io;
    use tokio::io::{duplex, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
    use tokio::sync::{oneshot, Notify};

    async fn small_response(stream: &mut BufReader<TcpStream>) {
        stream
            .get_mut()
            .write_all(b"GET /small HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut status = String::new();
        stream.read_line(&mut status).await.unwrap();
        assert_eq!(status, "HTTP/1.1 200 OK\r\n");
        loop {
            let mut line = String::new();
            assert_ne!(stream.read_line(&mut line).await.unwrap(), 0);
            if line == "\r\n" {
                break;
            }
        }
        let mut body = [0; 2];
        stream.read_exact(&mut body).await.unwrap();
        assert_eq!(&body, b"ok");
    }

    async fn count(metrics: &Metrics, expected: usize) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while metrics.active_connections.load(Ordering::Relaxed) != expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    async fn expect_closed(stream: &mut TcpStream) {
        let mut byte = [0];
        match tokio::time::timeout(Duration::from_secs(5), stream.read(&mut byte))
            .await
            .unwrap()
        {
            Ok(0) => {}
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::ConnectionReset | io::ErrorKind::BrokenPipe
                ) => {}
            result => panic!("expected connection to close: {result:?}"),
        }
    }

    async fn elapse(duration: Duration) {
        tokio::time::pause();
        tokio::task::yield_now().await;
        tokio::time::advance(duration).await;
        tokio::time::resume();
    }

    #[tokio::test(start_paused = true)]
    async fn additional_header_bytes_do_not_extend_the_deadline() {
        // A one-byte pipe makes each write wait for the server to read, so
        // both fragments are delivered on the controlled clock.
        let (mut client, server) = duplex(1);
        let router = Router::new().route("/small", get(|| async { "ok" }));
        let connection = tokio::spawn(
            http1_builder()
                .serve_connection(TokioIo::new(server), TowerToHyperService::new(router)),
        );
        client.write_all(b"GET /small HTTP/1.1\r\n").await.unwrap();
        sleep(HEADER_TIMEOUT / 2).await;
        client.write_all(b"Host: localhost\r\n").await.unwrap();

        let error = tokio::time::timeout(HEADER_TIMEOUT / 2 + Duration::from_secs(1), connection)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(error.is_timeout());
    }

    #[tokio::test]
    async fn idle_new_socket_releases_its_permit_at_the_header_deadline() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let metrics = Arc::new(Metrics::default());
        let limited = LimitedListener::new(listener, 1, metrics.clone());
        let router = Router::new().route("/small", get(|| async { "ok" }));
        let (stop, shutdown) = oneshot::channel();
        let server = tokio::spawn(limited.serve(router, async {
            let _ = shutdown.await;
        }));

        let mut idle = TcpStream::connect(address).await.unwrap();
        count(&metrics, 1).await;
        elapse(HEADER_TIMEOUT + Duration::from_secs(1)).await;
        expect_closed(&mut idle).await;
        count(&metrics, 0).await;

        let mut replacement = BufReader::new(TcpStream::connect(address).await.unwrap());
        small_response(&mut replacement).await;
        stop.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .unwrap()
            .unwrap();
        count(&metrics, 0).await;
    }

    #[tokio::test]
    async fn idle_keepalive_and_slow_response_release_socket_permits_at_deadlines() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let metrics = Arc::new(Metrics::default());
        let limited = LimitedListener::new(listener, 2, metrics.clone());
        let started = Arc::new(Notify::new());
        let router = Router::new().route("/small", get(|| async { "ok" })).route(
            "/large",
            get({
                let started = started.clone();
                move || {
                    let started = started.clone();
                    async move {
                        // Exceeds the loopback send buffer: the handler completes,
                        // while its non-reading client's response remains in flight.
                        let body = vec![b'x'; 32 * 1024 * 1024];
                        started.notify_one();
                        body
                    }
                }
            }),
        );
        let (stop, shutdown) = oneshot::channel();
        let server = tokio::spawn(async move {
            limited
                .serve(router, async {
                    let _ = shutdown.await;
                })
                .await;
        });
        let mut slow = TcpStream::connect(address).await.unwrap();
        slow.write_all(b"GET /large HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        started.notified().await;
        let mut keepalive = BufReader::new(TcpStream::connect(address).await.unwrap());
        small_response(&mut keepalive).await;
        count(&metrics, 2).await;
        let mut overflow = TcpStream::connect(address).await.unwrap();
        let _ = overflow
            .write_all(b"GET /small HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await;
        expect_closed(&mut overflow).await;
        assert_eq!(metrics.connection_rejections.load(Ordering::Relaxed), 1);
        // The same socket can issue another request without a second permit.
        small_response(&mut keepalive).await;
        count(&metrics, 2).await;

        elapse(HEADER_TIMEOUT + Duration::from_secs(1)).await;
        expect_closed(keepalive.get_mut()).await;
        count(&metrics, 1).await;

        elapse(CONNECTION_LIFETIME - HEADER_TIMEOUT).await;
        count(&metrics, 0).await;

        let mut replacement = BufReader::new(TcpStream::connect(address).await.unwrap());
        small_response(&mut replacement).await;
        drop(replacement);
        stop.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .unwrap()
            .unwrap();
        count(&metrics, 0).await;
    }

    #[tokio::test]
    async fn timed_out_running_mutation_drains_on_shutdown_and_reopens() {
        use crate::test_support::TestStore;
        use axum::routing::post;
        use shrunk::{Point, Query};
        use std::sync::Mutex;

        let database = TestStore::new();
        let (executor, worker) = database.executor(2);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let metrics = executor.metrics();
        let limited = LimitedListener::new(listener, 2, metrics.clone());
        let (release, wait) = std::sync::mpsc::channel();
        let wait = Arc::new(Mutex::new(Some(wait)));
        let started = Arc::new(Notify::new());
        let router = Router::new().route(
            "/mutate",
            post({
                let executor = executor.clone();
                let started = started.clone();
                move || {
                    let executor = executor.clone();
                    let started = started.clone();
                    let wait = wait.clone();
                    async move {
                        executor
                            .run(move |store| {
                                started.notify_one();
                                wait.lock().unwrap().take().unwrap().recv().unwrap();
                                store.db.append("disconnected", &[Point::new(1, 9.0)])?;
                                Ok(serde_json::json!({"written": 1}))
                            })
                            .await
                            .map(axum::Json)
                    }
                }
            }),
        );
        let (stop, shutdown) = oneshot::channel();
        let server = tokio::spawn({
            let executor = executor.clone();
            async move {
                limited
                    .serve(router, async move {
                        let _ = shutdown.await;
                        executor.close();
                    })
                    .await;
            }
        });
        let mut client = TcpStream::connect(address).await.unwrap();
        client
            .write_all(b"POST /mutate HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
        started.notified().await;
        elapse(CONNECTION_LIFETIME + Duration::from_secs(1)).await;
        expect_closed(&mut client).await;
        count(&metrics, 0).await;
        stop.send(()).unwrap();
        // Finish closes admission too, and must wait for this accepted write.
        let drained = tokio::spawn(worker.finish());
        tokio::task::yield_now().await;
        assert!(!drained.is_finished());
        release.send(()).unwrap();
        drained.await.unwrap().unwrap();
        tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .unwrap()
            .unwrap();
        let db = shrunk::Db::open_rw(&database.0.join("shrunk.db")).unwrap();
        assert_eq!(
            db.query(&Query::new("disconnected", i64::MIN, i64::MAX))
                .unwrap()
                .points,
            vec![Point::new(1, 9.0)]
        );
    }
}
