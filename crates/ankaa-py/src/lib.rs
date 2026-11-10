//! Python bindings for ANKAA (`ankaa._native`).
//!
//! - [`connect`] / `Client`: Arrow Flight client against `ankaad serve` (SQL text in a CMD
//!   descriptor, `DoGet` with the returned ticket, bearer token in `authorization`).
//! - [`open_capture`] / `Dataset`: in-process `DataFusion` over a `.ankc` capture, the same path
//!   as `ankaad replay`.
//!
//! Results cross into Python through the Arrow `PyCapsule` interface (`__arrow_c_stream__`,
//! `__arrow_c_schema__`) as `arro3`-style objects provided by `pyo3-arrow`; the pure-Python
//! wrapper turns them into `pyarrow.Table` when pyarrow is installed. No row is copied on the
//! way out: the capsule hands over the Arrow buffers.
//!
//! The GIL is released (`Python::detach`) while the network, the decoders and the query engine
//! run, so Python threads can issue queries concurrently.
#![forbid(unsafe_code)]

mod capture;
mod tables;

use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{Criteria, FlightDescriptor, Ticket};
use futures::TryStreamExt;
use pyo3::exceptions::{PyConnectionError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3_arrow::{PySchema, PyTable};
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;

const VERSION: &str = env!("CARGO_PKG_VERSION");

fn runtime() -> PyResult<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("ankaa-py")
        .enable_all()
        .build()
        .map_err(|e| PyRuntimeError::new_err(format!("tokio runtime: {e}")))
}

fn status_err(e: &tonic::Status) -> PyErr {
    match e.code() {
        tonic::Code::InvalidArgument => PyValueError::new_err(e.message().to_owned()),
        tonic::Code::Unauthenticated => PyConnectionError::new_err(format!("unauthenticated: {}", e.message())),
        tonic::Code::Unavailable => PyConnectionError::new_err(format!("unavailable: {}", e.message())),
        _ => PyRuntimeError::new_err(format!("{}: {}", e.code(), e.message())),
    }
}

fn to_table(batches: Vec<RecordBatch>, schema: SchemaRef) -> PyResult<PyTable> {
    PyTable::try_new(batches, schema).map_err(|e| PyRuntimeError::new_err(e.to_string()))
}

// ---------------------------------------------------------------------------------------------
// Dataset: in-process DataFusion over a capture file

/// An in-memory dataset built from one `.ankc` capture.
#[pyclass(module = "ankaa._native", frozen)]
pub struct Dataset {
    rt: tokio::runtime::Runtime,
    ctx: datafusion::prelude::SessionContext,
    stats: capture::CaptureStats,
    path: String,
}

impl std::fmt::Debug for Dataset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Dataset").field("path", &self.path).finish_non_exhaustive()
    }
}

#[pymethods]
impl Dataset {
    /// Run a SQL query and return the result as an Arrow table (`PyCapsule` interface).
    fn sql(&self, py: Python<'_>, query: &str) -> PyResult<PyTable> {
        let ctx = self.ctx.clone();
        let q = query.to_owned();
        let out: Result<(Vec<RecordBatch>, SchemaRef), String> = py.detach(|| {
            self.rt.block_on(async move {
                let df = ctx.sql(&q).await.map_err(|e| format!("sql: {e}"))?;
                let schema: SchemaRef = Arc::new(df.schema().as_arrow().clone());
                let batches = df.collect().await.map_err(|e| format!("execute: {e}"))?;
                Ok((batches, schema))
            })
        });
        let (batches, schema) = out.map_err(PyValueError::new_err)?;
        to_table(batches, schema)
    }

    /// Result schema of `query` without executing it.
    fn schema(&self, py: Python<'_>, query: &str) -> PyResult<PySchema> {
        let ctx = self.ctx.clone();
        let q = query.to_owned();
        let out: Result<SchemaRef, String> = py.detach(|| {
            self.rt.block_on(async move {
                let df = ctx.sql(&q).await.map_err(|e| format!("sql: {e}"))?;
                Ok(Arc::new(df.schema().as_arrow().clone()))
            })
        });
        Ok(PySchema::new(out.map_err(PyValueError::new_err)?))
    }

    /// Registered table names.
    fn tables(&self) -> Vec<String> {
        capture::table_names(&self.ctx)
    }

    /// Decode counters: frames, events, trades, deltas, gaps, snapshots, dropped, ...
    fn stats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let s = self.stats;
        let d = PyDict::new(py);
        d.set_item("frames", s.frames)?;
        d.set_item("events", s.events)?;
        d.set_item("trades", s.trades)?;
        d.set_item("deltas", s.deltas)?;
        d.set_item("gaps", s.gaps)?;
        d.set_item("snapshots", s.snapshots)?;
        d.set_item("dropped", s.dropped)?;
        d.set_item("rejected_frames", s.rejected_frames)?;
        d.set_item("symbols", s.symbols)?;
        d.set_item("resyncs", s.resyncs)?;
        d.set_item("crossed_messages", s.crossed_messages)?;
        Ok(d)
    }

    #[getter]
    fn path(&self) -> &str {
        &self.path
    }

    fn __repr__(&self) -> String {
        format!(
            "Dataset(path={:?}, trades={}, deltas={}, tables={:?})",
            self.path,
            self.stats.trades,
            self.stats.deltas,
            self.tables()
        )
    }
}

/// Decode a `.ankc` capture into in-memory tables and return a `Dataset`.
#[pyfunction]
#[pyo3(signature = (path, expect_snapshots = false))]
fn open_capture(py: Python<'_>, path: &str, expect_snapshots: bool) -> PyResult<Dataset> {
    let rt = runtime()?;
    let p = std::path::PathBuf::from(path);
    let loaded = py.detach(|| capture::load(&p, expect_snapshots));
    let (ctx, stats) = loaded.map_err(PyValueError::new_err)?;
    Ok(Dataset {
        rt,
        ctx,
        stats,
        path: path.to_owned(),
    })
}

// ---------------------------------------------------------------------------------------------
// Client: Arrow Flight

/// Flight client bound to one `ankaad serve` endpoint.
#[pyclass(module = "ankaa._native", frozen)]
pub struct Client {
    rt: tokio::runtime::Runtime,
    inner: FlightServiceClient<Channel>,
    auth: Option<MetadataValue<tonic::metadata::Ascii>>,
    addr: String,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client").field("addr", &self.addr).finish_non_exhaustive()
    }
}

impl Client {
    fn request<T>(&self, msg: T) -> tonic::Request<T> {
        let mut req = tonic::Request::new(msg);
        if let Some(a) = &self.auth {
            req.metadata_mut().insert("authorization", a.clone());
        }
        req
    }

    /// `GetFlightInfo` for `sql`: (ticket, result schema).
    async fn flight_info(
        mut client: FlightServiceClient<Channel>,
        req: tonic::Request<FlightDescriptor>,
    ) -> Result<(Ticket, Schema), PyErr> {
        let info = client
            .get_flight_info(req)
            .await
            .map_err(|e| status_err(&e))?
            .into_inner();
        let ticket = info
            .endpoint
            .first()
            .and_then(|e| e.ticket.clone())
            .ok_or_else(|| PyRuntimeError::new_err("flight info has no endpoint"))?;
        let schema = info
            .try_decode_schema()
            .map_err(|e| PyRuntimeError::new_err(format!("schema: {e}")))?;
        Ok((ticket, schema))
    }
}

#[pymethods]
impl Client {
    /// Run a SQL query on the server and return the result as an Arrow table.
    fn sql(&self, py: Python<'_>, query: &str) -> PyResult<PyTable> {
        // One call: the SQL text is the ticket, and the result stream starts with its schema.
        // (`GetFlightInfo` first, as generic Flight clients do, costs a second round trip and
        // plans the query twice.)
        let client = self.inner.clone();
        let auth = self.auth.clone();
        let ticket = Ticket::new(query.to_owned());
        let out: PyResult<(Vec<RecordBatch>, SchemaRef)> = py.detach(|| {
            self.rt.block_on(async move {
                let mut req = tonic::Request::new(ticket);
                if let Some(a) = auth {
                    req.metadata_mut().insert("authorization", a);
                }
                let mut client = client;
                let stream = client.do_get(req).await.map_err(|e| status_err(&e))?.into_inner();
                let mut decoder = arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(
                    stream.map_err(|e| arrow_flight::error::FlightError::Tonic(Box::new(e))),
                );
                let mut batches: Vec<RecordBatch> = Vec::new();
                while let Some(batch) = decoder
                    .try_next()
                    .await
                    .map_err(|e| PyRuntimeError::new_err(format!("do_get: {e}")))?
                {
                    batches.push(batch);
                }
                // an empty result still carries its schema in the stream
                let schema = match batches.first() {
                    Some(b) => b.schema(),
                    None => decoder
                        .schema()
                        .cloned()
                        .ok_or_else(|| PyRuntimeError::new_err("do_get: the stream had no schema"))?,
                };
                Ok((batches, schema))
            })
        });
        let (batches, schema) = out?;
        to_table(batches, schema)
    }

    /// Result schema of `query` (planned on the server, nothing executed).
    fn schema(&self, py: Python<'_>, query: &str) -> PyResult<PySchema> {
        let req = self.request(FlightDescriptor::new_cmd(query.to_owned()));
        let client = self.inner.clone();
        let out: PyResult<Schema> = py.detach(|| {
            self.rt.block_on(async move {
                let (_ticket, schema) = Self::flight_info(client, req).await?;
                Ok(schema)
            })
        });
        Ok(PySchema::new(Arc::new(out?)))
    }

    /// Names of the tables the server exposes (`ListFlights`).
    fn tables(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        let req = self.request(Criteria::default());
        let mut client = self.inner.clone();
        let out: PyResult<Vec<String>> = py.detach(|| {
            self.rt.block_on(async move {
                let stream = client.list_flights(req).await.map_err(|e| status_err(&e))?.into_inner();
                let infos: Vec<_> = stream.try_collect().await.map_err(|e| status_err(&e))?;
                let mut names: Vec<String> = infos
                    .into_iter()
                    .filter_map(|i| i.flight_descriptor.and_then(|d| d.path.into_iter().next()))
                    .collect();
                names.sort();
                Ok(names)
            })
        });
        out
    }

    #[getter]
    fn addr(&self) -> &str {
        &self.addr
    }

    fn __repr__(&self) -> String {
        format!("Client(addr={:?}, auth={})", self.addr, self.auth.is_some())
    }
}

fn read_pem(path: &str, what: &str) -> PyResult<Vec<u8>> {
    std::fs::read(path).map_err(|e| PyValueError::new_err(format!("{what} {path}: {e}")))
}

/// Connect to an `ankaad` Flight endpoint (`host:port`).
#[pyfunction]
#[pyo3(signature = (addr, token = None, tls_ca = None, tls_domain = None, tls_cert = None, tls_key = None, connect_timeout_s = 10.0))]
#[allow(clippy::too_many_arguments)]
fn connect(
    py: Python<'_>,
    addr: &str,
    token: Option<&str>,
    tls_ca: Option<&str>,
    tls_domain: Option<&str>,
    tls_cert: Option<&str>,
    tls_key: Option<&str>,
    connect_timeout_s: f64,
) -> PyResult<Client> {
    let ca = tls_ca.map(|p| read_pem(p, "tls_ca")).transpose()?;
    let identity = match (tls_cert, tls_key) {
        (Some(c), Some(k)) => Some((read_pem(c, "tls_cert")?, read_pem(k, "tls_key")?)),
        (None, None) => None,
        _ => {
            return Err(PyValueError::new_err(
                "tls_cert and tls_key must be given together",
            ));
        }
    };
    let auth = token
        .map(|t| MetadataValue::try_from(format!("Bearer {t}")))
        .transpose()
        .map_err(|e| PyValueError::new_err(format!("token: {e}")))?;
    let endpoint = ankaa_flight::client_endpoint(
        addr,
        ca.as_deref(),
        tls_domain,
        identity.as_ref().map(|(c, k)| (c.as_slice(), k.as_slice())),
    )
    .map_err(|e| PyValueError::new_err(format!("endpoint: {e}")))?
    .connect_timeout(std::time::Duration::from_secs_f64(connect_timeout_s.max(0.0)));
    let rt = runtime()?;
    let channel: Result<Channel, tonic::transport::Error> =
        py.detach(|| rt.block_on(endpoint.connect()));
    let channel = channel.map_err(|e| PyConnectionError::new_err(format!("connect {addr}: {e}")))?;
    Ok(Client {
        rt,
        inner: FlightServiceClient::new(channel),
        auth,
        addr: addr.to_owned(),
    })
}

// ---------------------------------------------------------------------------------------------
// In-process Flight server for the Python test-suite (feature `test-server`)

#[cfg(feature = "test-server")]
mod test_server {
    use std::sync::Arc;

    use pyo3::exceptions::{PyRuntimeError, PyValueError};
    use pyo3::prelude::*;

    use crate::{capture, runtime};

    /// Handle of a running in-process Flight server; `stop()` or drop shuts it down.
    #[pyclass(module = "ankaa._native")]
    pub struct TestServer {
        rt: Option<tokio::runtime::Runtime>,
        stop: Option<tokio::sync::oneshot::Sender<()>>,
        task: Option<tokio::task::JoinHandle<()>>,
        #[pyo3(get)]
        port: u16,
        #[pyo3(get)]
        addr: String,
    }

    impl std::fmt::Debug for TestServer {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("TestServer").field("addr", &self.addr).finish_non_exhaustive()
        }
    }

    impl TestServer {
        fn shutdown(&mut self) {
            if let Some(tx) = self.stop.take() {
                let _ = tx.send(());
            }
            if let (Some(rt), Some(task)) = (self.rt.as_ref(), self.task.take()) {
                let _ = rt.block_on(async { tokio::time::timeout(std::time::Duration::from_secs(5), task).await });
            }
            if let Some(rt) = self.rt.take() {
                rt.shutdown_background();
            }
        }
    }

    #[pymethods]
    impl TestServer {
        fn stop(&mut self, py: Python<'_>) {
            py.detach(|| self.shutdown());
        }

        fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
            slf
        }

        #[pyo3(signature = (*_args))]
        fn __exit__(&mut self, py: Python<'_>, _args: &Bound<'_, pyo3::types::PyTuple>) {
            self.stop(py);
        }
    }

    impl Drop for TestServer {
        fn drop(&mut self) {
            self.shutdown();
        }
    }

    /// Start a Flight server on `127.0.0.1:<random port>` serving the tables of `capture_path`.
    #[pyfunction]
    #[pyo3(signature = (capture_path, token = None, expect_snapshots = false))]
    pub fn _test_server(
        py: Python<'_>,
        capture_path: &str,
        token: Option<String>,
        expect_snapshots: bool,
    ) -> PyResult<(u16, TestServer)> {
        let p = std::path::PathBuf::from(capture_path);
        let (ctx, _stats) = py
            .detach(|| capture::load(&p, expect_snapshots))
            .map_err(PyValueError::new_err)?;
        let rt = runtime()?;
        let listener = rt
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .map_err(|e| PyRuntimeError::new_err(format!("bind: {e}")))?;
        let port = listener
            .local_addr()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?
            .port();
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let service = ankaa_flight::AnkaaFlight::new(Arc::new(ctx), token);
        let task = rt.spawn(async move {
            if let Err(e) = ankaa_flight::serve(service, listener, None, async {
                let _ = rx.await;
            })
            .await
            {
                eprintln!("ankaa test server: {e}");
            }
        });
        Ok((
            port,
            TestServer {
                rt: Some(rt),
                stop: Some(tx),
                task: Some(task),
                port,
                addr: format!("127.0.0.1:{port}"),
            },
        ))
    }
}

/// Native module: `ankaa._native`.
#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", VERSION)?;
    m.add("HAS_TEST_SERVER", cfg!(feature = "test-server"))?;
    m.add_class::<Dataset>()?;
    m.add_class::<Client>()?;
    m.add_function(wrap_pyfunction!(open_capture, m)?)?;
    m.add_function(wrap_pyfunction!(connect, m)?)?;
    #[cfg(feature = "test-server")]
    {
        m.add_class::<test_server::TestServer>()?;
        m.add_function(wrap_pyfunction!(test_server::_test_server, m)?)?;
    }
    Ok(())
}
