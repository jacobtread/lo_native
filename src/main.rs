use anyhow::{anyhow, Context};
use axum::{
    body::Body,
    extract::DefaultBodyLimit,
    http::{header, HeaderValue, Response, StatusCode},
    routing::{get, post},
    Extension, Json, Router,
};
use axum_typed_multipart::{FieldData, TryFromMultipart, TypedMultipart};
use bytes::Bytes;
use clap::Parser;
use error::DynHttpError;
use libreofficekit::{
    CallbackType, DocUrl, FilterTypes, Office, OfficeError, OfficeOptionalFeatures,
    OfficeVersionInfo,
};
use parking_lot::Mutex;
use rand::{distributions::Alphanumeric, Rng};
use serde::Serialize;
use std::{env::temp_dir, ffi::CStr, path::PathBuf, rc::Rc, sync::Arc};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error};
use tracing_subscriber::EnvFilter;

mod error;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Path to the office installation (Omit to determine automatically)
    #[arg(long)]
    office_path: Option<String>,

    /// Port to bind the server to, defaults to 8080
    #[arg(long)]
    port: Option<u16>,

    /// Host to bind the server to, defaults to 0.0.0.0
    #[arg(long)]
    host: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    _ = dotenvy::dotenv();

    // Start configuring a `fmt` subscriber
    let subscriber = tracing_subscriber::fmt()
        // Use the logging options from env variables
        .with_env_filter(EnvFilter::from_default_env())
        // Display source code file paths
        .with_file(true)
        // Display source code line numbers
        .with_line_number(true)
        // Don't display the event's target (module path)
        .with_target(false)
        // Build the subscriber
        .finish();

    // use that subscriber to process traces emitted after this point
    tracing::subscriber::set_global_default(subscriber)?;

    let args = Args::parse();

    let mut office_path: Option<PathBuf> = None;

    // Try loading office path from command line
    if let Some(path) = args.office_path {
        office_path = Some(PathBuf::from(&path));
    }

    // Try loading office path from environment variables
    if office_path.is_none() {
        if let Ok(path) = std::env::var("LIBREOFFICE_SDK_PATH") {
            office_path = Some(PathBuf::from(&path));
        }
    }

    // Try determine default office path
    if office_path.is_none() {
        office_path = Office::find_install_path();
    }

    // Check a path was provided
    let office_path = match office_path {
        Some(value) => value,
        None => {
            error!("no office install path provided, cannot start server");
            panic!();
        }
    };

    debug!("using libreoffice install from: {}", office_path.display());

    // Determine the address to run the server on
    let server_address = if args.host.is_some() || args.port.is_some() {
        let host = args.host.unwrap_or_else(|| "0.0.0.0".to_string());
        let port = args.port.unwrap_or(8080);

        format!("{host}:{port}")
    } else {
        std::env::var("SERVER_ADDRESS").context("missing SERVER_ADDRESS")?
    };

    // Create office access and get office details
    let (office_details, office_handle) = create_office_runner(office_path).await?;

    // Create the router
    let app = Router::new()
        .route("/status", get(status))
        .route("/office-version", get(office_version))
        .route("/supported-formats", get(supported_formats))
        .route("/convert", post(convert))
        .route("/collect-garbage", post(collect_garbage))
        .layer(DefaultBodyLimit::max(1024 * 1024 * 1024))
        .layer(Extension(office_handle))
        .layer(Extension(Arc::new(office_details)));

    // Create a TCP listener
    let listener = tokio::net::TcpListener::bind(&server_address)
        .await
        .context("failed to bind http server")?;

    debug!("server started on: {server_address}");

    // Serve the app from the listener
    axum::serve(listener, app)
        .await
        .context("failed to serve")?;

    Ok(())
}

/// Messages the office runner can process
pub enum OfficeMsg {
    /// Message to convert a file
    Convert {
        /// The file bytes to convert
        bytes: Bytes,

        /// The return channel for sending back the result
        tx: oneshot::Sender<anyhow::Result<Bytes>>,
    },

    /// Tells office to clean up and trim its memory usage
    CollectGarbage,

    /// Message to check if the server is busy, ignored
    BusyCheck,
}

/// Handle to send messages to the office runner
#[derive(Clone)]
pub struct OfficeHandle(mpsc::Sender<OfficeMsg>);

/// Creates a new office runner on its own thread providing
/// a handle to access it via messages
async fn create_office_runner(path: PathBuf) -> anyhow::Result<(OfficeDetails, OfficeHandle)> {
    let (tx, rx) = mpsc::channel(1);

    let (startup_tx, startup_rx) = oneshot::channel();

    std::thread::spawn(move || {
        let mut startup_tx = Some(startup_tx);

        if let Err(cause) = office_runner(path, rx, &mut startup_tx) {
            error!(%cause, "failed to start office runner");

            // Send the error to the startup channel if its still available
            if let Some(startup_tx) = startup_tx.take() {
                _ = startup_tx.send(Err(cause));
            }
        }
    });

    // Wait for a successful startup
    let office_details = startup_rx.await.context("startup channel unavailable")??;
    let office_handle = OfficeHandle(tx);

    Ok((office_details, office_handle))
}

#[derive(Debug, Default)]
struct RunnerState {
    password_requested: bool,
}

#[derive(Debug)]
struct OfficeDetails {
    filter_types: Option<FilterTypes>,
    version: Option<OfficeVersionInfo>,
}

/// Main event loop for an office runner
fn office_runner(
    path: PathBuf,
    mut rx: mpsc::Receiver<OfficeMsg>,
    startup_tx: &mut Option<oneshot::Sender<anyhow::Result<OfficeDetails>>>,
) -> anyhow::Result<()> {
    // Create office instance
    let office = Office::new(&path).context("failed to create office instance")?;

    let tmp_dir = temp_dir();

    // Generate random ID for the path name
    let random_id = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(10)
        .map(|value| value as char)
        .collect::<String>();

    // Create input and output paths
    let temp_in = tmp_dir.join(format!("lo_native_input_{random_id}"));
    let temp_out = tmp_dir.join(format!("lo_native_output_{random_id}.pdf"));

    let runner_state = Rc::new(Mutex::new(RunnerState::default()));

    // Allow prompting for passwords
    office
        .set_optional_features(OfficeOptionalFeatures::DOCUMENT_PASSWORD)
        .context("failed to set optional features")?;

    // Load supported filters and office version details
    let filter_types = office.get_filter_types().ok();
    let version = office.get_version_info().ok();

    office
        .register_callback({
            let runner_state = runner_state.clone();
            let input_url = DocUrl::from_path(&temp_in).context("failed to create input url")?;

            move |office, ty, payload| {
                debug!(?ty, "callback invoked");

                let state = &mut *runner_state.lock();

                if let CallbackType::DocumentPassword = ty {
                    state.password_requested = true;

                    // Provide now password
                    if let Err(cause) = office.set_document_password(&input_url, None) {
                        error!(?cause, "failed to set document password");
                    }
                }

                if let CallbackType::JSDialog = ty {
                    let payload = unsafe { CStr::from_ptr(payload) };
                    let value: serde_json::Value =
                        serde_json::from_slice(payload.to_bytes()).unwrap();

                    debug!(?value, "js dialog request");
                }
            }
        })
        .context("failed to register office callback")?;

    // Report successful startup
    if let Some(startup_tx) = startup_tx.take() {
        _ = startup_tx.send(Ok(OfficeDetails {
            filter_types,
            version,
        }));
    }

    // Get next message
    while let Some(msg) = rx.blocking_recv() {
        let (input, output) = match msg {
            OfficeMsg::Convert { bytes, tx } => (bytes, tx),

            OfficeMsg::CollectGarbage => {
                if let Err(cause) = office.trim_memory(2000) {
                    error!(%cause, "failed to collect garbage")
                }
                continue;
            }
            // Busy checks are ignored
            OfficeMsg::BusyCheck => continue,
        };

        let temp_in = TempFile {
            path: temp_in.clone(),
        };
        let temp_out = TempFile {
            path: temp_out.clone(),
        };

        // Convert document
        let result = convert_document(&office, temp_in, temp_out, input, &runner_state);

        // Send response
        _ = output.send(result);

        // Reset runner state
        *runner_state.lock() = RunnerState::default();
    }

    Ok(())
}

/// Converts the provided document bytes into PDF format returning
/// the converted bytes
fn convert_document(
    office: &Office,

    temp_in: TempFile,
    temp_out: TempFile,

    input: Bytes,

    runner_state: &Rc<Mutex<RunnerState>>,
) -> anyhow::Result<Bytes> {
    let in_url = temp_in.doc_url()?;
    let out_url = temp_out.doc_url()?;

    // Write to temp file
    std::fs::write(&temp_in.path, input).context("failed to write temp input")?;

    // Load document
    let mut doc = match office.document_load_with_options(&in_url, "InteractionHandler=0,Batch=1") {
        Ok(value) => value,
        Err(err) => match err {
            OfficeError::OfficeError(err) => {
                error!(%err, "failed to load document");

                let _state = &*runner_state.lock();

                // File was encrypted with a password
                if err.contains("Unsupported URL") {
                    return Err(anyhow!("file is encrypted"));
                }

                // File is malformed or corrupted
                if err.contains("loadComponentFromURL returned an empty reference") {
                    return Err(anyhow!("file is corrupted"));
                }

                return Err(OfficeError::OfficeError(err).into());
            }
            err => return Err(err.into()),
        },
    };

    debug!("document loaded");

    // Convert document
    let result = doc.save_as(&out_url, "pdf", None)?;

    // Attempt to free up some memory
    _ = office.trim_memory(1000);

    if !result {
        return Err(anyhow!("failed to convert file"));
    }

    // Read document context
    let bytes = std::fs::read(&temp_out.path).context("failed to read temp out file")?;

    Ok(Bytes::from(bytes))
}

/// Request to convert a file
#[derive(TryFromMultipart)]
struct UploadAssetRequest {
    /// The file to convert
    #[form_data(limit = "unlimited")]
    file: FieldData<Bytes>,
}

/// POST /convert
///
/// Converts the provided file to PDF format responding with the PDF file
async fn convert(
    Extension(office): Extension<OfficeHandle>,
    TypedMultipart(UploadAssetRequest { file }): TypedMultipart<UploadAssetRequest>,
) -> Result<Response<Body>, DynHttpError> {
    let (tx, rx) = oneshot::channel();

    // Convert the file
    office
        .0
        .send(OfficeMsg::Convert {
            bytes: file.contents,
            tx,
        })
        .await
        .context("failed to send convert request")?;

    // Wait for the response
    let converted = rx.await.context("failed to get convert response")??;

    // Build the response
    let response = Response::builder()
        .header(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/pdf"),
        )
        .body(Body::from(converted))
        .context("failed to create response")?;

    Ok(response)
}

/// Result from checking the server busy state
#[derive(Serialize)]
struct StatusResponse {
    /// Whether the server is busy
    is_busy: bool,
}

/// GET /status
///
/// Checks if the converter is currently busy
async fn status(Extension(office): Extension<OfficeHandle>) -> Json<StatusResponse> {
    let is_locked = office.0.try_send(OfficeMsg::BusyCheck).is_err();
    Json(StatusResponse { is_busy: is_locked })
}

#[derive(Serialize)]
struct VersionResponse {
    /// Major version of LibreOffice
    major: u32,
    /// Minor version of LibreOffice
    minor: u32,
    /// Libreoffice "Build ID"
    build_id: String,
}

/// GET /office-version
///
/// Checks if the converter is currently busy
async fn office_version(
    Extension(details): Extension<Arc<OfficeDetails>>,
) -> Result<Json<VersionResponse>, StatusCode> {
    let version = details.version.as_ref().ok_or(StatusCode::NOT_FOUND)?;
    let product_version = &version.product_version;

    Ok(Json(VersionResponse {
        build_id: version.build_id.clone(),
        major: product_version.major,
        minor: product_version.minor,
    }))
}

#[derive(Serialize)]
struct SupportedFormat {
    /// Name of the file format
    name: String,
    /// Mime type of the format
    mime: String,
}

/// GET /supported-formats
///
/// Provides an array of supported file formats
async fn supported_formats(
    Extension(details): Extension<Arc<OfficeDetails>>,
) -> Result<Json<Vec<SupportedFormat>>, StatusCode> {
    let types = details.filter_types.as_ref().ok_or(StatusCode::NOT_FOUND)?;

    let formats: Vec<SupportedFormat> = types
        .values
        .iter()
        .map(|(key, value)| SupportedFormat {
            name: key.to_string(),
            mime: value.media_type.to_string(),
        })
        .collect();

    Ok(Json(formats))
}

/// POST /collect-garbage
///
/// Collects garbage from the office converter
async fn collect_garbage(Extension(office): Extension<OfficeHandle>) -> StatusCode {
    _ = office.0.send(OfficeMsg::CollectGarbage).await;
    StatusCode::OK
}

/// Temporary file that will be removed when it's [Drop] is called
struct TempFile {
    /// Path to the temporary file
    path: PathBuf,
}

impl TempFile {
    fn doc_url(&self) -> Result<DocUrl, OfficeError> {
        DocUrl::from_path(&self.path)
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        if self.path.exists() {
            dbg!(&self.path);
            _ = std::fs::remove_file(&self.path)
        }
    }
}
