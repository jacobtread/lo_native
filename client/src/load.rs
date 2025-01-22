use crate::{ConvertOffice, OfficeConvertClient, RequestError};
use async_trait::async_trait;
use std::{
    sync::{atomic::AtomicUsize, Arc},
    time::Duration,
};
use thiserror::Error;
use tokio::{
    sync::{Mutex, Notify},
    time::{sleep, timeout, Instant},
};
use tracing::{debug, error};

/// Round robbin load balancer, will pass convert jobs
/// around to the next available client, connections
/// will wait until there is an available client
#[derive(Clone)]
pub struct OfficeConvertLoadBalancer {
    /// Inner portion of the load balancer
    inner: Arc<OfficeConvertLoadBalancerInner>,
}

impl OfficeConvertLoadBalancer {
    /// Creates a load balancer from the provided collection of clients
    ///
    /// ## Arguments
    /// * `clients` - The clients to load balance amongst
    pub fn new<I>(clients: I) -> Self
    where
        I: IntoIterator<Item = OfficeConvertClient>,
    {
        Self::new_with_timing(clients, Default::default())
    }

    /// Creates a load balancer from the provided collection of clients
    /// with timing configuration
    ///
    /// ## Arguments
    /// * `clients` - The clients to load balance amongst
    /// * `timing` - Timing configuration
    pub fn new_with_timing<I>(clients: I, timing: LoadBalancerTiming) -> Self
    where
        I: IntoIterator<Item = OfficeConvertClient>,
    {
        let clients = clients
            .into_iter()
            .map(|client| {
                Mutex::new(LoadBalancedClient {
                    client,
                    busy_externally_at: None,
                })
            })
            .collect::<Vec<_>>();

        let inner = OfficeConvertLoadBalancerInner {
            clients,
            free_notify: Notify::new(),
            active: AtomicUsize::new(0),
            timing,
        };

        Self {
            inner: Arc::new(inner),
        }
    }

    /// Checks if all client connections are blocked externally, used
    /// to handle the case when to not wait on notifiers
    pub async fn is_externally_blocked(&self) -> bool {
        let inner = &*self.inner;
        for client in inner.clients.iter() {
            let client = match timeout(Duration::from_secs(1), client.lock()).await {
                Ok(value) => value,
                // Couldn't obtain the lock, this client is likely in use so we can
                // consider ourselves to not be externally blocked
                Err(_) => return false,
            };

            // Client is busy externally
            if client.busy_externally_at.is_none() {
                return false;
            }
        }

        true
    }
}

pub struct LoadBalancerTiming {
    /// Time in-between external busy checks
    pub retry_busy_check_after: Duration,
    /// Time to wait before repeated attempts
    pub retry_single_external: Duration,
    /// Timeout to wait on the notifier for
    pub notify_timeout: Duration,
}

impl Default for LoadBalancerTiming {
    fn default() -> Self {
        Self {
            retry_busy_check_after: Duration::from_secs(5),
            retry_single_external: Duration::from_secs(1),
            notify_timeout: Duration::from_secs(120),
        }
    }
}

struct OfficeConvertLoadBalancerInner {
    /// Available clients the load balancer can use
    clients: Vec<Mutex<LoadBalancedClient>>,

    /// Number of active in use clients
    active: AtomicUsize,

    /// Notifier for connections that are no longer busy
    free_notify: Notify,

    /// Timing for various actions
    timing: LoadBalancerTiming,
}

struct LoadBalancedClient {
    /// The actual client
    client: OfficeConvertClient,

    /// Last time the server reported as busy externally
    busy_externally_at: Option<Instant>,
}

#[derive(Debug, Error)]
pub enum LoadBalanceError {
    #[error("no servers available for load balancing")]
    NoServers,
}

#[async_trait]
impl ConvertOffice for OfficeConvertLoadBalancer {
    async fn convert(&self, file: Vec<u8>) -> Result<bytes::Bytes, RequestError> {
        let inner = &*self.inner;

        let total_clients = inner.clients.len();
        let multiple_clients = total_clients > 1;

        loop {
            for (index, client) in inner.clients.iter().enumerate() {
                let mut client = match client.try_lock() {
                    Ok(value) => value,
                    // Server is already in use
                    Err(_) => continue,
                };

                let client = &mut *client;

                let now = Instant::now();

                if let Some(busy_externally_at) = client.busy_externally_at {
                    let since_check = now.duration_since(busy_externally_at);

                    // Don't check this server if the busy check timeout hasn't passed (only if we have multiple choices)
                    if since_check < inner.timing.retry_busy_check_after && multiple_clients {
                        continue;
                    }
                }

                // Check if the server is busy externally (Busy outside of our control)
                let externally_busy = match client.client.is_busy().await {
                    Ok(value) => value,
                    Err(err) => {
                        error!("failed to perform server busy check at {index}: {err}");

                        // Mark erroneous servers as busy
                        true
                    }
                };

                // Store the busy state if busy
                if externally_busy {
                    debug!("server at {index} is busy externally");

                    client.busy_externally_at = Some(now);
                    continue;
                }

                // Clear external busy state
                client.busy_externally_at = None;

                debug!("obtained available server {index} for convert");

                // Increase active counter
                inner
                    .active
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

                let response = client.client.convert(file).await;

                // Notify waiters that this server is now free
                inner.free_notify.notify_waiters();

                // Decrease active counter
                inner
                    .active
                    .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);

                return response;
            }

            let active_counter = inner.active.load(std::sync::atomic::Ordering::SeqCst);

            // Handle case where all clients are blocked externally, we won't be woken by any clients
            // in this case, so instead of waiting for the notifier we wait a short duration
            //
            // If number of active connections are zero we can assume we are blocked for some reason
            // likely an external factor, we would never get notified so we must poll instead?
            let externally_blocked = self.is_externally_blocked().await;
            if externally_blocked || active_counter < 1 {
                debug!("all servers are externally blocked, delaying next attempt");
                sleep(inner.timing.retry_single_external).await;
                continue;
            }

            debug!("no available servers, waiting until one is available");

            // All servers are in use, wait for the free notifier, this has a timeout
            // incase a complication occurs
            _ = timeout(inner.timing.notify_timeout, inner.free_notify.notified()).await;
        }
    }
}
