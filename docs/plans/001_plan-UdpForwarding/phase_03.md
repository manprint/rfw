# Fase 2 — Motore UDP, monitor passivo, sblocco e documentazione

Intent: i forwarder UDP funzionano da CLI, env e YAML, con le stesse proprietà operative del TCP:
- hot reload;
- bind retry;
- health;
- stats/metriche;
- DNS dinamico;
- monitor con alert;
- shutdown rapido.

Le prestazioni hanno evidenza numerica (G-UDP-BENCH) e la compilazione cross-platform è verificata (G-XPLAT).
Prerequisiti: P1 DONE (`Protocol`, label `/udp`, settings `udp_*`, `ForwarderStats::with_protocol`, contatori
`datagrams_*`, `ProbeReport.protocol`, gate D17 attivo).
Chiusura di fase: P2; review di agent-1:opus.

## Contratto di stato e ownership
Leggere prima STATE.md §0–1.
- Aprire l'unità prima degli edit; fare checkpoint ai confini di recovery indicati.
- Un worker delegato lavora solo nella sua unità OPEN.
- Il design mancante, o due fix falliti sullo stesso problema, vanno ad agent-1:opus.

## Contesto di design locale
Plan revision: 1.

### Decisioni (significato locale)
- **D5 — modello sessione.**
  - Un listener `tokio::net::UdpSocket` per forwarder, condiviso con `Arc`.
  - Una sessione per client `SocketAddr`, con un proprio socket upstream **connesso** (porta effimera): il
    kernel filtra le sorgenti e consegna gli ICMP.
  - Il task listener possiede in esclusiva `HashMap<SocketAddr, SessionEntry>` (std SipHash), senza lock.
- **D6 — client→upstream** nel task listener:
  - batch da `LISTENER_BATCH = 64` `try_recv_from` per wakeup;
  - send con syscall raw `socket2::SockRef::from(&upstream).send(data)`, **mai** `upstream.try_send()`/`send().await`;
  - `WouldBlock` = drop contato; il listener non fa mai `.await` su questo percorso (I-4).
- **D7 — upstream→client** (superata da D7a, rev 2: la lettura è `recv_upstream`, `try_io(READABLE)` poi `try_io(ERROR)`; un `try_io` con interest combinato non esegue mai la closure) in un task per sessione:
  - attesa: `upstream.ready(Interest::READABLE | Interest::ERROR)`;
  - lettura: `upstream.try_io(Interest::READABLE | Interest::ERROR, || (&*sockref).read(buf))`, con buffer
    thread-local da 64 KiB;
  - invio: `listener.try_send_to(data, client)`. Su `WouldBlock`, copia in `Vec` e `listener.send_to(..).await`
    in `select!` con cancel;
  - con batch pieno (`SESSION_BATCH = 64`) si fa `yield_now`. Il borrow del buffer thread-local non attraversa
    mai un `.await`.
- **D8/D8a — classificatore unico** `classify_udp_err`:

  | ErrorKind | Classe |
  |---|---|
  | `WouldBlock` | WouldBlock |
  | `Interrupted` | Transient |
  | `ConnectionRefused`, `ConnectionReset` | `Icmp(PortUnreachable)` |
  | `HostUnreachable` | `Icmp(HostUnreachable)` |
  | `NetworkUnreachable` | `Icmp(NetworkUnreachable)` |
  | altro | Fatal |

  Uso per percorso:

  | Percorso | WouldBlock | Transient | Icmp | Fatal |
  |---|---|---|---|---|
  | recv listener | fine batch | ignora | ignora (Windows: riguarda un client) | warn rate-limited + backoff 50 ms |
  | send upstream (raw) | drop + warn rate-limited "send buffer full" | drop + debug | drop + `mark_udp_icmp_error` + debug; sessione tenuta | drop + debug; sessione tenuta |
  | recv upstream | fine batch (Idle) | continua | `mark_udp_icmp_error` + debug, continua | `dead = true`, il task esce |
  | send verso client | slow path await | — | — | drop + debug |

- **D9 — eviction.**
  - Sweep fisso ogni `SWEEP_INTERVAL = 1 s`, solo dal listener.
  - Il timeout si legge live (`knobs.udp_session_timeout()`).
  - Si rimuove la sessione se `dead` o se `now_ms - last_activity_ms >= timeout_ms`.
- **D10 — DNS.** Per i remote hostname, `run_dns_refresher` gira ogni `DNS_REFRESH_INTERVAL = 1 s`:
  - chiama `resolve_targets`;
  - pubblica il set su `watch::Sender<Arc<[SocketAddr]>>` solo se il set ordinato cambia;
  - `observe_addrs` → `Event::DnsChanged`;
  - su errore: si tiene l'ultimo set, warn rate-limited 30 s, `mark_connect_failed`.

  Quando il set cambia, il listener chiude le sessioni con `remote` non più nel set. Le nuove sessioni usano
  `targets.first()`. Remote literal: set fisso, nessun refresher.
- **D11 — stop.** Cancel del token padre (le sessioni hanno token figli) → `close_all` → drop dell'engine →
  `tasks.close()` → `tasks.wait()` entro `SESSION_STOP_GRACE = 2 s` → ritorno. A quel punto l'ultimo
  `Arc<UdpSocket>` è caduto e la porta è libera (I-6). Niente SO_REUSEADDR. Le sessioni UDP non entrano nel
  `conn_tracker` TCP.
- **D12.**
  - `udp_session_timeout_secs` (60, hot reload via knob);
  - `udp_max_sessions` (16384, solo avvio): semaforo globale `udp_limit` nel manager, separato da `max_connections`.
- **D13 — contatori per forwarder UDP.**

  | Contatore | Significato |
  |---|---|
  | `bytes_sent` / `bytes_received` | byte payload |
  | `datagrams_sent` / `datagrams_received` | datagrammi inoltrati |
  | `datagrams_dropped` | ogni drop, in qualsiasi direzione |
  | `active_connections` | sessioni vive |
  | `rejected_connections` | datagrammi di client nuovi rifiutati per il cap |

- **D14 — monitor passivo** (2.2). Probe = solo DNS; verdetto = `udp_verdict`, con truth table in 2.2.
- **D15 — logging.**
  - Mai sopra `debug` per datagramma.
  - A `debug`: apertura e chiusura sessione, con campi `client`, `local`, `remote`, `reason`, `duration_ms`, `tx`, `rx`.
  - Drop, cap, EMFILE e DNS: warn/error rate-limited tramite `LogGate`, con campo `suppressed`.
  - A info: `Listening (udp) on …`, chiusure per cambio DNS, `Shutting down`.
- **D16** — `Event::UdpSessionLimitReached {label, rejected_total}`:
  - Warning, throttle `limit:{label}`;
  - testo `*UDP session limit reached*`;
  - alert locale al massimo ogni `LIMIT_ALERT_SPACING` (60 s).
- **D17** — il gate di 1.1 resta attivo per tutta la 2.1. Lo rimuove la 2.2.
- **D21** — buffer di ricezione fisso `UDP_BUF_BYTES = 65536` (≥ 65527, il payload massimo IPv6).
  `buffer_bytes` non si applica a UDP.
- **D22** — `socket_buffer_bytes` si applica al listener (al bind) e a ogni nuovo upstream, best effort.
  - Per il listener si rilegge `recv_buffer_size()`; se è minore della richiesta, warn
    `"UDP listener receive buffer capped by the kernel; raise net.core.rmem_max"`.
  - Linux riporta il doppio del valore richiesto, quindi il confronto con la richiesta è corretto.
- **D23** — upstream:
  `socket2::Socket::new(Domain::for_address(remote), Type::DGRAM, Some(socket2::Protocol::UDP))` →
  `set_nonblocking(true)` → buffer → `connect` → `UdpSocket::from_std(socket.into())`.
- **D24** — budget fd unix (2.2): `fd_plan`, poi raise del soft limit o warn.
- **D28** — contatori a batch, una `fetch_add` per contatore per batch, saltata se zero.
  - `now_ms` si legge una volta per batch.
  - `touch` usa `fetch_max` solo se il valore cambia.
  - `mark_connect_ok_at` è throttled a 100 ms, ma scrive sempre se esiste un ICMP con `ms >= ultimo ok`.
- **D29** — un datagramma di lunghezza 0 si inoltra.

### Invarianti di questa fase
- I-2: confini dei datagrammi 1:1 (`datagram_boundaries_are_preserved_for_all_sizes`).
- I-3: nessun cross-talk (`each_client_gets_its_own_session_and_only_its_replies`).
- I-4: il listener non attende su client→upstream (review 2.1 + uso esclusivo di `forward_upstream` sincrono).
- I-5: sessioni ≤ cap; gauge = sessioni vive; torna a 0 (`session_cap_drops_and_counts_new_flows`,
  `idle_sessions_expire_and_release_their_slot`, e2e `idle_session_expires`).
- I-6: stop a porta rilasciata (`cancel_releases_the_port_before_returning`, `manager::tests::stop_one_releases_the_udp_port`).
- I-7: gate attivo fino alla 2.2.
- I-8: log rate-limited (review + e2e `udp_drops_are_rate_limited_in_logs`).
- I-9: hot reload (`session_timeout_is_read_live`, e2e `yaml_protocol_udp_and_hot_reload_retarget`, `tcp_and_udp_share_a_port_independently`).
- I-11: SIGTERM ≤ 3 s (e2e `sigterm_exits_fast_with_idle_udp_sessions`).
- I-14: il primo datagramma non si perde (`first_datagram_of_a_new_session_is_never_dropped`).
- I-15: nessun ripristino senza evidenza (`monitor::tests::udp_verdict_truth_table`).

### Evidenze di ricerca applicate (dettagli in overview.md)
- **R2** (kernel, documentato):
  - un ICMP su UDP connesso imposta `sk_err`; `poll` riporta **solo EPOLLERR**;
  - l'errore è consumato dalla prima `recv` o `send`;
  - su loopback non c'è rate limit ICMP.

  Conseguenza (inferenza del supervisore): servono `Interest::ERROR` sul wait e un `try_io` con
  interest ERROR separato (D7a, rev 2). `try_recv()` di tokio con interest READABLE non consumerebbe l'errore e farebbe busy loop.
- **R3** (sorgente tokio 1.53.1 / mio 1.2.4, letto):
  - `try_io` non esegue la closure se la readiness filtrata è vuota, e azzera la readiness solo su `WouldBlock`;
  - un socket appena registrato ha readiness vuota.

  Conseguenza: send raw sull'upstream (D6), altrimenti il primo datagramma di ogni sessione andrebbe perso (I-14).
- **R5**: socket2 0.6.5 ha `impl Read for &Socket`, `SockRef: From<&S: AsFd/AsSocket>`, `Domain::for_address`
  e `From<Socket> for std::net::UdpSocket`.
- **R6** (inferenza): Windows segnala ICMP come `WSAECONNRESET` (`ConnectionReset`), anche sul listener. Per
  questo il listener lo ignora.

## Sub-phase

### 2.1 Motore UDP (`src/udp.rs`) e dispatch nel manager
- **Model:** agent-2:sonnet
- **Assignment:**
  - implementazione + test unit;
  - agent-1:opus fa la review **prima** del dispatch (conferma che lo scheletro sotto combacia con il codice a
    HEAD) e **dopo** il diff (concorrenza, lifecycle, I-4, I-6, I-14, nessun borrow del buffer attraverso un
    `.await`, log level).
- **Files:**
  - READ:
    - `src/forwarder.rs`: `run_forwarder` (loop di bind), `ListeningGuard`, `ConnGuard`, `cancelled`,
      `resolve_targets`, `BIND_RETRY_DELAY`, `LIMIT_ALERT_SPACING`;
    - `src/stats.rs`: `ForwarderStats`, `elapsed_ms`, `mark_connect_ok`, `mark_connect_failed`, `observe_addrs`, `NEVER_MS`;
    - `src/config.rs`: `RuntimeKnobs`, `MAX_UDP_SESSIONS`;
    - `src/manager.rs`: `ForwarderManager`, `new`, `start_one`;
    - `src/notify.rs`: `Event`, `kind`, `severity`, `throttle`, `render`, i test con la lista di tutti gli eventi;
    - `src/testutil.rs`: `spawn_fake_dns`, `resolver_for`.
  - WRITE: `src/forwarder.rs` (solo visibilità), `src/stats.rs`, `src/config.rs` (knob), `src/notify.rs`,
    `src/manager.rs`, `src/main.rs` (`mod udp;`).
  - NEW: `src/udp.rs`, registrato in `src/main.rs` con `mod udp;` dopo `mod testutil;` (in ordine alfabetico:
    tra `mod testutil;` e la fine della lista dei `mod`).
- **Change:**
  Preconditions: P1 DONE; gate D17 presente (nessuna config reale può arrivare al dispatch UDP; lo esercitano
  solo i test).
  Contract: le decisioni di questa sezione, più lo scheletro qui sotto, che è normativo. Nomi, costanti,
  messaggi di log, ordine del `select!` e classificazione vanno rispettati. Commenti e formattazione sono liberi.
  Scope:
  - Nessun cambio al percorso TCP, salvo `pub(crate)` sui simboli elencati e il dispatch in `start_one`.
  - `run_monitor` resta invariato (in 2.1 per un cfg UDP sonderebbe in TCP, ma è raggiungibile solo dai test).
  - Niente GRO/GSO (fase 3), nessun cambio a `main.rs` oltre `mod udp;`.
  Steps:
  1. S1 — `src/forwarder.rs`: rendere `pub(crate)`
     - `struct ListeningGuard` e il suo `fn new`;
     - `struct ConnGuard` e il suo `fn new`;
     - `async fn cancelled`;
     - `const BIND_RETRY_DELAY`, `const LIMIT_ALERT_SPACING`.

     Nient'altro. Expected: compila.
  2. S2 — `src/stats.rs`, aggiunte:
     ```rust
     /// Kind of the last ICMP error reported on a UDP session's upstream socket.
     #[derive(Debug, Clone, Copy, PartialEq, Eq)]
     #[repr(u8)]
     pub enum IcmpKind { PortUnreachable = 1, HostUnreachable = 2, NetworkUnreachable = 3 }
     ```
     - Campi di `ForwarderStats`:
       - `udp_icmp_error_ms: AtomicU64`, inizializzato a `NEVER_MS`, con doc "ms since epoch of the last ICMP
         error seen by any session";
       - `udp_icmp_kind: AtomicU8`, inizializzato a 0.

       Inizializzarli in `with_protocol`.
     - Metodi:
       ```rust
       /// Milliseconds since this forwarder's epoch: the clock of every `*_ms` field.
       pub fn now_ms(&self) -> u64 { self.elapsed_ms() }
       pub fn add_datagrams_sent(&self, n: u64) { self.datagrams_sent.fetch_add(n, Ordering::Relaxed); }
       pub fn add_datagrams_received(&self, n: u64) { /* idem */ }
       pub fn add_datagrams_dropped(&self, n: u64) { /* idem */ }
       /// A UDP reply arrived at `now_ms`. Throttled to one store per 100 ms, except that a
       /// reply after the latest ICMP error is always recorded (the monitor compares the two).
       pub fn mark_connect_ok_at(&self, now_ms: u64) {
           let last = self.last_connect_ok_ms.load(Ordering::Relaxed);
           let icmp = self.udp_icmp_error_ms.load(Ordering::Relaxed);
           let due = last == NEVER_MS
               || last.saturating_add(100) <= now_ms
               || (icmp != NEVER_MS && icmp >= last);
           if due { self.last_connect_ok_ms.store(now_ms, Ordering::Relaxed); }
       }
       /// A UDP session saw an ICMP error at `now_ms`. Wakes the monitor.
       pub fn mark_udp_icmp_error(&self, now_ms: u64, kind: IcmpKind) {
           self.udp_icmp_kind.store(kind as u8, Ordering::Relaxed);
           self.udp_icmp_error_ms.store(now_ms, Ordering::Relaxed);
           self.probe_kick.notify_one();
       }
       ```
  3. S3 — `src/config.rs` `RuntimeKnobs`:
     - campo `udp_session_timeout_secs: AtomicU64`, inizializzato con `default_udp_session_timeout()` in
       `from_settings`, `store` in `update` (dopo il clamp);
     - getter:
       ```rust
       /// Idle time after which a UDP session is closed (read live by every sweep).
       pub fn udp_session_timeout(&self) -> std::time::Duration {
           std::time::Duration::from_secs(self.udp_session_timeout_secs.load(Ordering::Relaxed).max(1))
       }
       ```
  4. S4 — `src/notify.rs`, variante (dopo `ConnectionLimitReached`):
     ```rust
     /// `udp_max_sessions` saturated; datagrams from new UDP clients are being dropped.
     UdpSessionLimitReached { label: String, rejected_total: u64 },
     ```
     - `kind()` → `"udp_session_limit_reached"`;
     - `severity()` → `Warning`, nello stesso gruppo di `ConnectionLimitReached`;
     - `throttle()` → `Some((format!("limit:{label}"), THROTTLE_LIMIT))`;
     - `render()`:
       ```rust
       Event::UdpSessionLimitReached { label, rejected_total } => {
           text.push_str(&format!(
               "{icon} *UDP session limit reached*\nForwarder {}\nudp_max_sessions is saturated; datagrams from new clients are being dropped ({rejected_total} refused so far).",
               mono(label)
           ));
       }
       ```
     - Test esistenti: aggiungere la variante alla tabella di render (accanto a `ConnectionLimitReached`, needles
       `["*UDP session limit reached*", "7 refused"]` con `rejected_total: 7`) e alla lista `repetitive` del test
       di throttle.
  5. S5 — NEW `src/udp.rs` secondo lo scheletro normativo qui sotto.
  6. S6 — `src/manager.rs`:
     - campo `udp_limit: Arc<Semaphore>`, con doc "Global cap on concurrent UDP sessions (`udp_max_sessions`, startup-only)";
     - in `new`: `udp_limit: Arc::new(Semaphore::new(settings.udp_max_sessions.clamp(1, MAX_UDP_SESSIONS)))`;
     - in `start_one`: `let udp_limit = self.udp_limit.clone();` e, nel supervisore,
       ```rust
       let inner = match cfg_clone.protocol {
           Protocol::Tcp => tokio::spawn(run_forwarder(cfg_clone, token_clone, stats, resolver, knobs, conn_tracker, conn_limit, notifier.clone())),
           Protocol::Udp => tokio::spawn(run_udp_forwarder(cfg_clone, token_clone, stats, resolver, knobs, udp_limit, notifier.clone())),
       };
       ```
     - import `crate::config::{MAX_UDP_SESSIONS, Protocol}` e `crate::udp::run_udp_forwarder`.
  7. S7 — `src/main.rs`: `mod udp;`. Expected: `cargo build --all-targets` senza warning.
  8. S8 — test (sotto), poi G-HYG + G-U2.1.

  Recovery boundary: dopo S5 (motore che compila) fare checkpoint §6 con l'elenco dei test già verdi.
  Failure handling:
  - **I-14 rosso** (il primo datagramma si perde): verificare che il send upstream sia la raw `SockRef::send`.
    Non aggiungere retry né sleep.
  - **Busy loop / CPU al 100 % su una sessione dopo ICMP**: verificare `Interest::ERROR` in `ready` e il ramo `try_io(Interest::ERROR)` di `recv_upstream`; mai un `try_io` con interest combinato (D7a).
  - **Port in use al rebind** dopo lo stop: verificare che `run_udp_forwarder` attenda `tasks.wait()` e che nessun
    `Arc<UdpSocket>` sopravviva fuori dai task tracciati.
  - Qualsiasi deviazione dallo scheletro va escalata ad agent-1:opus.

#### Scheletro normativo di `src/udp.rs`

```rust
//! UDP forwarding: one listener per forwarder, one connected upstream socket
//! per client address (a "session").
//!
//! Client -> target runs inside the listener task and never awaits: datagrams
//! are drained in batches and written to the session's upstream socket with a
//! raw non-blocking send; a full socket buffer drops (and counts) the datagram
//! instead of stalling every other client. Target -> client runs in one task
//! per session. Sessions expire after `udp_session_timeout_secs` of silence.

use std::cell::RefCell;
use std::collections::HashMap;
use std::io::{self, Read};
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use hickory_resolver::TokioResolver;
use tokio::io::Interest;
use tokio::net::UdpSocket;
use tokio::sync::{watch, OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info, warn};

use crate::config::{ForwarderConfig, RuntimeKnobs};
use crate::forwarder::{
    cancelled, resolve_targets, ConnGuard, ListeningGuard, BIND_RETRY_DELAY, LIMIT_ALERT_SPACING,
};
use crate::notify::{Event, Notifier};
use crate::stats::{ForwarderStats, IcmpKind};

/// Receive buffer: fits the largest UDP payload (65507 IPv4, 65527 IPv6).
const UDP_BUF_BYTES: usize = 65536;
/// Datagrams handled per listener wakeup before yielding to other tasks.
const LISTENER_BATCH: usize = 64;
/// Datagrams handled per session wakeup before yielding.
const SESSION_BATCH: usize = 64;
/// Cadence of the idle-session sweep (the timeout itself is read live).
const SWEEP_INTERVAL: Duration = Duration::from_secs(1);
/// Cadence of the hostname refresh (hickory's cache answers until the TTL expires).
const DNS_REFRESH_INTERVAL: Duration = Duration::from_secs(1);
/// Upper bound on waiting for session tasks when a forwarder stops.
const SESSION_STOP_GRACE: Duration = Duration::from_secs(2);
/// At most one warning per this window for each kind of datagram drop.
const DROP_LOG_SPACING: Duration = Duration::from_secs(10);
/// At most one DNS-failure warning per this window.
const DNS_FAIL_LOG_SPACING: Duration = Duration::from_secs(30);
/// Pause after a non-transient listener receive error, so it cannot spin.
const RECV_ERROR_BACKOFF: Duration = Duration::from_millis(50);

thread_local! {
    /// Per-worker-thread reply buffer: sessions share it instead of each
    /// holding 64 KiB. Never borrowed across an `.await`.
    static REPLY_BUF: RefCell<Vec<u8>> = RefCell::new(vec![0_u8; UDP_BUF_BYTES]);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UdpErr { WouldBlock, Transient, Icmp(IcmpKind), Fatal }

fn classify_udp_err(e: &io::Error) -> UdpErr {
    match e.kind() {
        io::ErrorKind::WouldBlock => UdpErr::WouldBlock,
        io::ErrorKind::Interrupted => UdpErr::Transient,
        io::ErrorKind::ConnectionRefused | io::ErrorKind::ConnectionReset => {
            UdpErr::Icmp(IcmpKind::PortUnreachable)
        }
        io::ErrorKind::HostUnreachable => UdpErr::Icmp(IcmpKind::HostUnreachable),
        io::ErrorKind::NetworkUnreachable => UdpErr::Icmp(IcmpKind::NetworkUnreachable),
        _ => UdpErr::Fatal,
    }
}

/// Rate limit for one warning on a hot path.
#[derive(Debug, Default)]
struct LogGate { last: Option<Instant>, suppressed: u64 }

impl LogGate {
    /// `Some(n)` = log now, `n` occurrences were suppressed since the last line.
    fn check(&mut self, spacing: Duration) -> Option<u64> {
        let now = Instant::now();
        if self.last.is_some_and(|t| now.duration_since(t) < spacing) {
            self.suppressed += 1;
            return None;
        }
        self.last = Some(now);
        Some(std::mem::take(&mut self.suppressed))
    }
}

#[derive(Debug, Default)]
struct Gates { send_full: LogGate, no_target: LogGate, open_failed: LogGate, recv_failed: LogGate }

/// Counters accumulated over one batch, flushed with one atomic add each.
#[derive(Debug, Default)]
struct Batch { bytes: u64, datagrams: u64, dropped: u64 }

/// State shared by the listener and a session's reply task.
struct SessionShared {
    client: SocketAddr,
    remote: SocketAddr,
    upstream: UdpSocket,
    /// `stats.now_ms()` of the last datagram in either direction.
    last_activity_ms: AtomicU64,
    /// The upstream socket failed for good; the listener reaps the session.
    dead: AtomicBool,
    /// Target -> client datagrams forwarded (close log only).
    rx_datagrams: AtomicU64,
    /// Keeps `active_connections` and the `udp_max_sessions` slot for exactly
    /// as long as the upstream socket exists.
    _guard: ConnGuard,
    _permit: OwnedSemaphorePermit,
}

impl SessionShared {
    fn touch(&self, now_ms: u64) {
        if self.last_activity_ms.load(Ordering::Relaxed) < now_ms {
            self.last_activity_ms.fetch_max(now_ms, Ordering::Relaxed);
        }
    }
}

/// Listener-owned bookkeeping for one session.
struct SessionEntry {
    shared: Arc<SessionShared>,
    /// Child of the forwarder token: cancelled on eviction or stop.
    cancel: CancellationToken,
    opened: Instant,
    /// Client -> target datagrams forwarded (close log only).
    tx_datagrams: u64,
}

fn close_entry(label: &str, e: &SessionEntry, reason: &'static str) {
    e.cancel.cancel();
    debug!(
        forwarder = %label,
        client = %e.shared.client,
        remote = %e.shared.remote,
        reason,
        duration_ms = u64::try_from(e.opened.elapsed().as_millis()).unwrap_or(u64::MAX),
        tx = e.tx_datagrams,
        rx = e.shared.rx_datagrams.load(Ordering::Relaxed),
        "UDP session closed"
    );
}

fn same_addr_set(a: &[SocketAddr], b: &[SocketAddr]) -> bool {
    let mut a = a.to_vec();
    let mut b = b.to_vec();
    a.sort_unstable();
    b.sort_unstable();
    a.dedup();
    b.dedup();
    a == b
}

#[cfg(unix)]
fn is_fd_exhaustion(e: &io::Error) -> bool {
    matches!(e.raw_os_error(), Some(libc::EMFILE) | Some(libc::ENFILE))
}
#[cfg(windows)]
fn is_fd_exhaustion(e: &io::Error) -> bool {
    e.raw_os_error() == Some(10024) // WSAEMFILE
}
#[cfg(not(any(unix, windows)))]
fn is_fd_exhaustion(_: &io::Error) -> bool {
    false
}

fn connect_upstream(remote: SocketAddr, sockbuf: Option<usize>) -> io::Result<UdpSocket> {
    let socket = socket2::Socket::new(
        socket2::Domain::for_address(remote),
        socket2::Type::DGRAM,
        Some(socket2::Protocol::UDP),
    )?;
    socket.set_nonblocking(true)?;
    if let Some(sz) = sockbuf {
        // Best effort, like the TCP path: the OS may cap or refuse the size.
        let _ = socket.set_recv_buffer_size(sz);
        let _ = socket.set_send_buffer_size(sz);
    }
    socket.connect(&remote.into())?;
    UdpSocket::from_std(socket.into())
}

fn tune_listener(socket: &UdpSocket, sockbuf: Option<usize>, label: &str) {
    let Some(want) = sockbuf else { return };
    let sock = socket2::SockRef::from(socket);
    if let Err(e) = sock.set_recv_buffer_size(want) {
        warn!(forwarder = %label, error = %e, "set_recv_buffer_size(udp listener) failed");
    }
    if let Err(e) = sock.set_send_buffer_size(want) {
        warn!(forwarder = %label, error = %e, "set_send_buffer_size(udp listener) failed");
    }
    if let Ok(got) = sock.recv_buffer_size() {
        if got < want {
            warn!(
                forwarder = %label,
                requested = want,
                effective = got,
                "UDP listener receive buffer capped by the kernel; raise net.core.rmem_max"
            );
        }
    }
}

/// Run one UDP forwarder until `cancel` fires. Returns only after every
/// session task has stopped, i.e. once the local port is free again.
pub async fn run_udp_forwarder(
    cfg: ForwarderConfig,
    cancel: CancellationToken,
    stats: Arc<ForwarderStats>,
    resolver: Arc<TokioResolver>,
    knobs: Arc<RuntimeKnobs>,
    udp_limit: Arc<Semaphore>,
    notifier: Notifier,
) {
    let cfg = Arc::new(cfg);
    let local_str = cfg.local_addr();
    let label: Arc<str> = Arc::from(cfg.label());
    let literal: Option<IpAddr> = cfg.remote_host.parse().ok();

    // Bind with auto-reconnect loop (same shape and messages as TCP).
    let mut bind_failures: u32 = 0;
    let socket = loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                info!(forwarder = %label, "Cancelled before bind");
                return;
            }
            result = UdpSocket::bind(&local_str) => {
                match result {
                    Ok(s) => break s,
                    Err(e) => {
                        bind_failures = bind_failures.saturating_add(1);
                        error!(forwarder = %label, error = %e, attempt = bind_failures, "Bind failed, retry in 5s");
                        notifier.notify(Event::BindFailed {
                            label: label.to_string(),
                            local_addr: local_str.clone(),
                            error: e.to_string(),
                            attempts: bind_failures,
                        });
                        if cancelled(&cancel, BIND_RETRY_DELAY).await {
                            return;
                        }
                    }
                }
            }
        }
    };

    tune_listener(&socket, knobs.socket_buffer_bytes(), &label);
    info!(forwarder = %label, "Listening (udp) on {} -> {}:{}", local_str, cfg.remote_host, cfg.remote_port);
    if bind_failures > 0 {
        notifier.notify(Event::BindRecovered {
            label: label.to_string(),
            local_addr: local_str.clone(),
            failed_attempts: bind_failures,
        });
    }
    let _listening = ListeningGuard::new(stats.clone());

    let socket = Arc::new(socket);
    let tasks = TaskTracker::new();
    let initial: Arc<[SocketAddr]> = match literal {
        Some(ip) => Arc::from(vec![SocketAddr::new(ip, cfg.remote_port)]),
        None => Arc::from(Vec::new()),
    };
    let (tx, targets) = watch::channel(initial);
    let dns_watch = literal.is_none();
    if dns_watch {
        tasks.spawn(run_dns_refresher(
            cfg.clone(), label.clone(), stats.clone(), resolver, knobs.clone(),
            notifier.clone(), tx, cancel.clone(),
        ));
    } else {
        drop(tx);
    }

    let mut engine = Engine {
        label: label.clone(), socket, stats, knobs, udp_limit, notifier,
        cancel, tasks: tasks.clone(), targets, dns_watch,
        sessions: HashMap::new(), gates: Gates::default(), last_limit_alert: None,
    };
    let mut buf = vec![0_u8; UDP_BUF_BYTES];
    engine.run(&mut buf).await;
    engine.close_all("shutdown");
    drop(engine);
    tasks.close();
    if tokio::time::timeout(SESSION_STOP_GRACE, tasks.wait()).await.is_err() {
        warn!(
            forwarder = %label,
            "UDP sessions did not stop within {}s; the port may stay bound briefly",
            SESSION_STOP_GRACE.as_secs()
        );
    }
    info!(forwarder = %label, "Shutting down");
}

enum ListenerDrain { Idle, Full, Failed }

struct Engine {
    label: Arc<str>,
    socket: Arc<UdpSocket>,
    stats: Arc<ForwarderStats>,
    knobs: Arc<RuntimeKnobs>,
    udp_limit: Arc<Semaphore>,
    notifier: Notifier,
    cancel: CancellationToken,
    tasks: TaskTracker,
    targets: watch::Receiver<Arc<[SocketAddr]>>,
    dns_watch: bool,
    sessions: HashMap<SocketAddr, SessionEntry>,
    gates: Gates,
    last_limit_alert: Option<Instant>,
}

impl Engine {
    async fn run(&mut self, buf: &mut [u8]) {
        let mut sweep = tokio::time::interval(SWEEP_INTERVAL);
        sweep.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        sweep.tick().await; // the first tick is immediate
        loop {
            // `select!` drops every branch future before running the chosen
            // handler, so handlers may borrow `self` mutably.
            tokio::select! {
                biased;
                _ = self.cancel.cancelled() => return,
                _ = sweep.tick() => self.sweep(),
                changed = self.targets.changed(), if self.dns_watch => match changed {
                    Ok(()) => self.on_targets_changed(),
                    Err(_) => self.dns_watch = false, // refresher gone (shutdown)
                },
                ready = self.socket.readable() => match ready {
                    Ok(()) => match self.drain_listener(buf) {
                        ListenerDrain::Idle => {}
                        ListenerDrain::Full => tokio::task::yield_now().await,
                        ListenerDrain::Failed => {
                            if cancelled(&self.cancel, RECV_ERROR_BACKOFF).await { return; }
                        }
                    },
                    Err(e) => {
                        if let Some(s) = self.gates.recv_failed.check(DROP_LOG_SPACING) {
                            warn!(forwarder = %self.label, error = %e, suppressed = s, "UDP listener readiness failed");
                        }
                        if cancelled(&self.cancel, RECV_ERROR_BACKOFF).await { return; }
                    }
                },
            }
        }
    }

    fn drain_listener(&mut self, buf: &mut [u8]) -> ListenerDrain {
        let now_ms = self.stats.now_ms();
        let mut batch = Batch::default();
        let mut outcome = ListenerDrain::Full;
        for _ in 0..LISTENER_BATCH {
            match self.socket.try_recv_from(buf) {
                Ok((n, client)) => self.on_client_datagram(client, &buf[..n], now_ms, &mut batch),
                Err(e) => match classify_udp_err(&e) {
                    UdpErr::WouldBlock => { outcome = ListenerDrain::Idle; break; }
                    UdpErr::Transient | UdpErr::Icmp(_) => {}
                    UdpErr::Fatal => {
                        if let Some(s) = self.gates.recv_failed.check(DROP_LOG_SPACING) {
                            warn!(forwarder = %self.label, error = %e, suppressed = s, "UDP listener receive failed");
                        }
                        outcome = ListenerDrain::Failed;
                        break;
                    }
                },
            }
        }
        if batch.datagrams > 0 {
            self.stats.add_sent(batch.bytes);
            self.stats.add_datagrams_sent(batch.datagrams);
        }
        if batch.dropped > 0 {
            self.stats.add_datagrams_dropped(batch.dropped);
        }
        outcome
    }

    fn on_client_datagram(&mut self, client: SocketAddr, data: &[u8], now_ms: u64, batch: &mut Batch) {
        if let Some(entry) = self.sessions.get_mut(&client) {
            if !entry.shared.dead.load(Ordering::Relaxed) {
                forward_upstream(entry, data, now_ms, batch, &self.stats, &mut self.gates.send_full, &self.label);
                return;
            }
        }
        if let Some(dead) = self.sessions.remove(&client) {
            close_entry(&self.label, &dead, "upstream error");
        }
        if let Some(entry) = self.open_session(client, now_ms, batch) {
            let entry = self.sessions.entry(client).or_insert(entry);
            forward_upstream(entry, data, now_ms, batch, &self.stats, &mut self.gates.send_full, &self.label);
        }
    }

    /// Synchronous on purpose: about four syscalls, no await (I-4).
    fn open_session(&mut self, client: SocketAddr, now_ms: u64, batch: &mut Batch) -> Option<SessionEntry> {
        // Separate statement: the watch read guard drops here, not after the else block.
        let first = self.targets.borrow().first().copied();
        let Some(remote) = first else {
            batch.dropped += 1;
            self.stats.mark_connect_failed();
            if let Some(s) = self.gates.no_target.check(DROP_LOG_SPACING) {
                warn!(forwarder = %self.label, suppressed = s, "UDP target not resolved yet; datagram dropped");
            }
            return None;
        };
        let permit = match self.udp_limit.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                batch.dropped += 1;
                self.stats.inc_rejected();
                debug!(forwarder = %self.label, client = %client, "UDP session rejected: udp_max_sessions reached");
                if self.last_limit_alert.is_none_or(|t| t.elapsed() >= LIMIT_ALERT_SPACING) {
                    self.last_limit_alert = Some(Instant::now());
                    warn!(forwarder = %self.label, "udp_max_sessions saturated; dropping datagrams from new clients");
                    self.notifier.notify(Event::UdpSessionLimitReached {
                        label: self.label.to_string(),
                        rejected_total: self.stats.rejected_connections.load(Ordering::Relaxed),
                    });
                }
                return None;
            }
        };
        let upstream = match connect_upstream(remote, self.knobs.socket_buffer_bytes()) {
            Ok(s) => s,
            Err(e) => {
                batch.dropped += 1;
                self.stats.mark_connect_failed();
                if let Some(s) = self.gates.open_failed.check(DROP_LOG_SPACING) {
                    if is_fd_exhaustion(&e) {
                        error!(forwarder = %self.label, error = %e, suppressed = s,
                            "UDP session open failed: out of file descriptors (raise the fd limit or lower udp_max_sessions)");
                    } else {
                        warn!(forwarder = %self.label, remote = %remote, error = %e, suppressed = s,
                            "UDP session open failed; datagram dropped");
                    }
                }
                return None; // `permit` drops here: the slot is released
            }
        };
        let local = upstream.local_addr().ok();
        let shared = Arc::new(SessionShared {
            client, remote, upstream,
            last_activity_ms: AtomicU64::new(now_ms),
            dead: AtomicBool::new(false),
            rx_datagrams: AtomicU64::new(0),
            _guard: ConnGuard::new(self.stats.clone()),
            _permit: permit,
        });
        let cancel = self.cancel.child_token();
        self.tasks.spawn(run_session(
            shared.clone(), self.socket.clone(), self.stats.clone(), self.label.clone(), cancel.clone(),
        ));
        debug!(forwarder = %self.label, client = %client, local = ?local, remote = %remote, "UDP session opened");
        Some(SessionEntry { shared, cancel, opened: Instant::now(), tx_datagrams: 0 })
    }

    fn sweep(&mut self) {
        if self.sessions.is_empty() { return; }
        let timeout_ms = u64::try_from(self.knobs.udp_session_timeout().as_millis()).unwrap_or(u64::MAX);
        let now_ms = self.stats.now_ms();
        let label = &self.label;
        self.sessions.retain(|_, e| {
            let reason = if e.shared.dead.load(Ordering::Relaxed) {
                Some("upstream error")
            } else if now_ms.saturating_sub(e.shared.last_activity_ms.load(Ordering::Relaxed)) >= timeout_ms {
                Some("idle timeout")
            } else {
                None
            };
            match reason {
                Some(r) => { close_entry(label, e, r); false }
                None => true,
            }
        });
    }

    fn on_targets_changed(&mut self) {
        let set: Arc<[SocketAddr]> = self.targets.borrow_and_update().clone();
        let before = self.sessions.len();
        let label = &self.label;
        self.sessions.retain(|_, e| {
            if set.contains(&e.shared.remote) { return true; }
            close_entry(label, e, "target address left DNS");
            false
        });
        let closed = before - self.sessions.len();
        if closed > 0 {
            info!(forwarder = %self.label, closed, new = ?set,
                "UDP target DNS changed; closed sessions to addresses no longer published");
        }
    }

    fn close_all(&mut self, reason: &'static str) {
        for (_, e) in self.sessions.drain() {
            close_entry(&self.label, &e, reason);
        }
    }
}

/// Client -> target for one datagram. Never awaits (I-4); the raw send also
/// works on a socket registered a moment ago (I-14).
fn forward_upstream(
    entry: &mut SessionEntry, data: &[u8], now_ms: u64, batch: &mut Batch,
    stats: &ForwarderStats, gate: &mut LogGate, label: &str,
) {
    let shared = &entry.shared;
    match socket2::SockRef::from(&shared.upstream).send(data) {
        Ok(_) => {
            entry.tx_datagrams += 1;
            batch.datagrams += 1;
            batch.bytes += data.len() as u64;
            shared.touch(now_ms);
        }
        Err(e) => {
            batch.dropped += 1;
            match classify_udp_err(&e) {
                UdpErr::WouldBlock => {
                    if let Some(s) = gate.check(DROP_LOG_SPACING) {
                        warn!(forwarder = %label, client = %shared.client, suppressed = s,
                            "UDP upstream send buffer full; datagram dropped (consider socket_buffer_bytes)");
                    }
                }
                UdpErr::Icmp(kind) => {
                    stats.mark_udp_icmp_error(now_ms, kind);
                    debug!(forwarder = %label, client = %shared.client, remote = %shared.remote, error = %e,
                        "UDP send reported an ICMP error from the target; datagram dropped");
                }
                UdpErr::Transient | UdpErr::Fatal => {
                    debug!(forwarder = %label, client = %shared.client, error = %e,
                        "UDP upstream send failed; datagram dropped");
                }
            }
        }
    }
}

enum SessionDrain { Idle, Full, Blocked(Vec<u8>), Dead }

async fn run_session(
    shared: Arc<SessionShared>, listener: Arc<UdpSocket>, stats: Arc<ForwarderStats>,
    label: Arc<str>, cancel: CancellationToken,
) {
    loop {
        let ready = tokio::select! {
            biased;
            _ = cancel.cancelled() => return,
            r = shared.upstream.ready(Interest::READABLE | Interest::ERROR) => r,
        };
        if let Err(e) = ready {
            shared.dead.store(true, Ordering::Relaxed);
            debug!(forwarder = %label, client = %shared.client, error = %e, "UDP session readiness failed; session marked dead");
            return;
        }
        match drain_upstream(&shared, &listener, &stats, &label) {
            SessionDrain::Idle => {}
            SessionDrain::Full => tokio::task::yield_now().await,
            SessionDrain::Dead => return,
            SessionDrain::Blocked(pending) => {
                let sent = tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return,
                    r = listener.send_to(&pending, shared.client) => r,
                };
                match sent {
                    Ok(_) => {
                        shared.rx_datagrams.fetch_add(1, Ordering::Relaxed);
                        stats.add_received(pending.len() as u64);
                        stats.add_datagrams_received(1);
                    }
                    Err(e) => {
                        stats.add_datagrams_dropped(1);
                        debug!(forwarder = %label, client = %shared.client, error = %e, "UDP reply send failed; datagram dropped");
                    }
                }
            }
        }
    }
}

/// Target -> client, synchronous: the thread-local buffer is never held
/// across an await.
fn drain_upstream(shared: &SessionShared, listener: &UdpSocket, stats: &ForwarderStats, label: &str) -> SessionDrain {
    REPLY_BUF.with(|cell| {
        let mut buf = cell.borrow_mut();
        let now_ms = stats.now_ms();
        let sock = socket2::SockRef::from(&shared.upstream);
        let mut batch = Batch::default();
        let mut replied = false;
        let mut outcome = SessionDrain::Full;
        for _ in 0..SESSION_BATCH {
            // READABLE|ERROR: an ICMP error is reported as EPOLLERR alone and
            // must be consumed by a receive, or `ready()` would spin.
            // rev 2 (D7a): implemented as `recv_upstream(&shared.upstream, &sock, &mut buf[..])`,
            // READABLE then ERROR; a combined-interest try_io never runs its closure.
            let res = recv_upstream(&shared.upstream, &sock, &mut buf[..]);
            match res {
                Ok(n) => {
                    replied = true;
                    match listener.try_send_to(&buf[..n], shared.client) {
                        Ok(_) => { batch.datagrams += 1; batch.bytes += n as u64; }
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                            outcome = SessionDrain::Blocked(buf[..n].to_vec());
                            break;
                        }
                        Err(e) => {
                            batch.dropped += 1;
                            debug!(forwarder = %label, client = %shared.client, error = %e, "UDP reply send failed; datagram dropped");
                        }
                    }
                }
                Err(e) => match classify_udp_err(&e) {
                    UdpErr::WouldBlock => { outcome = SessionDrain::Idle; break; }
                    UdpErr::Transient => {}
                    UdpErr::Icmp(kind) => {
                        stats.mark_udp_icmp_error(now_ms, kind);
                        debug!(forwarder = %label, client = %shared.client, remote = %shared.remote, error = %e,
                            "UDP target answered with an ICMP error");
                    }
                    UdpErr::Fatal => {
                        shared.dead.store(true, Ordering::Relaxed);
                        debug!(forwarder = %label, client = %shared.client, error = %e,
                            "UDP upstream receive failed; session marked dead");
                        outcome = SessionDrain::Dead;
                        break;
                    }
                },
            }
        }
        if replied {
            shared.touch(now_ms);
            stats.mark_connect_ok_at(now_ms);
        }
        if batch.datagrams > 0 {
            shared.rx_datagrams.fetch_add(batch.datagrams, Ordering::Relaxed);
            stats.add_received(batch.bytes);
            stats.add_datagrams_received(batch.datagrams);
        }
        if batch.dropped > 0 {
            stats.add_datagrams_dropped(batch.dropped);
        }
        outcome
    })
}

#[allow(clippy::too_many_arguments)]
async fn run_dns_refresher(
    cfg: Arc<ForwarderConfig>, label: Arc<str>, stats: Arc<ForwarderStats>,
    resolver: Arc<TokioResolver>, knobs: Arc<RuntimeKnobs>, notifier: Notifier,
    tx: watch::Sender<Arc<[SocketAddr]>>, cancel: CancellationToken,
) {
    let mut fail_gate = LogGate::default();
    loop {
        let result = tokio::select! {
            biased;
            _ = cancel.cancelled() => return,
            r = resolve_targets(&cfg.remote_host, cfg.remote_port, None, &resolver, knobs.dns_timeout()) => r,
        };
        match result {
            Ok(addrs) => {
                let ips: Vec<IpAddr> = addrs.iter().map(|a| a.ip()).collect();
                if let Some(old) = stats.observe_addrs(&ips) {
                    let mut new = ips.clone();
                    new.sort_unstable();
                    new.dedup();
                    info!(forwarder = %label, host = %cfg.remote_host, old = ?old, new = ?new, "Target DNS answer changed");
                    notifier.notify(Event::DnsChanged { label: label.to_string(), host: cfg.remote_host.clone(), old, new });
                }
                tx.send_if_modified(|cur| {
                    if same_addr_set(cur, &addrs) { return false; }
                    *cur = Arc::from(addrs);
                    true
                });
            }
            Err(e) => {
                stats.mark_connect_failed();
                if let Some(s) = fail_gate.check(DNS_FAIL_LOG_SPACING) {
                    warn!(forwarder = %label, host = %cfg.remote_host, error = %e, suppressed = s,
                        "UDP target DNS resolution failed; keeping the last known addresses");
                }
            }
        }
        if cancelled(&cancel, DNS_REFRESH_INTERVAL).await { return; }
    }
}
```

Note normative sullo scheletro:
- Il loop di bind è la copia di quello TCP (`run_forwarder`, a HEAD c50db1f) con `UdpSocket::bind`.
  Se a HEAD il loop TCP differisce, si allinea al TCP e si registra la differenza nella review.
- `ConnGuard::new` chiama `inc_connections()` e il drop chiama `dec_connections()`: per UDP
  `active_connections` = sessioni vive, senza altro codice.
- `ForwarderStats::last_addrs()` è `#[cfg(test)]` a HEAD: va usato solo nei test.
- `run_dns_refresher` ha 8 argomenti, da qui l'`#[allow(clippy::too_many_arguments)]`, come
  `run_forwarder`. `run_udp_forwarder` ne ha 7 e non serve.
- `(&*sock).read(..)` usa `impl Read for &socket2::Socket` (R5). Non sostituirlo con `upstream.try_recv()` (R2).
- Gli errori di compilazione dovuti al borrow checker sui campi disgiunti di `self` in `on_client_datagram`
  (`self.sessions` + `self.gates.send_full` + `self.stats` + `self.label`) sono attesi solo se si passa per un
  metodo `&self`: tenere gli accessi come campi diretti, come nello scheletro.

- **Unit tests:** in `src/udp.rs` `#[cfg(test)] mod tests`, salvo dove indicato.

  Helper di test, privati del modulo:
  - `free_udp_port()`: claim set statico come `manager::tests::free_port`, ma con `std::net::UdpSocket::bind("127.0.0.1:0")`;
  - `system_resolver()`;
  - `spawn_fwd(remote_host, remote_port, settings) -> Fwd { port, stats, knobs, limit, cancel, task }`, che non
    attende;
  - `start_fwd(..)` = `spawn_fwd` + attesa `stats.snapshot().listening` (≤ 3 s);
  - `spawn_udp_echo(bind: &str) -> (u16, Arc<Mutex<HashSet<SocketAddr>>>)`: echo con buffer da 65536 che
    registra i peer;
  - `spawn_udp_tagged_on(addr: SocketAddr, tag: &'static [u8])`: risponde `tag + payload`;
  - `roundtrip(c: &UdpSocket, port, payload, within) -> Option<Vec<u8>>`;
  - `wait_until(cond, within, what)`.

  Il forwarder di test usa `ForwarderStats::with_protocol(cfg.label(), Protocol::Udp)`,
  `RuntimeKnobs::from_settings`, `Semaphore::new(settings.udp_max_sessions)` e `Notifier::disabled()`.

  | Test | Fixture / azione | Asserzioni esatte |
  |------|------------------|-------------------|
  | `forwards_a_datagram_and_its_reply` | echo + forwarder literal; un client invia `b"hello"` | risposta `b"hello"` entro 2 s; poi `datagrams_sent == 1`, `datagrams_received == 1`, `bytes_sent == 5`, `bytes_received == 5`, `active_connections == 1` (attendere fino a 1 s per i contatori) |
  | `first_datagram_of_a_new_session_is_never_dropped` (`#[tokio::test(flavor = "multi_thread", worker_threads = 2)]`) | 100 iterazioni: client **nuovo**, un solo `send_to(format!("first-{i}"))`, **nessun retry** | ognuna riceve la propria eco entro 1 s; a fine test `datagrams_dropped == 0`, `datagrams_sent == 100` |
  | `datagram_boundaries_are_preserved_for_all_sizes` | un client; dimensioni `[0, 1, 100, 1472, 1473, 9000, 60000, 65507]`, payload `(i % 251) as u8`; uno alla volta | ogni risposta ha la stessa lunghezza e lo stesso contenuto; `datagrams_received == 8` |
  | `each_client_gets_its_own_session_and_only_its_replies` (multi_thread) | 8 client concorrenti, ciascuno 10 datagrammi `"c{i}-{k}"` | ogni client riceve solo i propri 10 payload, in qualsiasi ordine; l'echo vede esattamente 8 peer distinti; `active_connections == 8` |
  | `idle_sessions_expire_and_release_their_slot` | `udp_session_timeout_secs: 1`, `udp_max_sessions: 1`; client A round-trip | entro 4 s `active_connections == 0` e `limit.available_permits() == 1`; poi il client B (nuovo) ottiene la sua eco |
  | `session_cap_drops_and_counts_new_flows` | `udp_max_sessions: 2`; client A e B ok; client C invia | C senza risposta entro 500 ms; `rejected_connections == 1`; `datagrams_dropped >= 1`; A riceve ancora la sua eco |
  | `cancel_releases_the_port_before_returning` | 5 sessioni vive; `cancel.cancel()`; `timeout(3 s, task)` | il task termina entro 3 s; subito dopo `std::net::UdpSocket::bind(("127.0.0.1", port))` riesce; `listening == false`; `active_connections == 0`; `limit.available_permits() == 16384` |
  | `sessions_follow_a_dns_change_and_old_ones_close` (`#[cfg(target_os = "linux")]`) | due backend tagged sulla stessa porta P: `127.0.0.1:P` "one" e `127.0.0.2:P` "two" (P da bind di `127.0.0.1:0`, poi bind di `127.0.0.2:P` con retry su conflitto); `spawn_fake_dns(answer=127.0.0.1, ttl=1)`; forwarder verso `backend.test:P` con `resolver_for(dns)` e `dns_timeout_secs: 2` | lo stesso client riceve prima una risposta che inizia con `one` (≤ 5 s); dopo `answer.store(127.0.0.2)` riceve una risposta che inizia con `two` entro 10 s (retry ogni 200 ms); `stats.last_addrs() == Some(vec![127.0.0.2])` |
  | `classify_udp_err_covers_every_kind` | `io::Error::from(kind)` | le 6 righe della tabella D8, più `PermissionDenied` → Fatal e `Other` → Fatal |
  | `bind_conflict_retries_then_listens` | porta occupata da un `std::net::UdpSocket`; `spawn_fwd` | dopo 300 ms `listening == false`; drop dell'occupante; entro 8 s `listening == true` e un round-trip riesce |
  | `session_timeout_is_read_live` | timeout 60; una sessione; `knobs.update(&Settings { udp_session_timeout_secs: 1, ..Settings::default() })` | entro 4 s `active_connections == 0` |
  | `icmp_errors_are_recorded_and_kick_the_monitor` (`#[cfg(target_os = "linux")]`) | remote = `127.0.0.1:{free_udp_port()}` senza nulla in ascolto; un client invia 3 datagrammi a 100 ms | `timeout(3 s, stats.probe_kick().notified())` restituisce Ok; `active_connections == 1` (la sessione non è chiusa dall'ICMP) |
  | `log_gate_reports_suppressed_counts` | `LogGate::default()` | `check(60 s) == Some(0)`, poi tre `None`, poi `check(Duration::ZERO) == Some(3)`, poi `check(Duration::ZERO) == Some(0)` |
  | `same_addr_set_ignores_order` | — | `[a,b]` ≡ `[b,a]`; `[a]` ≢ `[a,b]`; `[a,a]` ≡ `[a]` |

  Fuori da `udp.rs`:
  - `stats::tests::mark_connect_ok_at_is_throttled_but_never_hides_a_reply_after_icmp`:
    1. `ok_at(1000)` → stored 1000;
    2. `ok_at(1050)` → ancora 1000;
    3. `ok_at(1100)` → 1100;
    4. `mark_udp_icmp_error(1120, Port)`, poi `ok_at(1130)` → 1130;
    5. `ok_at(1140)` → 1130.

    Il valore si legge con `#[cfg(test)] fn last_connect_ok_ms_raw(&self) -> u64`.
  - `config::tests::knobs_expose_the_udp_session_timeout`: da settings 7 → 7 s; `update` con 0 → 1 s (clamp).
  - `notify::tests::udp_session_limit_alert_is_rendered_and_throttled`: `kind() == "udp_session_limit_reached"`,
    `severity() == Warning`, `throttle().unwrap().0 == "limit:a:1->b:2/udp"`, il render contiene
    `*UDP session limit reached*` e `(7 refused so far)`. Più le due tabelle esistenti aggiornate (S4).
  - `manager::tests::stop_one_releases_the_udp_port`:
    - manager di default;
    - `start_all(&[ForwarderConfig { protocol: Protocol::Udp, local_port: p, remote: 127.0.0.1:9, .. }])`;
    - attendere `listening` dalla `collect()` del registry;
    - `sync(&[]).await`;
    - subito dopo `std::net::UdpSocket::bind(("127.0.0.1", p))` riesce.
  - Gate: G-U2.1. Discovery: `udp::tests::` = 14 test su Linux (12 altrove), più i 4 nomi esterni.
- **e2e tests:** N/A in 2.1. Il gate D17 impedisce ancora di avviare un UDP dal binario; gli e2e arrivano con lo
  sblocco in 2.2.
- **Done:**
  - `src/udp.rs` conforme allo scheletro;
  - G-HYG e G-U2.1 pass con discovery;
  - doppia review di agent-1:opus registrata (pre-dispatch e post-diff);
  - unità chiusa, commit `sub-phase(2.1)`.

### 2.2 Sblocco, monitor passivo, avvio/shutdown, e2e UDP
- **Model:** agent-2:sonnet
- **Assignment:** implementazione + test unit ed e2e; agent-1:opus fa la review dopo il diff (truth table del
  monitor, rimozione completa del gate, messaggi, budget fd).
- **Files:**
  - READ: `src/monitor.rs` (`run_monitor`, `probe_target`, `probe_all`, `Downtime`, mod tests: `cfg`, `knobs`,
    `next_event`, `wait_reachable`, `Notifier::for_test`); `src/main.rs`; `src/notify.rs` (render `DnsChanged`);
    `tests/common/mod.rs`; `tests/e2e_webhook.rs` (`spawn_webhook_sink`, `expect_message`, `rfw_with_webhook`);
    `tests/e2e_lifecycle.rs`.
  - WRITE: `src/config.rs`, `src/stats.rs`, `src/monitor.rs`, `src/main.rs`, `src/notify.rs`, `src/udp.rs`
    (`ensure_fd_budget`), `Cargo.toml` (`description`), `tests/common/mod.rs`, `tests/e2e_lifecycle.rs`,
    `tests/e2e_webhook.rs`.
  - NEW: `tests/e2e_udp.rs`, con `mod common;` in testa come gli altri e2e. Cargo lo scopre da solo.
- **Change:**
  Preconditions: 2.1 DONE.
  Contract:
  - **Sblocco (D17)**: eliminare `reject_unsupported_protocols`, le sue due chiamate, il test unit
    `udp_forwarders_are_rejected_until_the_engine_lands` e l'e2e `udp_forwarder_is_refused_until_supported`.
    Dopo questo passo `git grep -n "not supported by this build yet" -- src tests` non stampa nulla (exit 1).
  - **stats.rs**:
    ```rust
    impl IcmpKind {
        pub fn as_str(self) -> &'static str {
            match self {
                IcmpKind::PortUnreachable => "port unreachable",
                IcmpKind::HostUnreachable => "host unreachable",
                IcmpKind::NetworkUnreachable => "network unreachable",
            }
        }
        fn from_u8(v: u8) -> Option<Self> {
            match v { 1 => Some(Self::PortUnreachable), 2 => Some(Self::HostUnreachable), 3 => Some(Self::NetworkUnreachable), _ => None }
        }
    }
    /// What the UDP monitor needs from the data path, read in one go.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct UdpSample {
        pub now_ms: u64,
        /// `(ms, kind)` of the latest ICMP error, if any.
        pub icmp: Option<(u64, IcmpKind)>,
        /// ms of the latest recorded reply (`last_connect_ok_ms`), if any.
        pub reply_ms: Option<u64>,
        /// Lifetime client -> target datagrams.
        pub sent: u64,
    }
    pub fn udp_sample(&self) -> UdpSample
    ```
    `udp_sample` legge `udp_icmp_error_ms` (NEVER → `None`) e `udp_icmp_kind` via `from_u8` (`None` → `icmp: None`),
    poi `last_connect_ok_ms` (NEVER → `None`), `datagrams_sent` e `now_ms()`.
  - **monitor.rs**:
    - `const ICMP_QUIET_MS: u64 = 3_000;`, con doc "An ICMP error older than this, with traffic still flowing and
      no newer error, no longer counts: the kernel would have reported a fresh one (ICMP is rate-limited to about
      1/s per destination)".
    - Tipo del verdetto:
      ```rust
      #[derive(Debug, Clone, Copy, PartialEq, Eq)]
      enum UdpVerdict { Up, Down { kind: IcmpKind, age_ms: u64 }, Hold }
      ```
    - `fn udp_verdict(cur: &UdpSample, prev: Option<&UdpSample>) -> UdpVerdict`, le cui righe si valutano **in
      ordine**:

      | # | Condizione | Verdetto |
      |---|-----------|----------|
      | 1 | `cur.icmp == None` | Up |
      | 2 | `cur.reply_ms >= icmp_ms` | Up |
      | 3 | `prev == None` | Down |
      | 4 | `prev.icmp.map(ms) != Some(icmp_ms)`, cioè un ICMP nuovo dall'ultimo tick | Down |
      | 5 | `cur.now_ms - icmp_ms < ICMP_QUIET_MS` | Down |
      | 6 | `cur.sent > prev.sent` | Up |
      | 7 | altrimenti | Hold |

      `Down.age_ms = cur.now_ms.saturating_sub(icmp_ms)`.
    - `pub async fn probe_udp_target(cfg: &ForwarderConfig, resolver: &TokioResolver, dns_timeout: Duration) -> ProbeOutcome`:
      - `resolve_targets` + latenza;
      - `Ok((targets[0], elapsed))`;
      - `addrs` come in `probe_target` (vuoto per un literal);
      - su errore `Err(e.to_string())`.
    - `probe_all`: `let outcome = match cfg.protocol { Protocol::Tcp => probe_target(..).await, Protocol::Udp => probe_udp_target(&cfg, &resolver, dns_timeout).await };`.
    - Refactor senza cambio di comportamento: estrarre dal `match result` di `run_monitor` due funzioni private:
      - `fn record_up(label: &str, target: &str, down: &mut Option<Downtime>, address: Option<SocketAddr>, stats: &ForwarderStats, notifier: &Notifier)`;
      - `fn record_down(label: &str, target: &str, down: &mut Option<Downtime>, error: String, stats: &ForwarderStats, knobs: &RuntimeKnobs, notifier: &Notifier)`.

      Estrarre anche il blocco DnsChanged come `fn report_dns_change(label: &str, host: &str, addrs: &[IpAddr], stats: &ForwarderStats, notifier: &Notifier)`.
      Il ramo TCP le chiama con gli stessi argomenti di prima, con log e alert identici.
    - In testa a `run_monitor`:
      `if cfg.protocol == Protocol::Udp { return run_udp_monitor(cfg, cancel, stats, resolver, knobs, notifier).await; }`.
    - `async fn run_udp_monitor(..)`, stessa firma:
      1. Attesa identica al TCP: intervallo, `min(intervallo, DOWN_PROBE_INTERVAL)` se down, kick con
         `KICK_MIN_SPACING`. Il primo giro è immediato.
      2. **Nessuno skip `recent_ok`.**
      3. `let o = probe_udp_target(&cfg, &resolver, knobs.dns_timeout()).await;`
      4. `report_dns_change` se `o.addrs` non è vuoto.
      5. `let cur = stats.udp_sample();`
      6. Se `o.result` è `Err(e)` → `record_down(.., e, ..)`. Altrimenti, `match udp_verdict(&cur, prev.as_ref())`:

         | Verdetto | Azione |
         |---|---|
         | `Up` | `record_up(.., Some(addr dal probe), ..)` |
         | `Down{kind, age_ms}` | `record_down(.., format!("{} from the target {} ago; no reply since", kind.as_str(), fmt_duration(Duration::from_millis(age_ms))), ..)` |
         | `Hold` | nulla: nessun cambio di stato, nessuna valutazione della soglia |

      7. `prev = Some(cur)` a ogni giro.
  - **notify.rs**: nel render di `DnsChanged`, se `label.ends_with("/udp")`, la seconda frase diventa
    "New UDP sessions now go to the new address; sessions to addresses no longer published were closed.".
    Il TCP resta invariato.
  - **config.rs**: `about` di `CliArgs` → "Rust Forwarder — TCP/UDP port forwarder with hot-reload,
    auto-reconnect, and dynamic DNS". **Cargo.toml** `description` → stessa stringa.
  - **udp.rs**, budget fd (D24):
    ```rust
    /// Headroom for listeners, TCP connections, the log file, DNS and the metrics endpoint.
    #[cfg(unix)]
    const FD_HEADROOM: u64 = 256;

    #[cfg(unix)]
    #[derive(Debug, PartialEq, Eq)]
    enum FdPlan { Enough, Raise { to: u64, sufficient: bool }, Short }

    #[cfg(unix)]
    fn fd_plan(soft: u64, hard: u64, needed: u64) -> FdPlan {
        if soft >= needed { FdPlan::Enough }
        else if hard > soft { let to = hard.min(needed); FdPlan::Raise { to, sufficient: to >= needed } }
        else { FdPlan::Short }
    }

    /// Make room for `udp_max_sessions` upstream sockets: raise the soft
    /// RLIMIT_NOFILE when the hard limit allows, warn when it cannot suffice.
    #[cfg(unix)]
    pub(crate) fn ensure_fd_budget(udp_max_sessions: usize)
    #[cfg(not(unix))]
    pub(crate) fn ensure_fd_budget(_udp_max_sessions: usize) {}
    ```
    - Unix: `needed = udp_max_sessions as u64 + FD_HEADROOM`; `libc::getrlimit(libc::RLIMIT_NOFILE, &mut rl)`
      (errore → `warn` e ritorno).
    - `Raise` → `setrlimit` con `rlim_cur = to` e `rlim_max` invariato:
      - successo → `info!("Raised the open-file soft limit from {soft} to {to} for udp_max_sessions")`;
      - errore → warn.
    - `Raise{sufficient:false}`, `Short` o raise fallito →
      `warn!("Open-file limit {current} is below udp_max_sessions + {FD_HEADROOM} = {needed}; UDP sessions beyond it will fail to open (raise ulimit -n / LimitNOFILE, or lower udp_max_sessions)")`.
    - Cast `as u64` e `as libc::rlim_t` per i tipi di piattaforma.
  - **main.rs**:
    - la riga di avvio diventa `info!("  {} -> {}:{}{}", fwd.local_addr(), fwd.remote_host, fwd.remote_port, if fwd.protocol == Protocol::Udp { " (udp)" } else { "" });`;
    - prima di `ForwarderManager::new`: `if forwarders.iter().any(|f| f.protocol == Protocol::Udp) { udp::ensure_fd_budget(settings.udp_max_sessions); }`;
    - startup probe, ramo `Ok`: se `r.protocol == Protocol::Udp` usare `info!(forwarder, target, via, latency_ms, "Startup probe: target resolved (udp, passive monitoring)")`,
      altrimenti il messaggio TCP invariato;
    - `Stopping`: `.filter(|s| s.protocol == Protocol::Tcp)` prima di `.map(|s| s.active_connections)`. Commento:
      "only TCP connections are drained; UDP sessions end at once".
  - **Usage text** di `main.rs` (bail "No forwarders configured"): l'ultima riga diventa
    "Forwarder format: local_host:local_port:remote_host:remote_port[/tcp|/udp]".

  Scope: il comportamento del monitor TCP è byte-identico, perché il refactor è solo estrazione. Nessun cambio a `udp.rs` oltre `ensure_fd_budget`.
  Steps:
  1. S1 — Sblocco (rimozioni) + `about`/description + usage. Expected: `cargo test --bin rfw config::tests::` verde.
  2. S2 — stats `udp_sample`, monitor (refactor prima, poi UDP). Expected: tutti i `monitor::tests::` esistenti verdi
     **prima** di aggiungere il ramo UDP; poi i nuovi.
  3. S3 — notify DnsChanged, main.rs, `ensure_fd_budget`.
  4. S4 — Harness e2e in `tests/common/mod.rs`:
     ```rust
     pub fn free_udp_port() -> u16;          // UDP bind 127.0.0.1:0, claim nel set condiviso
     pub fn free_dual_port() -> u16;         // libera in TCP *e* UDP sullo stesso numero, claim
     async fn bind_unclaimed_udp() -> tokio::net::UdpSocket;
     pub async fn spawn_udp_echo() -> u16;   // echo, buffer 65536
     pub async fn spawn_udp_echo_on(port: u16); // echo su una porta data (panic se il bind fallisce)
     pub async fn spawn_udp_tagged(tag: &'static [u8]) -> u16; // risponde tag + payload
     pub async fn udp_client() -> tokio::net::UdpSocket;      // bind 127.0.0.1:0
     pub async fn udp_roundtrip(c: &tokio::net::UdpSocket, port: u16, payload: &[u8], within: Duration) -> Option<Vec<u8>>;
     /// Retry "ping" every 100ms for up to 10s until rfw answers; returns the reply.
     pub async fn wait_udp_forwarding(c: &tokio::net::UdpSocket, port: u16) -> Vec<u8>;
     /// Poll every 50ms for up to 5s until binding UDP 127.0.0.1:port succeeds.
     pub async fn wait_udp_port_free(port: u16);
     /// Value of `"field":N` inside the /stats object whose label is `label`.
     pub fn stat_of(body: &str, label: &str, field: &str) -> Option<u64>;
     ```
  5. S5 — e2e (sotto), poi G-HYG + G-U2.2.

  Recovery boundary: dopo S3 (unit verdi), checkpoint §6. Gli e2e sono lunghi da stabilizzare.
  Failure handling:
  - un e2e flaky per timing non si "aggiusta" allungando oltre i tempi indicati né aggiungendo retry
    all'asserzione: va escalato con log (`spawn_rfw_logged`);
  - un `monitor::tests` TCP esistente che cambia esito dopo il refactor significa che il refactor non è puro:
    va annullato e rifatto.
- **Unit tests:**
  - `monitor::tests::udp_verdict_truth_table`: un caso per riga 1–7, più due casi di ordine.
    - Riga 2 batte riga 4: ICMP nuovo ma reply più recente → Up.
    - Riga 6 con `age_ms == 3000` esatti → Up (confine incluso).
    - I `UdpSample` si costruiscono a mano.
  - `monitor::tests::udp_monitor_alerts_on_icmp_and_recovers_on_reply`:
    - cfg Udp `127.0.0.1:9`; stats `with_protocol(.., Udp)`; `knobs(1, 1)`; `Notifier::for_test()`; spawn `run_monitor`;
    - `wait_reachable(Some(true))`;
    - poi ogni 300 ms `stats.mark_udp_icmp_error(stats.now_ms(), IcmpKind::PortUnreachable)`;
    - `next_event` entro 8 s = `TargetUnreachable`, con `last_error` che contiene "port unreachable from the target";
    - si smette con gli ICMP; `stats.mark_connect_ok_at(stats.now_ms())`;
    - `next_event` entro 5 s = `TargetRecovered`.
  - `monitor::tests::udp_monitor_freezes_without_new_evidence`:
    - `knobs(1, 10)`; dopo `wait_reachable(Some(true))`, un solo `mark_udp_icmp_error`;
    - entro 3 s `target_reachable == Some(false)`;
    - nessun evento entro 6 s (Hold dopo 3 s, soglia 10 s mai valutata): `timeout(6 s, rx.recv())` è `Err`;
    - lo stato resta `Some(false)`.
  - `monitor::tests::udp_probe_is_dns_only`: `probe_all(&[cfg Udp 127.0.0.1:9 (nessun listener)], ..)` →
    `outcome == Ok((127.0.0.1:9, _))` e `protocol == Udp`.
  - `stats::tests::udp_sample_reflects_icmp_and_replies`:
    - stats nuovo → `icmp None`, `reply None`, `sent 0`;
    - dopo `mark_udp_icmp_error(50, Host)`, `mark_connect_ok_at(60)` e `add_datagrams_sent(3)` →
      `icmp Some((50, HostUnreachable))`, `reply Some(60)`, `sent 3`.
  - `notify::tests::dns_change_render_mentions_udp_sessions`: label `…/udp` → contiene "New UDP sessions now go
    to the new address"; label TCP → contiene ancora "connections already open stay on the old one".
  - `udp::tests::fd_budget_math` (`#[cfg(unix)]`):

    | Input | Atteso |
    |---|---|
    | `fd_plan(20000, 20000, 16640)` | Enough |
    | `fd_plan(1024, 1048576, 16640)` | `Raise{to:16640, sufficient:true}` |
    | `fd_plan(1024, 4096, 16640)` | `Raise{to:4096, sufficient:false}` |
    | `fd_plan(1024, 1024, 16640)` | Short |
    | `fd_plan(1024, u64::MAX, 16640)` | `Raise{to:16640, sufficient:true}` |

  - Rimossi: `udp_forwarders_are_rejected_until_the_engine_lands`.
  - Gate: G-U2.2 (unit). Discovery: i 7 nomi.
- **e2e tests:** NEW `tests/e2e_udp.rs`. Ogni test è `#[tokio::test]`, salvo dove indicato.
  - Porte rfw: `free_udp_port()` / `free_dual_port()`. Backend dagli helper `common`.
  - Metriche: `--metrics-addr 127.0.0.1:{free_port()}` + `wait_port(metrics)`.
  - Lettura stats: `stat_of(&http_get(m, "/stats").await, label, field)`.
  - Label UDP: `format!("127.0.0.1:{p}->127.0.0.1:{e}/udp")`.

  | Test | Input | Osservazioni attese |
  |------|-------|--------------------|
  | `udp_round_trip_and_metrics` | CLI `127.0.0.1:P:127.0.0.1:E/udp` + metrics | `wait_udp_forwarding`; leggere `n0 = datagrams_sent`; 10 round-trip di payload distinti, ognuno identico; poi entro 3 s `datagrams_sent == n0+10`, `datagrams_received >= 10`, `datagrams_dropped == 0`; `/stats` contiene `"protocol":"udp"`; `/metrics` contiene `rfw_datagrams_sent_total{label="<label>"}` e `rfw_listening{label="<label>"} 1` |
  | `env_udp_forwarder_works` | `spawn_rfw_logged(&[], &[("RFW_FORWARDER_1", "127.0.0.1:P:127.0.0.1:E/udp")])` | `wait_udp_forwarding` restituisce `ping` |
  | `yaml_protocol_udp_and_hot_reload_retarget` | YAML `protocol: udp`, P → tagged `A`; client persistente | la risposta inizia con `A`; riscrittura del remote → tagged `B`; lo **stesso** client riceve una risposta che inizia con `B` entro 15 s (retry ogni 200 ms) |
  | `tcp_and_udp_share_a_port_independently` | P = `free_dual_port()`; YAML con due forwarder su P: tcp → `spawn_echo`, udp → `spawn_udp_tagged(b"U")` | TCP echo ok e UDP risponde `U…`; riscrittura del file senza la voce udp → entro 10 s `/stats` non contiene più la label udp, `wait_udp_port_free(P)` ok, il TCP fa ancora echo |
  | `duplicate_udp_forwarder_is_rejected` | CLI `127.0.0.1:P:127.0.0.1:1/udp 127.0.0.1:P:127.0.0.1:2/udp` (`rfw_command` + `output()`) | exit non zero; stderr contiene `Duplicate local address '127.0.0.1:{P}/udp'` |
  | `idle_session_expires` | `--udp-session-timeout 1` + metrics | dopo un round-trip `active_connections == 1`; entro 5 s `== 0` |
  | `udp_max_sessions_is_enforced_and_counted` | `--udp-max-sessions 1` + metrics | client A ok; client B senza risposta entro 1 s; `rejected_connections >= 1`; A ancora ok |
  | `health_reports_503_while_the_udp_port_is_taken` | `std::net::UdpSocket` occupa P; rfw UDP su P + metrics | `GET /` contiene `503` e la label; drop dell'occupante; entro 10 s `GET /` contiene `200 OK` |
  | `sigterm_exits_fast_with_idle_udp_sessions` (`#[cfg(unix)]`) | 50 client, un round-trip ciascuno, + metrics | `active_connections == 50`; `guard.terminate_and_wait(3 s) == true` |
  | `udp_drops_are_rate_limited_in_logs` | (a) `spawn_rfw_logged` con `--udp-max-sessions 1`: client A ok, poi 50 client nuovi un datagramma ciascuno in 2 s; (b) remote `nonexistent.invalid` con 50 datagrammi in 2 s | (a) `count("udp_max_sessions saturated") == 1` e `count("UDP session rejected") == 0` (è a debug); (b) `count("UDP target not resolved yet") == 1`, `count("UDP target DNS resolution failed") <= 1` |
  | `closed_udp_target_is_reported_unreachable` | remote `127.0.0.1:Q` con `Q = free_udp_port()` non in ascolto; `--probe-interval 1` + metrics; client che invia ogni 200 ms | entro 10 s `/metrics` contiene `rfw_target_reachable{label="<label>"} 0`; poi `spawn_udp_echo_on(Q)`; entro 10 s `rfw_target_reachable{label="<label>"} 1` |
  | `reference_scenario_tcp_and_udp_side_by_side` | lo scenario di overview.md, punti 1–6 | tutte le asserzioni elencate lì, incluse le label esatte e `rfw_datagrams_sent_total{label="127.0.0.1:P->127.0.0.1:E_udp/udp"} 1`, l'assenza di `rfw_datagrams_sent_total{label="127.0.0.1:P->127.0.0.1:E_tcp"}`, e SIGTERM ≤ 3 s (`#[cfg(unix)]` per quel passo) |
  | `udp_bench` (`#[ignore]`) | build release; vedi G-UDP-BENCH | stampa righe `UDP_BENCH …` (formato sotto); asserisce `windowed_loss == 0` e `p50_overhead_us <= 200` |

  `udp_bench`, i tre scenari:
  1. **latenza**: 2000 round-trip sequenziali da 64 B, prima diretti verso l'echo poi via rfw. Stampa
     `UDP_BENCH latency direct_p50_us=… direct_p99_us=… rfw_p50_us=… rfw_p99_us=… p50_overhead_us=…`.
  2. **windowed**: 4 client, ognuno con 16 datagrammi da 1200 B in volo (64 in totale; invio di uno nuovo per
     ogni eco), per 5 s. La finestra resta sotto i buffer socket di default (circa 2,3 KB di truesize per
     datagramma × 64 ≈ 150 KB < 212 992 di `rmem_default`), così una perdita indica un difetto di rfw e non un
     overflow previsto.
     Stampa `UDP_BENCH windowed pps=… mbit_s=… windowed_loss=…`. La perdita si misura alla fine: inviati meno
     ricevuti, dopo 1 s di drain.
  3. **blast**: 4 client inviano 50 000 datagrammi da 1200 B ciascuno senza attendere. Stampa
     `UDP_BENCH blast sent=… echoed=… loss_pct=…`. Solo registrazione, nessuna asserzione.

  `tests/e2e_webhook.rs` (helper esistenti):
  - `startup_reports_udp_targets_as_resolved`: un forwarder UDP verso `spawn_udp_echo` e uno TCP verso
    `spawn_echo`. Il messaggio di avvio contiene `udp: passive check` e `resolved to` e non contiene `unreachable`.
  - `udp_outage_is_alerted_and_recovery_reported`:
    - remote `127.0.0.1:Q` non in ascolto; `--probe-interval 1 --unreachable-after 2`; client che invia ogni 200 ms;
    - atteso un messaggio con `*Target unreachable*` e `port unreachable from the target`;
    - poi `spawn_udp_echo_on(Q)`; atteso `*Target reachable again*` entro 15 s.

  `tests/e2e_lifecycle.rs`: rimuovere `udp_forwarder_is_refused_until_supported`.

  Gate: G-U2.2 (e2e). Discovery: `e2e_udp` = 12 test eseguiti (+1 ignored, `udp_bench`) su Linux;
  `e2e_webhook` = 8 + 2.
- **Done:**
  - gate rimosso;
  - monitor UDP conforme alla truth table;
  - main e alert aggiornati;
  - e2e verdi;
  - G-HYG e G-U2.2 pass con discovery;
  - review di agent-1:opus;
  - unità chiusa, commit `sub-phase(2.2)`.

### 2.3 Documentazione UDP (README, forwarders.yml)
- **Model:** agent-3:haiku
- **Assignment:** solo documentazione + l'asserzione del test sul file di esempio; agent-1:opus verifica
  l'accuratezza a P2.
- **Files:**
  - READ: questo file (decisioni D*), `README.md`, `forwarders.yml`, `src/config.rs` (test
    `shipped_example_config_parses_and_validates`).
  - WRITE: `README.md`, `forwarders.yml`, `src/config.rs` (solo quel test).
  - NEW: nessuno.
- **Change:**
  Preconditions: 2.2 DONE. Lingua: inglese, stile e struttura esistenti. Non inventare comportamenti: ogni frase
  deve corrispondere a una decisione D* o a un messaggio di log/metrica di questo file.
  Steps:
  1. S1 — README, sezioni (le ancore sono i titoli esistenti):
     - **Features**: il bullet "TCP forwarding" diventa "**TCP and UDP forwarding** — Bidirectional proxy;
       UDP keeps one upstream socket per client address (session), preserving datagram boundaries".
       Nuovo bullet "**UDP sessions** — idle timeout (`udp_session_timeout_secs`, default 60s, hot-reloadable),
       global cap `udp_max_sessions` (default 16384), drops counted, never blocking other clients".
     - **Forwarder syntax**: il blocco diventa `local_host:local_port:remote_host:remote_port[/tcp|/udp]`.
       - Riga di tabella `/tcp`, `/udp`: "Optional protocol suffix (case-insensitive); default `tcp`".
       - Esempio `0.0.0.0:5353:1.1.1.1:53/udp   → DNS over UDP`.
       - Nota: "A TCP and a UDP forwarder may share the same local port; two UDP (or two TCP) forwarders may not."
     - **2. YAML config file**: aggiungere all'esempio una voce con `protocol: udp`. Nota sullo spelling esatto della
       chiave: una chiave sconosciuta viene ignorata e il forwarder resta TCP (rischio noto).
     - **3. Environment variables**: esempio `RFW_FORWARDER_3=0.0.0.0:5353:1.1.1.1:53/udp`.
     - **Full CLI reference**: le righe `--udp-session-timeout <SECS>` e `--udp-max-sessions <N>`, con il formato
       della tabella o del blocco esistente.
     - **Hot-reload**: aggiungere alla lista delle chiavi live `udp_session_timeout_secs`; `udp_max_sessions` va
       tra quelle "require a restart". Re-target UDP: "the listener is rebuilt on the same port; existing sessions
       are closed and the client's next datagram opens a new one".
     - **How it works**: nuova sottosezione "UDP", con:
       - sessione per client address, connected upstream socket;
       - batch di 64 datagrammi;
       - mai attesa sul percorso client→target (full buffer = drop contato);
       - un task per sessione per le risposte;
       - sweep 1 s;
       - DNS refresh 1 s con chiusura delle sessioni verso indirizzi non più pubblicati;
       - stop entro 2 s.
     - **Performance & tuning**: sottosezione "UDP":
       - `socket_buffer_bytes` vale per il listener (al bind) e per i nuovi upstream;
       - il warn "raise net.core.rmem_max";
       - `sysctl net.core.rmem_max/wmem_max`;
       - `netstat -su` / `nstat` per `RcvbufErrors`;
       - fd: una per sessione; rfw alza il soft limit fino a `udp_max_sessions + 256` se l'hard limit lo consente,
         altrimenti warn (`LimitNOFILE`);
       - limitazione: con bind wildcard su host multi-homed la risposta può partire da un IP sorgente diverso
         (niente IP_PKTINFO): bindare l'indirizzo specifico.
     - **Logging**: le righe UDP a info/warn (`Listening (udp) on …`, i warn rate-limited con `suppressed`);
       apertura e chiusura sessione a `debug` (`RUST_LOG=rfw=debug`).
     - **Traffic report**: esempio di riga UDP con `sessions=… dgrams tx=… rx=… drop=…`.
     - **Bandwidth metrics endpoint**:
       - famiglie `rfw_datagrams_sent_total`, `rfw_datagrams_received_total`, `rfw_datagrams_dropped_total`
         (solo label UDP);
       - per UDP, `rfw_active_connections` = sessioni e `rfw_rejected_connections_total` = datagrammi di client
         nuovi rifiutati dal cap;
       - JSON: `"protocol"` per ogni voce + i tre campi datagram per le voci UDP.
     - **Alerting** → "How the target is probed": paragrafo UDP:
       - nessun pacchetto viene inviato al target; probe = solo DNS;
       - unreachable quando le sessioni ricevono ICMP port/host/network unreachable e nessuna risposta è arrivata
         dopo; recovered alla prima risposta;
       - senza traffico lo stato resta quello che era;
       - il messaggio di avvio dice "resolved to … (udp: passive check)".

       Nuovo alert "UDP session limit reached".
     - **Development → Test**: riga `tests/e2e_udp.rs` nella tabella, con la copertura dei suoi test; il conteggio
       "five parts" diventa "six parts"; menzione di `cargo test --release --test e2e_udp udp_bench -- --ignored --nocapture`.
     - **Project structure**: `src/udp.rs  # UDP forwarding engine (sessions, DNS refresh, fd budget)` e
       `tests/e2e_udp.rs # UDP forwarding, sessions, reload, monitor`.
  2. S2 — `forwarders.yml`: quarta voce
     ```yaml
       # Forward UDP port 5353 to a DNS resolver (one upstream socket per client)
       - local_host: "0.0.0.0"
         local_port: 5353
         remote_host: "1.1.1.1"
         remote_port: 53
         protocol: udp
     ```
     Nel blocco settings, voci commentate `# udp_session_timeout_secs: 60` (con nota "hot-reloadable") e
     `# udp_max_sessions: 16384` (con nota "startup only"); aggiornare l'elenco delle flag CLI nel commento.
  3. S3 — `config::tests::shipped_example_config_parses_and_validates`: il conteggio atteso passa da 3 a 4,
     aggiungendo `assert_eq!(cfg.forwarders.iter().filter(|f| f.protocol == Protocol::Udp).count(), 1)`.
     Expected: `cargo test --bin rfw config::tests::shipped_example_config_parses_and_validates` pass.
  Recovery boundary: nessuno.
  Failure handling: se un comportamento da documentare non è chiaro in questo file, chiedere ad agent-1:opus;
  non dedurlo dal codice.
- **Unit tests:** solo `shipped_example_config_parses_and_validates` aggiornato (S3), gate G-U2.3.
- **e2e tests:** N/A. È solo documentazione; il file di esempio è coperto dal test unit.
- **Done:**
  - sezioni aggiornate;
  - G-HYG e G-U2.3 pass;
  - unità chiusa, commit `sub-phase(2.3)`;
  - accuratezza verificata da agent-1:opus a P2.

## Gate di fase e chiusura
- Gate richiesti: G-PHASE, G-XPLAT, G-PERF-TCP, G-UDP-BENCH (STATE.md §3).
- Asserzioni:
  - tutti i test verdi in debug e release, con discovery (unit + 6 e2e target);
  - G-XPLAT: `cargo check --all-targets` Windows GNU e build release dei due Darwin → exit 0;
  - G-PERF-TCP: mediana ≤ 1,15 × G-PERF-TCP-BASE;
  - G-UDP-BENCH: `windowed_loss == 0`, `p50_overhead_us <= 200`, numeri registrati in §7.
- Obbligo README: unità 2.3. P2 verifica ogni sezione rispetto al comportamento (log, metriche, flag) e confronta
  il testo con i messaggi esatti di `src/udp.rs`/`src/main.rs`.
- agent-1:opus fa la review di I-2…I-9, I-11, I-14, I-15, dell'assenza del gate e della documentazione.
- Aprire P2, registrare review e risultati, segnare la fase DONE, commit `phase(2)`.
