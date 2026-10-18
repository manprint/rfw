# Fase 3 — Fast path GRO/GSO opt-in (Linux)

Intent: con `udp_gro_gso: true` (YAML), `--udp-gro-gso` (CLI) o un hot reload, rfw su Linux riceve i burst
coalescati (UDP_GRO) e li inoltra con una sola `sendmsg` segmentata (UDP_SEGMENT). Questo riduce le syscall per
datagramma e lascia **identica** la sequenza di datagrammi vista da client e target (I-2, I-12).

Il default resta spento; su piattaforme diverse da Linux l'impostazione è ignorata con un warn. Il beneficio è
misurato (G-GSO-BENCH). La fase chiude il piano (G-FINAL).

Prerequisiti: P2 DONE.
Chiusura di fase: P3, review di agent-1:opus. È l'ultima fase: G-FINAL.

## Contratto di stato e ownership
Leggere prima STATE.md §0–1.
- Aprire l'unità prima degli edit e fare checkpoint ai confini indicati.
- Un worker delegato lavora solo nella sua unità OPEN.
- Il design mancante, o due fix falliti sullo stesso problema, vanno ad agent-1:opus.

## Contesto di design locale
Plan revision: 1.

### Decisioni (significato locale)
- **D18 — superficie.**
  - Setting `udp_gro_gso: bool`, `#[serde(default)]` = false.
  - CLI `--udp-gro-gso`: flag booleano con lo stesso pattern di `--splice`/`use_splice`. Se presente forza
    `true`; assente non tocca il file.
  - Knob `RuntimeKnobs::udp_gro_gso()`, hot reload.
  - **Listener**: il cambio si applica al primo tick di sweep (≤ 1 s) e all'avvio del motore.
  - **Sessioni**: la modalità si fissa alla creazione; quelle esistenti la tengono finché non si chiudono.
  - **Non Linux**: warn all'avvio `"udp_gro_gso is only supported on Linux; ignored"`, nessun altro effetto.
- **D19 — modulo `offload`** (`#[cfg(target_os = "linux")] mod offload` dentro `src/udp.rs`):
  - `recvmsg`/`sendmsg` libc scritti a mano, con cmsg `SOL_UDP`/`UDP_GRO` (in ricezione, `int`) e
    `SOL_UDP`/`UDP_SEGMENT` (in invio, `u16`);
  - cap `MAX_GSO_SEGMENTS = 64` segmenti e `GSO_MAX_PAYLOAD = 65507` byte per singola send;
  - send GSO fallita con EINVAL/EIO/EMSGSIZE → il buffer corrente viene diviso in userspace e GSO resta
    disabilitato **sticky** per quel socket, con un warn.
- **D21 — buffer.**
  - Su Linux il buffer del listener è `GRO_BUF_BYTES = 128 KiB`: 64 segmenti × 1472 = 94 208, più margine.
  - Le sessioni GRO usano un buffer thread-local separato `GRO_REPLY_BUF` (128 KiB, allocato lazy al primo uso
    su quel worker).
  - `MSG_TRUNC` in `msg_flags` → buffer scartato, drop contato pari ai segmenti visibili, warn rate-limited.
- **D31 — "coalescato".** Un buffer ricevuto di `len` byte con `segment` (gso_size dal cmsg, 0 se assente) è
  coalescato **solo** se `0 < segment < len`.
  - Tutti i segmenti sono lunghi `segment`, tranne l'ultimo che può essere più corto (garanzia GRO del kernel, R1).
  - Lo split è `buf.chunks(segment)`.
  - Un buffer non coalescato è un singolo datagramma, anche di lunghezza 0: segue esattamente il percorso della fase 2.
- **D32a (rev 3, supera la parte "spegnimento" di D32) — UDP_GRO non viene mai tolto da un socket.** Verificato in 3.1
  sul kernel 7.0: il cmsg UDP_GRO viene allegato alla *lettura* solo se il flag è attivo in quel momento, mentre
  la segmentazione per socket senza GRO avviene all'*accodamento*. Uno skb accodato con GRO attivo e letto dopo
  `set_gro(false)` arriva fuso e **senza** cmsg (`segment == 0`): nessuna ricezione può ricostruire i confini.
  Quindi: knob off ⇒ nessun nuovo socket riceve UDP_GRO (sessioni nuove), ma il listener che lo ha già lo tiene
  fino al restart, con ricezione GRO-aware e inoltro dei burst invariati. Il messaggio di spegnimento lo dice.
- **D32 — ricezione GRO-aware sticky.**
  - Gli skb accodati mentre UDP_GRO era attivo restano coalescati nella coda anche dopo averlo spento
    (inferenza del supervisore da R1: la segmentazione avviene all'accodamento). Quindi, una volta che un socket
    ha avuto UDP_GRO attivo, **ogni** sua ricezione passa da `offload::recv` con parsing cmsg, fino alla
    chiusura del socket.
  - Listener: `gro_aware` diventa true **prima** di impostare UDP_GRO e non torna mai false.
  - Sessione: `gro` è fissato alla creazione.
  - Un `recv_from` normale su un socket che ha avuto GRO fonderebbe i datagrammi (viola I-2): è vietato.
- **D33 — GSO solo su ciò che GRO ha coalescato.** rfw non coalescia mai in userspace: invia con UDP_SEGMENT
  solo un buffer ricevuto coalescato, con lo stesso `segment`. Ne segue che la sequenza in uscita è identica a
  quella in ingresso.
- **D34 — chunking della send GSO.**
  - Una send porta al massimo `chunk_bytes(segment) = segment × clamp(GSO_MAX_PAYLOAD / segment, 1, MAX_GSO_SEGMENTS)` byte.
  - I chunk partono sempre da un multiplo di `segment`.
  - Un chunk lungo ≤ `segment` (la coda) si invia senza cmsg.
  - Senza chunking, un client su kernel recenti (128 segmenti GSO) o un aggregato GRO più grande di 65507
    farebbe fallire la send e scatterebbe il fallback.
- **D35 — percorsi per direzione** (solo Linux, solo quando il buffer è coalescato):

  | Direzione | Dove gira | Send | Stato GSO | WouldBlock |
  |---|---|---|---|---|
  | client→target | task listener (I-4: mai await) | raw `offload::send(fd, chunk, seg, None)` sul socket upstream connesso, senza `try_io` (I-14) | `SessionEntry.gso_ok: bool` (owner listener) | segmenti residui scartati e contati, warn `send_full` |
  | target→client | task di sessione | `offload::send(fd, chunk, seg, Some(&client))` dentro `listener.try_io(Interest::WRITABLE, …)` (vedi `reply_coalesced` nello scheletro B) | `Arc<AtomicBool>` condiviso dal listener (`ListenerOffload.gso_ok`), clonato in ogni `SessionShared` | residuo in `Pending { data, segment }`, poi slow path |

  Lo slow path divide per `segment` e fa `send_to().await` di ogni pezzo, in `select!` con cancel. Lo slow path
  non usa mai GSO.
- **D36 — contatori.**
  - `datagrams_*` contano **segmenti**, `bytes_*` contano byte payload.
  - Un client nuovo rifiutato con un buffer coalescato di N segmenti conta N in `datagrams_dropped` e N in
    `rejected_connections` (nuovo `ForwarderStats::add_rejected(n)`; `open_session` riceve `datagrams: u64`, con 1
    dalla fase 2).
- **D37 — kernel senza UDP_GRO** (ENOPROTOOPT, kernel < 5.0):
  - `ListenerOffload.unsupported = true`;
  - warn una volta per forwarder: `"UDP GRO is not supported by this kernel (Linux 5.0+ required); udp_gro_gso has no effect"`;
  - nessun altro tentativo, né sul listener né sulle nuove sessioni.

### Invarianti di questa fase
- I-2: confini dei datagrammi 1:1 anche con GRO/GSO (`gro_gso_path_preserves_datagram_boundaries`, e2e
  `udp_gro_gso_flag_preserves_datagrams_byte_exact`).
- I-4: il listener non fa await sul percorso client→target. `forward_upstream_coalesced` è sincrono.
- I-12:
  - spento di default; il percorso di default è il codice della fase 2, invariato salvo le estrazioni elencate;
  - acceso, produce la stessa sequenza di datagrammi;
  - toggle a caldo senza fusioni (`gro_toggle_never_merges_datagrams`, e2e `udp_gro_gso_flag_hot_reload_toggles`).
- I-14: anche con GRO il primo datagramma (o burst) di una sessione nuova non si perde: send upstream raw.

### Evidenze di ricerca applicate (dettagli in overview.md R1, R5)
- **R1** (sorgenti kernel, documentato):
  - il cmsg GRO in ricezione è un `int` gso_size; il cmsg UDP_SEGMENT in invio è un `u16`;
  - GRO coalescia al massimo 64 segmenti uguali più una coda più corta;
  - la send GSO fallisce con EMSGSIZE/EINVAL (segmento > fragsize, troppi segmenti, `sk_no_check_tx`) o EIO
    (xfrm);
  - con UDP_GRO attivo, su loopback gli skb GSO arrivano intatti con il cmsg. Per questo i test usano un mittente
    GSO su loopback.
  - `SO_NO_CHECK=1` su un socket IPv4 rende EINVAL ogni send GSO: è il modo deterministico di forzare il
    fallback nei test.
- **R5** (registry locale, letto):
  - `socket2 0.6.5`: `unsafe fn SockAddr::try_init<F, T>(init: F) -> io::Result<(T, SockAddr)>` con
    `F: FnOnce(*mut SockAddrStorage, *mut socklen_t) -> io::Result<T>`; `SockAddr::as_ptr() -> *const SockAddrStorage`;
    `len() -> socklen_t`; `as_socket()`; `From<SocketAddr>`;
  - `libc 0.2.186`: `UDP_SEGMENT = 103`, `UDP_GRO = 104`, `SOL_UDP = 17`, `SO_NO_CHECK = 11`, `CMSG_*`, `MSG_TRUNC`.

## Sub-phase

### 3.1 Modulo `offload`, wiring nel motore, setting/knob/CLI
- **Model:** agent-2:sonnet
- **Assignment:** implementazione + test unit delle primitive. agent-1:opus fa la review dopo il diff su:
  - ogni blocco `unsafe` (puntatori, lunghezze, allineamento cmsg);
  - D32 sticky;
  - D34 chunking;
  - assenza di await nel listener;
  - percorso di default invariato.
- **Files:**
  - READ: `src/udp.rs` (tutto, come prodotto da 2.1/2.2); `src/config.rs` (`Settings`, `default_use_splice`,
    `use_splice` in `CliArgs`/`apply_cli_overrides`/`RuntimeKnobs`); `src/stats.rs` (`inc_rejected`);
    `src/main.rs` (blocco di avvio vicino a `ensure_fd_budget`).
  - WRITE: `src/udp.rs`, `src/config.rs`, `src/stats.rs`, `src/main.rs`.
  - NEW: nessun file. `mod offload` è inline in `src/udp.rs`, sotto i tipi esistenti e prima di `#[cfg(test)] mod tests`.
- **Change:**
  Preconditions: P2 DONE. A HEAD `src/udp.rs` contiene `drain_listener`, `on_client_datagram`, `open_session`,
  `forward_upstream`, `drain_upstream`, `run_session` e `SessionDrain::Blocked(Vec<u8>)` come nello scheletro
  della fase 2.
  Contract: D18, D19, D21, D31–D37 e lo scheletro qui sotto (normativo per nomi, firme, messaggi e ordine delle
  operazioni).
  Scope:
  - Su Linux con il flag spento, ogni ricezione e invio resta quello della fase 2.
  - Unici cambi al codice della fase 2:
    1. estrazione di `note_upstream_send_error` da `forward_upstream`, a comportamento identico;
    2. `open_session(.., datagrams: u64)`;
    3. `Blocked(Pending)` al posto di `Blocked(Vec<u8>)`;
    4. dispatch cfg in `Engine::run` e `run_session`;
    5. buffer listener `LISTENER_BUF_BYTES`.
  - Nessun cambio a TCP.
  Steps:
  1. S1 — `src/config.rs`:
     - `Settings.udp_gro_gso: bool` con `#[serde(default)]` e doc "Linux only: receive coalesced UDP bursts
       (UDP_GRO) and forward them with segmented sends (UDP_SEGMENT). Hot-reloadable";
     - default `false` in `impl Default`;
     - `CliArgs`: `/// Enable the Linux UDP GRO/GSO fast path (same as `udp_gro_gso: true`).`, poi
       `#[arg(long = "udp-gro-gso")] pub udp_gro_gso: bool`;
     - `apply_cli_overrides`: `if cli.udp_gro_gso { settings.udp_gro_gso = true; }`;
     - `RuntimeKnobs`: campo `udp_gro_gso: AtomicBool`, inizializzato da `s.udp_gro_gso` in `from_settings` e
       aggiornato in `update`;
     - getter, preceduto da `#[cfg_attr(not(target_os = "linux"), allow(dead_code))]`:
       ```rust
       /// Linux UDP GRO/GSO fast path (applied by listeners at their next sweep, by sessions at creation).
       pub fn udp_gro_gso(&self) -> bool { self.udp_gro_gso.load(Ordering::Relaxed) }
       ```
  2. S2 — `src/stats.rs`: `pub fn add_rejected(&self, n: u64) { self.rejected_connections.fetch_add(n, Ordering::Relaxed); }`.
  3. S3 — `src/main.rs`, subito dopo la chiamata a `udp::ensure_fd_budget` (stesso `if` su "c'è un forwarder UDP"):
     ```rust
     #[cfg(not(target_os = "linux"))]
     if settings.udp_gro_gso {
         warn!("udp_gro_gso is only supported on Linux; ignored");
     }
     ```
  4. S4 — `src/udp.rs`: modulo `offload` + helper (scheletro A).
  5. S5 — `src/udp.rs`: wiring nel motore (scheletro B). Expected:
     - `cargo build --all-targets` senza warning su Linux;
     - tutti i `udp::tests::` della fase 2 verdi **senza modifiche ai test**.
  6. S6 — test unit (sotto), poi G-HYG + G-U3.1.

  Recovery boundary: dopo S4 (modulo che compila, test delle primitive verdi), checkpoint §6.
  Failure handling:
  - **EINVAL dalla prima send GSO nei test loopback senza `SO_NO_CHECK`**: verificare `cmsg_len = CMSG_LEN(2)`,
    `msg_controllen = CMSG_SPACE(2)` e che `segment < chunk.len()`. Non disattivare il test.
  - **`ENOPROTOOPT` su `set_gro`** sul kernel di sviluppo (7.0): impossibile; escalare.
  - **Datagrammi fusi in qualunque test**: c'è una ricezione non GRO-aware su un socket con GRO. Rivedere il
    dispatch D32; non aggiungere workaround.
  - Altro design → agent-1:opus.

#### Scheletro A — `mod offload` e helper (normativo)

```rust
/// Listener receive buffer: room for a GRO-coalesced burst on Linux.
#[cfg(target_os = "linux")]
const LISTENER_BUF_BYTES: usize = offload::GRO_BUF_BYTES;
#[cfg(not(target_os = "linux"))]
const LISTENER_BUF_BYTES: usize = UDP_BUF_BYTES;

#[cfg(target_os = "linux")]
thread_local! {
    /// Reply buffer for sessions whose upstream socket has UDP_GRO (lazily
    /// allocated on first use by each worker thread).
    static GRO_REPLY_BUF: RefCell<Vec<u8>> = RefCell::new(vec![0_u8; offload::GRO_BUF_BYTES]);
}

/// Linux UDP segmentation offload: GRO on receive, GSO on send.
#[cfg(target_os = "linux")]
mod offload {
    use std::io;
    use std::mem;
    use std::net::SocketAddr;
    use std::os::fd::RawFd;
    use std::ptr;

    /// Most segments per GSO send (64 on older kernels, 128 on newer ones).
    pub(super) const MAX_GSO_SEGMENTS: usize = 64;
    /// Largest UDP payload one send may carry (the IPv4 bound; IPv6 allows 65527).
    pub(super) const GSO_MAX_PAYLOAD: usize = 65_507;
    /// Receive buffer when GRO may coalesce (64 x 1472 = 94 208, plus headroom).
    pub(super) const GRO_BUF_BYTES: usize = 128 * 1024;

    /// Control-message buffer, aligned for `cmsghdr`.
    #[repr(C, align(8))]
    struct CmsgBuf([u8; 64]);

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) struct Recv {
        pub len: usize,
        /// gso_size from the UDP_GRO cmsg; 0 when absent.
        pub segment: usize,
        /// Source address (only when asked for).
        pub from: Option<SocketAddr>,
        /// The kernel cut the buffer short (MSG_TRUNC): its tail is lost.
        pub truncated: bool,
    }

    /// A buffer of `len` bytes is a GRO burst only when `0 < segment < len`.
    pub(super) fn is_coalesced(len: usize, segment: usize) -> bool {
        segment > 0 && segment < len
    }

    /// Datagrams carried by a buffer (a non-coalesced buffer, even empty, is one).
    pub(super) fn segments(len: usize, segment: usize) -> u64 {
        if is_coalesced(len, segment) { len.div_ceil(segment) as u64 } else { 1 }
    }

    /// Bytes per GSO send: whole segments, at most MAX_GSO_SEGMENTS and GSO_MAX_PAYLOAD.
    pub(super) fn chunk_bytes(segment: usize) -> usize {
        segment * (GSO_MAX_PAYLOAD / segment).clamp(1, MAX_GSO_SEGMENTS)
    }

    /// EINVAL / EIO / EMSGSIZE from a segmented send: GSO is unusable on this
    /// socket or route; send one datagram at a time from now on.
    pub(super) fn is_fallback_error(e: &io::Error) -> bool {
        matches!(e.raw_os_error(), Some(libc::EINVAL) | Some(libc::EIO) | Some(libc::EMSGSIZE))
    }

    pub(super) fn set_gro(fd: RawFd, on: bool) -> io::Result<()> {
        let v: libc::c_int = on.into();
        // SAFETY: `v` outlives the call and the length matches its type.
        let rc = unsafe {
            libc::setsockopt(
                fd,
                libc::SOL_UDP,
                libc::UDP_GRO,
                (&v as *const libc::c_int).cast(),
                mem::size_of::<libc::c_int>() as libc::socklen_t,
            )
        };
        if rc == 0 { Ok(()) } else { Err(io::Error::last_os_error()) }
    }

    /// One `recvmsg`, parsing the UDP_GRO cmsg. Non-blocking sockets only;
    /// call it inside `try_io` so tokio clears readiness on WouldBlock.
    pub(super) fn recv(fd: RawFd, buf: &mut [u8], want_from: bool) -> io::Result<Recv> {
        let mut cmsg = CmsgBuf([0; 64]);
        let mut iov = libc::iovec { iov_base: buf.as_mut_ptr().cast(), iov_len: buf.len() };
        // SAFETY: `msghdr` is plain old data; all-zero is a valid empty header.
        let mut msg: libc::msghdr = unsafe { mem::zeroed() };
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;
        msg.msg_control = cmsg.0.as_mut_ptr().cast();
        msg.msg_controllen = cmsg.0.len() as _;
        let (len, from) = if want_from {
            // SAFETY: `try_init` passes zeroed storage and its size; the kernel
            // writes at most `msg_namelen` bytes there and reports the length.
            let (n, addr) = unsafe {
                socket2::SockAddr::try_init(|storage, len| {
                    msg.msg_name = storage.cast();
                    msg.msg_namelen = *len;
                    let n = libc::recvmsg(fd, &mut msg, 0);
                    if n < 0 {
                        return Err(io::Error::last_os_error());
                    }
                    *len = msg.msg_namelen;
                    Ok(n as usize)
                })
            }?;
            (n, addr.as_socket())
        } else {
            // SAFETY: every pointer in `msg` refers to a live local buffer.
            let n = unsafe { libc::recvmsg(fd, &mut msg, 0) };
            if n < 0 {
                return Err(io::Error::last_os_error());
            }
            (n as usize, None)
        };
        let mut segment = 0;
        // SAFETY: the CMSG_* walk stays within `msg_control`/`msg_controllen`
        // as updated by the kernel; the payload is read unaligned.
        unsafe {
            let mut c = libc::CMSG_FIRSTHDR(&msg);
            while !c.is_null() {
                if (*c).cmsg_level == libc::SOL_UDP && (*c).cmsg_type == libc::UDP_GRO {
                    let v = ptr::read_unaligned(libc::CMSG_DATA(c).cast::<libc::c_int>());
                    segment = usize::try_from(v).unwrap_or(0);
                }
                c = libc::CMSG_NXTHDR(&msg, c);
            }
        }
        Ok(Recv { len, segment, from, truncated: msg.msg_flags & libc::MSG_TRUNC != 0 })
    }

    /// One `sendmsg`. `segment > 0` adds a UDP_SEGMENT cmsg (GSO); `to` is
    /// `None` on a connected socket.
    pub(super) fn send(fd: RawFd, buf: &[u8], segment: u16, to: Option<&socket2::SockAddr>) -> io::Result<usize> {
        let mut cmsg = CmsgBuf([0; 64]);
        let mut iov = libc::iovec { iov_base: buf.as_ptr() as *mut libc::c_void, iov_len: buf.len() };
        // SAFETY: see `recv`.
        let mut msg: libc::msghdr = unsafe { mem::zeroed() };
        if let Some(addr) = to {
            msg.msg_name = addr.as_ptr() as *mut libc::c_void;
            msg.msg_namelen = addr.len();
        }
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;
        if segment > 0 {
            // SAFETY: 64 aligned bytes hold CMSG_SPACE(2); the header pointer
            // comes from CMSG_FIRSTHDR on that buffer.
            unsafe {
                msg.msg_control = cmsg.0.as_mut_ptr().cast();
                msg.msg_controllen = libc::CMSG_SPACE(mem::size_of::<u16>() as u32) as _;
                let c = libc::CMSG_FIRSTHDR(&msg);
                (*c).cmsg_level = libc::SOL_UDP;
                (*c).cmsg_type = libc::UDP_SEGMENT;
                (*c).cmsg_len = libc::CMSG_LEN(mem::size_of::<u16>() as u32) as _;
                ptr::write_unaligned(libc::CMSG_DATA(c).cast::<u16>(), segment);
            }
        }
        // SAFETY: every pointer in `msg` refers to a live local buffer.
        let n = unsafe { libc::sendmsg(fd, &msg, 0) };
        if n < 0 { Err(io::Error::last_os_error()) } else { Ok(n as usize) }
    }

    pub(super) enum GsoSend {
        Sent,
        /// `from` = offset of the first unsent byte: always a segment boundary.
        Stopped { from: usize, error: io::Error },
    }

    /// Send a coalesced buffer in as few GSO sends as D34 allows, stopping at
    /// the first error. `send_one(chunk, seg)` performs one send (seg 0 = plain).
    pub(super) fn send_chunks(
        buf: &[u8],
        segment: usize,
        mut send_one: impl FnMut(&[u8], u16) -> io::Result<usize>,
    ) -> GsoSend {
        let Ok(seg16) = u16::try_from(segment) else {
            return GsoSend::Stopped { from: 0, error: io::Error::from_raw_os_error(libc::EINVAL) };
        };
        let step = chunk_bytes(segment);
        let mut off = 0;
        while off < buf.len() {
            let end = (off + step).min(buf.len());
            let chunk = &buf[off..end];
            let seg = if chunk.len() > segment { seg16 } else { 0 };
            if let Err(error) = send_one(chunk, seg) {
                return GsoSend::Stopped { from: off, error };
            }
            off = end;
        }
        GsoSend::Sent
    }
}
```

Note su A:
- `msg_iovlen = 1` e i cast `as _` coprono le differenze di tipo glibc/musl (`size_t` contro `c_int`/`socklen_t`).
- Non aggiungere flag a `recvmsg`/`sendmsg`: i socket sono già non bloccanti.
- `segments(0, 0) == 1`: un datagramma vuoto conta 1.

#### Scheletro B — wiring nel motore (normativo)

```rust
// ---- tipi della fase 2, aggiunte cfg ----
struct Gates {
    send_full: LogGate, no_target: LogGate, open_failed: LogGate, recv_failed: LogGate,
    #[cfg(target_os = "linux")] gso_fallback: LogGate,
    #[cfg(target_os = "linux")] gro_trunc: LogGate,
}

struct SessionShared {
    /* campi della fase 2 invariati */
    /// UDP_GRO is set on `upstream`: every receive must parse cmsgs (D32).
    #[cfg(target_os = "linux")] gro: bool,
    /// Shared with the listener: replies may still use GSO (D35).
    #[cfg(target_os = "linux")] listener_gso: Arc<AtomicBool>,
}

struct SessionEntry {
    /* campi della fase 2 invariati */
    /// Client -> target sends may still use GSO on this session (D35).
    #[cfg(target_os = "linux")] gso_ok: bool,
}

/// A reply the listener could not take yet. `segment > 0` = coalesced (D31).
struct Pending { data: Vec<u8>, segment: usize }
enum SessionDrain { Idle, Full, Blocked(Pending), Dead }   // era Blocked(Vec<u8>)

#[cfg(target_os = "linux")]
struct ListenerOffload {
    /// UDP_GRO is set on the listener socket. Only ever goes true (D32a).
    gro_set: bool,
    /// The knob state last applied (new sessions get UDP_GRO only while true).
    enabled: bool,
    /// Receives parse GRO cmsgs. Sticky (D32).
    gro_aware: bool,
    /// The kernel refused UDP_GRO (D37).
    unsupported: bool,
    /// Replies may use GSO; cleared for good on the first rejection.
    gso_ok: Arc<AtomicBool>,
}
// Engine: nuovo campo `#[cfg(target_os = "linux")] off: ListenerOffload`, inizializzato in
// run_udp_forwarder con { gro_set: false, enabled: false, gro_aware: false, unsupported: false,
// gso_ok: Arc::new(AtomicBool::new(true)) }.
// run_udp_forwarder: `let mut buf = vec![0_u8; LISTENER_BUF_BYTES];` (era UDP_BUF_BYTES).

// ---- Engine::run ----
// prima del loop:            #[cfg(target_os = "linux")] self.apply_gro_knob();
// ramo sweep:                _ = sweep.tick() => { self.sweep(); #[cfg(target_os = "linux")] self.apply_gro_knob(); }
// ramo readable, Ok(()):
//     #[cfg(target_os = "linux")]
//     let drained = if self.off.gro_aware { self.drain_listener_gro(buf) } else { self.drain_listener(buf) };
//     #[cfg(not(target_os = "linux"))]
//     let drained = self.drain_listener(buf);
//     match drained { /* invariato */ }

#[cfg(target_os = "linux")]
impl Engine {
    // rev 3 (D32a): UDP_GRO is never cleared from a socket. `enabled` is the
    // knob state last applied; `gro_set` is the socket state and only goes true.
    fn apply_gro_knob(&mut self) {
        let want = self.knobs.udp_gro_gso() && !self.off.unsupported;
        if want == self.off.enabled { return; }
        if !want {
            self.off.enabled = false;
            info!(forwarder = %self.label, "UDP GRO/GSO disabled on the listener for new sessions (UDP_GRO stays set on the listener socket until restart)");
            return;
        }
        if !self.off.gro_set {
            self.off.gro_aware = true; // before the sockopt: never read a burst with a plain recv
            match offload::set_gro(self.socket.as_raw_fd(), true) {
                Ok(()) => self.off.gro_set = true,
                Err(e) if e.raw_os_error() == Some(libc::ENOPROTOOPT) => return self.mark_gro_unsupported(),
                Err(e) => {
                    // latch: one warn, no retry every sweep
                    self.off.unsupported = true;
                    warn!(forwarder = %self.label, error = %e, "Setting UDP_GRO on the listener failed; udp_gro_gso has no effect on this forwarder until restart");
                    return;
                }
            }
        }
        self.off.enabled = true;
        info!(forwarder = %self.label, "UDP GRO/GSO enabled on the listener");
    }

    fn mark_gro_unsupported(&mut self) {
        if !self.off.unsupported {
            self.off.unsupported = true;
            warn!(forwarder = %self.label, "UDP GRO is not supported by this kernel (Linux 5.0+ required); udp_gro_gso has no effect");
        }
    }

    /// `drain_listener` with GRO-aware receives (D32).
    fn drain_listener_gro(&mut self, buf: &mut [u8]) -> ListenerDrain {
        let now_ms = self.stats.now_ms();
        let fd = self.socket.as_raw_fd();
        let mut batch = Batch::default();
        let mut outcome = ListenerDrain::Full;
        for _ in 0..LISTENER_BATCH {
            match self.socket.try_io(Interest::READABLE, || offload::recv(fd, buf, true)) {
                Ok(r) => {
                    let Some(client) = r.from else { continue };
                    if r.truncated {
                        batch.dropped += offload::segments(r.len, r.segment);
                        if let Some(s) = self.gates.gro_trunc.check(DROP_LOG_SPACING) {
                            warn!(forwarder = %self.label, client = %client, suppressed = s,
                                "UDP GRO burst larger than the receive buffer; datagrams dropped");
                        }
                        continue;
                    }
                    self.on_client_buffer(client, &buf[..r.len], r.segment, now_ms, &mut batch);
                }
                Err(e) => match classify_udp_err(&e) { /* identico a drain_listener */ },
            }
        }
        /* flush identico a drain_listener */
        outcome
    }

    /// `on_client_datagram` for a buffer that may be a GRO burst.
    fn on_client_buffer(&mut self, client: SocketAddr, data: &[u8], segment: usize, now_ms: u64, batch: &mut Batch) {
        if !offload::is_coalesced(data.len(), segment) {
            return self.on_client_datagram(client, data, now_ms, batch);
        }
        if let Some(entry) = self.sessions.get_mut(&client) {
            if !entry.shared.dead.load(Ordering::Relaxed) {
                forward_upstream_coalesced(entry, data, segment, now_ms, batch, &self.stats, &mut self.gates, &self.label);
                return;
            }
        }
        if let Some(dead) = self.sessions.remove(&client) {
            close_entry(&self.label, &dead, "upstream error");
        }
        let n = offload::segments(data.len(), segment);
        if let Some(entry) = self.open_session(client, now_ms, batch, n) {
            let entry = self.sessions.entry(client).or_insert(entry);
            forward_upstream_coalesced(entry, data, segment, now_ms, batch, &self.stats, &mut self.gates, &self.label);
        }
    }
}

// ---- open_session (fase 2) ----
// firma: fn open_session(&mut self, client, now_ms, batch, datagrams: u64) -> Option<SessionEntry>
// ogni `batch.dropped += 1` → `batch.dropped += datagrams`; `inc_rejected()` → `add_rejected(datagrams)`;
// la chiamata in on_client_datagram passa 1.
// Dopo connect_upstream riuscita, prima di costruire SessionShared:
//     #[cfg(target_os = "linux")]
//     let gro = self.off.enabled && match offload::set_gro(upstream.as_raw_fd(), true) {
//         Ok(()) => true,
//         Err(e) => {
//             if e.raw_os_error() == Some(libc::ENOPROTOOPT) { self.mark_gro_unsupported(); }
//             false
//         }
//     };
// SessionShared { .., #[cfg(target_os = "linux")] gro, #[cfg(target_os = "linux")] listener_gso: self.off.gso_ok.clone() }
// SessionEntry  { .., #[cfg(target_os = "linux")] gso_ok: true }

// ---- forward_upstream (fase 2): estrazione a comportamento identico ----
// il ramo Err(e) diventa: `batch.dropped += 1; note_upstream_send_error(&e, &entry.shared, now_ms, stats, gate, label);`
fn note_upstream_send_error(e: &io::Error, shared: &SessionShared, now_ms: u64, stats: &ForwarderStats, gate: &mut LogGate, label: &str) {
    /* il match classify_udp_err della fase 2, testo dei log invariato */
}

/// Client -> target for a GRO burst. Synchronous (I-4); raw sends (I-14).
#[cfg(target_os = "linux")]
#[allow(clippy::too_many_arguments)]
fn forward_upstream_coalesced(
    entry: &mut SessionEntry, data: &[u8], segment: usize, now_ms: u64, batch: &mut Batch,
    stats: &ForwarderStats, gates: &mut Gates, label: &str,
) {
    let total = offload::segments(data.len(), segment);
    let mut from = 0;
    if entry.gso_ok {
        let fd = entry.shared.upstream.as_raw_fd();
        match offload::send_chunks(data, segment, |chunk, seg| offload::send(fd, chunk, seg, None)) {
            offload::GsoSend::Sent => {
                entry.tx_datagrams += total;
                batch.datagrams += total;
                batch.bytes += data.len() as u64;
                entry.shared.touch(now_ms);
                return;
            }
            offload::GsoSend::Stopped { from: f, error } => {
                let sent = (f / segment) as u64;
                entry.tx_datagrams += sent;
                batch.datagrams += sent;
                batch.bytes += f as u64;
                if sent > 0 { entry.shared.touch(now_ms); }
                if !offload::is_fallback_error(&error) {
                    batch.dropped += total - sent;
                    note_upstream_send_error(&error, &entry.shared, now_ms, stats, &mut gates.send_full, label);
                    return;
                }
                entry.gso_ok = false;
                if let Some(s) = gates.gso_fallback.check(DROP_LOG_SPACING) {
                    warn!(forwarder = %label, client = %entry.shared.client, error = %error, suppressed = s,
                        "UDP GSO rejected by the kernel for a session; it now sends one datagram at a time");
                }
                from = f;
            }
        }
    }
    for piece in data[from..].chunks(segment) {
        forward_upstream(entry, piece, now_ms, batch, stats, &mut gates.send_full, label);
    }
}

// ---- run_session: dispatch ----
//     #[cfg(target_os = "linux")]
//     let drained = if shared.gro { drain_upstream_gro(&shared, &listener, &stats, &label) } else { drain_upstream(&shared, &listener, &stats, &label) };
//     #[cfg(not(target_os = "linux"))]
//     let drained = drain_upstream(&shared, &listener, &stats, &label);
// drain_upstream (fase 2): WouldBlock → `SessionDrain::Blocked(Pending { data: buf[..n].to_vec(), segment: 0 })`.
// Ramo Blocked(p) del run_session (slow path, piattaforma neutra):
//     let pieces: Vec<&[u8]> = if p.segment > 0 && p.segment < p.data.len() { p.data.chunks(p.segment).collect() } else { vec![&p.data[..]] };
//     for piece in pieces {
//         let sent = tokio::select! { biased; _ = cancel.cancelled() => return, r = listener.send_to(piece, shared.client) => r };
//         match sent { Ok(_) => { rx_datagrams += 1; add_received(piece.len()); add_datagrams_received(1) }
//                      Err(e) => { add_datagrams_dropped(1); debug!(.., "UDP reply send failed; datagram dropped") } }
//     }

/// `drain_upstream` for a session whose upstream socket has UDP_GRO (D32).
#[cfg(target_os = "linux")]
fn drain_upstream_gro(shared: &SessionShared, listener: &UdpSocket, stats: &ForwarderStats, label: &str) -> SessionDrain {
    GRO_REPLY_BUF.with(|cell| {
        let mut buf = cell.borrow_mut();
        let now_ms = stats.now_ms();
        let fd = shared.upstream.as_raw_fd();
        let to = socket2::SockAddr::from(shared.client);
        let mut batch = Batch::default();
        let mut replied = false;
        let mut outcome = SessionDrain::Full;
        for _ in 0..SESSION_BATCH {
            // D7a (rev 2): single-flag interests only; READABLE first, ERROR on WouldBlock.
            let res = match shared.upstream.try_io(Interest::READABLE, || offload::recv(fd, &mut buf[..], false)) {
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    shared.upstream.try_io(Interest::ERROR, || offload::recv(fd, &mut buf[..], false))
                }
                res => res,
            };
            match res {
                Ok(r) => {
                    replied = true;
                    if r.truncated {
                        batch.dropped += offload::segments(r.len, r.segment);
                        debug!(forwarder = %label, client = %shared.client, "UDP GRO reply larger than the buffer; datagrams dropped");
                        continue;
                    }
                    let data = &buf[..r.len];
                    if !offload::is_coalesced(r.len, r.segment) {
                        /* identico al ramo Ok(n) di drain_upstream, con Pending { segment: 0 } */
                        continue;
                    }
                    if let Some(p) = reply_coalesced(listener, &to, shared.client, data, r.segment, &shared.listener_gso, &mut batch, label) {
                        outcome = SessionDrain::Blocked(p);
                        break;
                    }
                }
                Err(e) => match classify_udp_err(&e) { /* identico a drain_upstream */ },
            }
        }
        /* flush identico a drain_upstream (replied → touch + mark_connect_ok_at; contatori) */
        outcome
    })
}

/// Target -> client for a GRO burst: GSO through the listener, or one
/// datagram at a time once the listener's GSO was rejected. Returns the
/// unsent tail when the listener's send buffer is full.
#[cfg(target_os = "linux")]
#[allow(clippy::too_many_arguments)]
fn reply_coalesced(
    listener: &UdpSocket, to: &socket2::SockAddr, client: SocketAddr, data: &[u8], segment: usize,
    gso_ok: &AtomicBool, batch: &mut Batch, label: &str,
) -> Option<Pending> {
    let mut from = 0;
    if gso_ok.load(Ordering::Relaxed) {
        let fd = listener.as_raw_fd();
        let r = offload::send_chunks(data, segment, |chunk, seg| {
            listener.try_io(Interest::WRITABLE, || offload::send(fd, chunk, seg, Some(to)))
        });
        match r {
            offload::GsoSend::Sent => {
                batch.datagrams += offload::segments(data.len(), segment);
                batch.bytes += data.len() as u64;
                return None;
            }
            offload::GsoSend::Stopped { from: f, error } => {
                batch.datagrams += (f / segment) as u64;
                batch.bytes += f as u64;
                if error.kind() == io::ErrorKind::WouldBlock {
                    return Some(Pending { data: data[f..].to_vec(), segment });
                }
                if !offload::is_fallback_error(&error) {
                    batch.dropped += offload::segments(data.len() - f, segment);
                    debug!(forwarder = %label, client = %client, error = %error, "UDP reply send failed; datagrams dropped");
                    return None;
                }
                if gso_ok.swap(false, Ordering::Relaxed) {
                    warn!(forwarder = %label, error = %error,
                        "UDP GSO rejected by the kernel on the listener; replies now go one datagram at a time");
                }
                from = f;
            }
        }
    }
    let rest = &data[from..];
    let mut off = 0;
    for piece in rest.chunks(segment) {
        match listener.try_send_to(piece, client) {
            Ok(_) => { batch.datagrams += 1; batch.bytes += piece.len() as u64; }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                return Some(Pending { data: rest[off..].to_vec(), segment });
            }
            Err(e) => {
                batch.dropped += 1;
                debug!(forwarder = %label, client = %client, error = %e, "UDP reply send failed; datagram dropped");
            }
        }
        off += piece.len();
    }
    None
}
```

Note su B:
- `use std::os::fd::AsRawFd;` va sotto `#[cfg(target_os = "linux")]` (o `#[cfg(unix)]`) negli import di `udp.rs`.
- `segments(data.len() - f, segment)` è corretto perché `f` è un multiplo di `segment` (D34).
- Un `Pending` con `segment > 0` e `data.len() <= segment` (resta un solo pezzo) va allo slow path come singolo.
  La condizione `p.segment < p.data.len()` lo copre.

- **Unit tests:** in `src/udp.rs` `mod tests`, tutti `#[cfg(target_os = "linux")]`, salvo l'ultimo.

  Fixture:
  - `gro_socket()`: `std::net::UdpSocket` su `127.0.0.1:0`, non bloccante, con `offload::set_gro(fd, true)`. Se
    l'errore è `ENOPROTOOPT`, il test stampa `skip: no UDP_GRO` e ritorna; non succede sul kernel di sviluppo.
  - `gso_sender()`: `std::net::UdpSocket` su `127.0.0.1:0`.
  - Le letture usano `poll_recv(fd, within)`: un loop di `offload::recv` che su `WouldBlock` dorme 5 ms, fino a
    `within`.

  | Test | Fixture / azione | Asserzioni esatte |
  |------|------------------|-------------------|
  | `offload_gso_send_arrives_coalesced_at_a_gro_socket` | `offload::send(sender, 10×1000 B, 1000, Some(gro_addr))` | ritorno 10000; `recv` del ricevente → `len == 10000`, `segment == 1000`, `from == sender addr`, `!truncated` |
  | `offload_gso_send_is_split_for_a_socket_without_gro` | ricevente senza GRO; payload `i` ripetuto per `i in 0..10`, segmento 1000 | 10 `recv_from` normali, ciascuno da 1000 B con il contenuto `i` in ordine |
  | `offload_tail_segment_is_kept` | GSO 3×1000 + 200 verso un GRO socket | `len == 3200`, `segment == 1000`; `chunks(1000)` → lunghezze `[1000, 1000, 1000, 200]` |
  | `offload_zero_length_datagram_round_trips` | `send(fd, &[], 0, Some(addr))` verso un GRO socket | `len == 0`, `segment == 0`, `segments(0, 0) == 1` |
  | `offload_segments_and_chunk_bytes_math` | — | `segments(0,0)=1`, `segments(5,0)=1`, `segments(10,10)=1`, `segments(11,10)=2`, `segments(6400,100)=64`; `chunk_bytes(1000)=64000`, `chunk_bytes(1472)=64768`, `chunk_bytes(100)=6400`, `chunk_bytes(30000)=60000`, `chunk_bytes(65507)=65507` |
  | `offload_send_chunks_stops_at_the_first_error_with_its_offset` | closure fittizia che registra `(len, seg)`; buffer 6450 B, segmento 100; al 2° invio restituisce `Err(EINVAL)` | chiamate registrate `[(6400, 100), (50, 0)]`; esito `Stopped { from: 6400 }` con `is_fallback_error`; con la closure sempre Ok l'esito è `Sent`; con `WouldBlock` al 1° invio l'esito è `Stopped { from: 0 }` e kind `WouldBlock` |
  | `offload_gso_rejection_is_a_fallback_error` | sender con `setsockopt(SOL_SOCKET, SO_NO_CHECK, 1)` | `send(.., 100, Some(addr))` di 1000 B → `Err` con `is_fallback_error` true; 10 send senza segmento degli stessi pezzi → 10 datagrammi ricevuti |
  | `disabling_udp_gro_leaves_queued_bursts_merged` (rev 3, sostituisce `gro_aware_receive_is_exact_after_gro_is_disabled`) | GSO 10×100 verso un GRO socket, **poi** `set_gro(fd, false)`, poi una lettura `offload::recv` | pinna la premessa di D32a: `len == 1000`, `segment == 0` (burst fuso senza cmsg). Se un kernel futuro cambia comportamento il test fallisce e D32a va rivista; non adattare l'asserzione |
  | `reply_coalesced_falls_back_once_and_delivers_everything` | "listener" tokio `UdpSocket` con `SO_NO_CHECK`; client ricevente normale; `gso_ok = AtomicBool::new(true)`; buffer 8×500 distinti, segmento 500 | 1ª chiamata → `None`, `gso_ok == false`, `batch.datagrams == 8`, `batch.dropped == 0`; il client riceve gli 8 pezzi in ordine; 2ª chiamata → stesso esito senza tentare GSO |
  | `forward_upstream_coalesced_falls_back_and_counts` | `SessionEntry` costruita a mano: upstream connesso a un ricevente normale e con `SO_NO_CHECK`; `ConnGuard::new(stats)`; permit da `Semaphore::new(1)` | dopo una chiamata con 8×500: `entry.gso_ok == false`, `entry.tx_datagrams == 8`, `batch.datagrams == 8`, `batch.bytes == 4000`, `batch.dropped == 0`; il ricevente ha 8 datagrammi in ordine |
  | `config::tests::udp_gro_gso_setting_cli_and_knob` (tutte le piattaforme) | YAML `settings: { udp_gro_gso: true }`; default; `CliArgs::parse_from(["rfw", "--udp-gro-gso"])` + `apply_cli_overrides` su default | il YAML dà `true`; il default è `false`; la CLI dà `true`; `RuntimeKnobs::from_settings` + `update` riflettono il valore (true → false) |

  Gate: G-U3.1. Discovery: 10 nomi `udp::tests::` su Linux, più il test `config`.
- **e2e tests:** N/A in 3.1. Il comportamento osservabile end-to-end è coperto in 3.2, che possiede gli helper
  GSO di `tests/common`.
- **Done:**
  - scheletri A/B implementati;
  - test della fase 2 invariati e verdi;
  - G-HYG e G-U3.1 pass con discovery;
  - review di agent-1:opus (unsafe, D32, D34, I-4);
  - unità chiusa, commit `sub-phase(3.1)`.

### 3.2 Test end-to-end GRO/GSO, benchmark, documentazione
- **Model:** agent-2:sonnet
- **Assignment:** test di integrazione (unit a livello forwarder ed e2e), benchmark e README. agent-1:opus fa la
  review dopo il diff (oracoli dei test, numeri del bench, accuratezza del README).
- **Files:**
  - READ: `src/udp.rs` (`offload`, test helper della fase 2 `spawn_fwd`/`start_fwd`/`roundtrip`); `tests/common/mod.rs`;
    `tests/e2e_udp.rs` (`reference_scenario_tcp_and_udp_side_by_side`, `udp_bench`); `README.md` (sezioni UDP
    della 2.3); `forwarders.yml`.
  - WRITE: `src/udp.rs` (solo `mod tests`), `tests/common/mod.rs`, `tests/e2e_udp.rs`, `README.md`, `forwarders.yml`.
  - NEW: nessuno.
- **Change:**
  Preconditions: 3.1 DONE.
  Contract: D18, D31–D36; test e bench come sotto.
  Scope: nessun cambio a `src/` fuori dai test. Se un test rivela un difetto in 3.1, si apre una correzione su 3.1
  (nuovo attempt) e la si escala ad agent-1:opus. Non si corregge il codice di produzione dentro la 3.2.
  Steps:
  1. S1 — `tests/common/mod.rs`, helper `#[cfg(target_os = "linux")]` (gli e2e non vedono i moduli del binario,
     quindi una piccola copia delle primitive è voluta):
     ```rust
     /// Enable UDP_GRO on a socket (tests only).
     pub fn set_udp_gro(sock: &impl std::os::fd::AsRawFd);
     /// One sendmsg with a UDP_SEGMENT cmsg: `payload` is cut every `segment` bytes by the kernel.
     pub fn gso_send(sock: &std::net::UdpSocket, payload: &[u8], segment: u16, to: std::net::SocketAddr) -> std::io::Result<usize>;
     /// GRO-aware recvmsg: (len, segment, from); segment 0 = single datagram.
     pub fn gro_recv(sock: &std::net::UdpSocket, buf: &mut [u8]) -> std::io::Result<(usize, usize, std::net::SocketAddr)>;
     /// Echo server with UDP_GRO: echoes every receive with the same segmentation (GSO when coalesced).
     pub fn spawn_udp_gso_echo() -> u16;    // thread std, socket bloccante con read timeout 100 ms, termina quando il processo esce
     ```
     L'implementazione ricalca `offload::send`/`offload::recv` (stessi cmsg; `libc` e `socket2` sono già
     dev-dependency).
  2. S2 — unit a livello forwarder in `src/udp.rs` `mod tests` (tabella).
  3. S3 — e2e in `tests/e2e_udp.rs`:
     - estrarre il corpo di `reference_scenario_tcp_and_udp_side_by_side` in
       `async fn reference_scenario(extra_args: &[&str])`;
     - il test esistente chiama `reference_scenario(&[])`, con le stesse asserzioni.
  4. S4 — bench `udp_gso_bench`.
  5. S5 — README e `forwarders.yml`.
  6. S6 — G-HYG, G-U3.2, G-GSO-BENCH.

  Recovery boundary: dopo S3 (test verdi), checkpoint §6.
  Failure handling:
  - flakiness da timing → escalation con log (`spawn_rfw_logged`, `RUST_LOG=rfw=debug`), senza allentare le asserzioni;
  - speedup del bench < 1,0 → registrare in §7 ed escalare ad agent-1:opus. Non è un fallimento da "sistemare"
    nei test.
- **Unit tests** (forwarder intero, `#[cfg(target_os = "linux")]`, in `udp::tests`). Nei test con `udp_gro_gso: true`
  nei `Settings`, il client è un `std::net::UdpSocket` normale (senza GRO) che invia con `offload::send`.

  | Test | Fixture / azione | Asserzioni esatte |
  |------|------------------|-------------------|
  | `gro_gso_path_preserves_datagram_boundaries` | backend `spawn_udp_gso_echo` locale al modulo (stessa logica dell'helper e2e, scritta con `offload`); forwarder con `udp_gro_gso: true`; tre burst inviati uno alla volta: (a) 64×1000 B, payload `[b, i]` ripetuto; (b) 10×1000 + 300 B; (c) un singolo 1000 B | per ogni burst il client riceve esattamente i datagrammi inviati, **nello stesso ordine e con lo stesso contenuto** (entro 3 s per burst); alla fine `datagrams_sent == 76`, `datagrams_received == 76`, `datagrams_dropped == 0`, `bytes_sent == bytes_received == 75300` |
  | `gro_toggle_never_merges_datagrams` | backend echo **normale** che registra ogni datagramma ricevuto; forwarder con `udp_gro_gso: false`; 6 round: `knobs.update(&Settings { udp_gro_gso: round % 2 == 0, ..Settings::default() })`, attesa 1,3 s, burst GSO 32×500 distinti | in ogni round il backend registra esattamente i 32 datagrammi da 500 B nell'ordine inviato e il client riceve 32 echi esatti |
  | `coalesced_burst_from_a_refused_client_counts_every_datagram` | `udp_gro_gso: true`, `udp_max_sessions: 1`; client A occupa la sessione; client B invia un burst GSO 10×100 | entro 2 s `rejected_connections == 10` e `datagrams_dropped >= 10`; B non riceve nulla in 500 ms |
  | `first_burst_of_a_new_session_is_never_dropped` | `udp_gro_gso: true`; 20 client **nuovi**, ognuno un solo burst GSO 16×200, nessun retry | ognuno riceve i 16 echi entro 2 s; `datagrams_dropped == 0` |

  Gate: G-U3.2 (unit). Discovery: i 4 nomi.
- **e2e tests:** `tests/e2e_udp.rs`, tutti `#[cfg(target_os = "linux")]`.

  | Test | Input | Osservazioni attese |
  |------|-------|--------------------|
  | `udp_gro_gso_flag_preserves_datagrams_byte_exact` | CLI `127.0.0.1:P:127.0.0.1:E/udp --udp-gro-gso` + metrics; E = `spawn_udp_gso_echo()`; client normale; 5 burst sequenziali `gso_send` 64×1000 distinti | ogni burst torna con 64 echi identici e in ordine (entro 3 s); poi entro 3 s, sui delta, `datagrams_sent` +320, `datagrams_received` +320, `datagrams_dropped` +0 |
  | `udp_gro_gso_flag_hot_reload_toggles` | YAML con un forwarder UDP verso `spawn_udp_gso_echo` e `settings: { udp_gro_gso: false }`; `spawn_rfw_logged` | burst 16×500 esatto; riscrittura con `true` → entro 5 s il log contiene `UDP GRO/GSO enabled on the listener`; burst esatto; riscrittura con `false` → entro 5 s il log contiene `UDP GRO/GSO disabled on the listener`; burst esatto |
  | `reference_scenario_with_udp_gro_gso` | `reference_scenario(&["--udp-gro-gso"])` | tutte le asserzioni dello scenario di riferimento |
  | `udp_gso_bench` (`#[ignore]`) | vedi sotto | stampa `UDP_GSO_BENCH …`; asserisce `loss_plain == 0`, `loss_gso == 0` e `speedup >= 1.0` |

  `udp_gso_bench`:
  - backend `spawn_udp_gso_echo`;
  - 4 client normali, ognuno con un solo burst GSO 16×1200 in volo (64 datagrammi in volo in totale, come
    `udp_bench`); il burst successivo parte quando tornano tutti i 16 echi; 5 s per modalità;
  - due run separati di rfw sulla stessa macchina: senza flag (`plain`) e con `--udp-gro-gso` (`gso`);
  - stampa `UDP_GSO_BENCH plain_mbit_s=… gso_mbit_s=… speedup=… loss_plain=… loss_gso=…`;
  - perdita = datagrammi inviati − echi ricevuti, dopo 1 s di drain.

  Gate: G-U3.2 (e2e). Discovery: `e2e_udp` = 15 test eseguiti (+2 ignored) su Linux.
- **README** (inglese, stile esistente):
  - **Performance & tuning**, sottosezione "UDP GRO/GSO (Linux, opt-in)":
    - cosa fa (D31, D33);
    - quando conviene: flussi ad alto pps con peer o NIC che usano GSO/GRO, cioè trasferimenti bulk QUIC-like;
      con traffico a datagrammi singoli e radi non porta vantaggi;
    - requisiti: kernel 5.0+, altrimenti un warn e nessun effetto;
    - hot reload: il listener al tick successivo (≤ 1 s); le sessioni esistenti tengono la loro modalità fino alla
      chiusura; le ricezioni restano GRO-aware fino al riavvio;
    - fallback automatico con i due warn testuali;
    - ignorato fuori da Linux;
    - comando del bench `cargo test --release --test e2e_udp udp_gso_bench -- --ignored --exact --nocapture`.

    Nessun numero di prestazione nel README: i numeri vanno in STATE.md §7.
  - **Full CLI reference**: `--udp-gro-gso`.
  - **YAML settings**: `udp_gro_gso`.
  - **Hot-reload**: `udp_gro_gso` nella lista live, con la nota sulle sessioni esistenti e (rev 3, D32a) la nota:
    "turning it off stops GRO on new sessions; a listener that already has UDP_GRO keeps it until restart".
  - **Development → Test**: menzione del bench GSO.
  - `forwarders.yml`: `# udp_gro_gso: false   # Linux only, hot-reloadable` nel blocco settings, più la flag
    nell'elenco CLI del commento.
- **Done:**
  - test e bench presenti e verdi;
  - G-HYG, G-U3.2 e G-GSO-BENCH pass, con numeri in §7;
  - README e `forwarders.yml` aggiornati;
  - review di agent-1:opus;
  - unità chiusa, commit `sub-phase(3.2)`.

## Gate di fase e chiusura (finale)
- Gate richiesti: G-PHASE, G-XPLAT, G-PERF-TCP, G-UDP-BENCH (rieseguito: il percorso di default non deve
  regredire), G-GSO-BENCH, G-FINAL (STATE.md §3).
- Asserzioni:
  - tutti i test verdi in debug e release, con discovery;
  - G-XPLAT exit 0: verifica la compilazione dei `cfg(not(target_os = "linux"))` e degli attributi cfg sui campi;
  - G-PERF-TCP: mediana ≤ 1,15 × G-PERF-TCP-BASE;
  - G-UDP-BENCH: stesse soglie di P2, e `p50_overhead_us ≤ max(1,2 × valore P2, valore P2 + 10)` rispetto al valore
    registrato in §7 (il percorso di default non regredisce; il margine assoluto assorbe il rumore a livello di µs);
  - G-GSO-BENCH: perdite 0 e speedup ≥ 1,0 (uno speedup < 1,3 richiede una nota di agent-1:opus in §7);
  - G-FINAL: `reference_scenario_tcp_and_udp_side_by_side` e `reference_scenario_with_udp_gro_gso` verdi in release.
- Obbligo README: unità 3.2. P3 rilegge le sezioni UDP (2.3 + 3.2) rispetto ai messaggi di log, alle flag e alle
  metriche a HEAD.
- agent-1:opus fa la review di:
  - I-1…I-15;
  - l'invarianza del percorso dati TCP. Nel diff di `src/forwarder.rs` dalla baseline c50db1f sono ammessi solo:
    - `protocol: Protocol::Tcp` nei literal dei test (1.1);
    - `traffic_row` e la riga del report (1.2);
    - `pub(crate)` sui simboli elencati in 2.1.

    `run_forwarder`, gestione connessioni, copy/splice e keepalive devono restare byte-identici;
  - la sicurezza (nessun `{:?}` di `CliArgs`/`ReloadPolicy`, nessun segreto nei file) e del
  piano rispetto al risultato.
- Aprire P3, registrare review e risultati, segnare la fase e il piano DONE, commit `phase(3)`. Niente version
  bump, tag o push (D26).
