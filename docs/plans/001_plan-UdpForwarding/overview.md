# UDP forwarding — Plan overview

Redatto il 2026-10-18 da agent-1:opus (supervisore). Cartella: `docs/plans/001_plan-UdpForwarding/`.
L'esecuzione parte da [STATE.md](STATE.md): readiness e avanzamento live stanno solo lì.

## Obiettivo e scenario di riferimento

Aggiungere a rfw (oggi solo TCP, v1.0.6) i forwarder **UDP**, con la stessa configurazione (CLI posizionale,
`RFW_FORWARDER_N`, YAML con hot-reload) e le stesse proprietà operative del TCP. Le priorità sono:
banda e pps elevati, bassa latenza, stabilità sotto flood, hot reload, logging fatto bene e monitoraggio/alert.
Prima del lavoro UDP si corregge il bug TCP B1 (precedenza della configurazione al reload).

**Scenario di riferimento (oracolo G-FINAL, test `reference_scenario_tcp_and_udp_side_by_side`)**

1. Avvio: `rfw -f cfg.yml --metrics-addr 127.0.0.1:M`, dove `cfg.yml` contiene:
   - un forwarder TCP `127.0.0.1:P -> 127.0.0.1:E_tcp` (echo TCP);
   - un forwarder **UDP sulla stessa porta** `P`, `protocol: udp`, `127.0.0.1:P -> 127.0.0.1:E_udp` (echo UDP);
   - `settings.udp_session_timeout_secs: 30`.
2. Un datagramma UDP verso `P` torna identico. Un round-trip TCP verso `P` torna identico.
3. `/stats` contiene le label esatte:
   - `127.0.0.1:P->127.0.0.1:E_tcp`, con `"protocol":"tcp"`;
   - `127.0.0.1:P->127.0.0.1:E_udp/udp`, con `"protocol":"udp"` e `"datagrams_sent":1`.
4. `/metrics` contiene `rfw_datagrams_sent_total{label="127.0.0.1:P->127.0.0.1:E_udp/udp"} 1`.
   Le famiglie `rfw_datagrams_*` non compaiono per la label TCP.
5. Si riscrive il file con `udp_session_timeout_secs: 1`: la sessione UDP scade entro 5 s
   (`active_connections` della label UDP = 0) e il TCP continua a servire.
6. SIGTERM: il processo esce entro 3 s.

**Esclusioni**
- Nessun bump di versione, tag, push, release o deploy (D26).
- Nessun IP_PKTINFO né source-IP selection per bind wildcard multi-homed: è una limitazione documentata (D25).
- Nessuna rilevazione delle chiavi YAML sconosciute (rischio documentato).
- Nessun probe attivo verso target UDP (D14).

## Decisioni

| ID | Decisione e conseguenza | Autorità/fonte | Supersedes |
|----|--------------------------|----------------|------------|
| D1 | Sintassi CLI/env: suffisso opzionale `/udp` o `/tcp`, case-insensitive. Senza suffisso = tcp. Grammatica in D1a. | utente Q1, 2026-10-18 | — |
| D1a | Parsing: `s.rsplit_once('/')`. Nessun `/` → tcp. Suffisso `tcp`/`udp` (ignore-case) → protocollo, corpo = parte sinistra. Qualsiasi altro suffisso → `Err("Unknown protocol suffix '/{suffix}' in '{s}': expected /tcp or /udp")` (il caso `53/` dà suffisso vuoto: `'/'`). Il corpo deve avere 4 parti `:`, altrimenti `Err("Invalid forwarder format '{s}': expected local_host:local_port:remote_host:remote_port[/tcp|/udp]")`. | supervisore | — |
| D2 | YAML: campo `protocol: tcp\|udp` per forwarder, `#[serde(default)]` = tcp, alias `TCP`/`UDP`. Un valore sconosciuto è un errore di parse: all'avvio rfw esce, al reload si ha ConfigReloadFailed e resta la config precedente. | utente Q1 | — |
| D3 | Label TCP byte-identiche a v1.0.6 (`lh:lp->rh:rp`). Label UDP = `lh:lp->rh:rp/udp`. La label resta l'identità per la mappa del manager, lo stats registry, `sync` e le serie Prometheus. | utente Q1 | — |
| D4 | Il duplicato si valuta sulla coppia (protocol, local_addr): TCP+UDP sulla stessa porta è ammesso, UDP+UDP no. Il messaggio TCP resta invariato (`Duplicate local address '0.0.0.0:8080'. …`); quello UDP mostra `'0.0.0.0:53/udp'`. | supervisore | — |
| D5 | Modello sessione: un socket listener UDP per forwarder. Per ogni client `SocketAddr` si apre un socket upstream **connesso** (porta effimera), che dà filtraggio sorgente kernel e consegna errori ICMP. Il task listener possiede in esclusiva `HashMap<SocketAddr, SessionEntry>` (std SipHash, resistente a HashDoS con sorgenti spoofate), senza lock. | supervisore | — |
| D6 | Percorso client→upstream nel listener, a batch di 64 datagrammi per wakeup (`try_recv_from`). Si invia con **syscall raw** `SockRef::from(&upstream).send()`, senza usare la readiness di tokio: un socket appena registrato ha readiness vuota e `try_send` scarterebbe il primo datagramma. `WouldBlock` = drop contato, il listener non attende mai. | supervisore + advisor | — |
| D7 | ~~Superata da D7a (rev 2).~~ Percorso upstream→client nel task sessione: `upstream.ready(READABLE\|ERROR)`, poi `try_io(READABLE\|ERROR, read raw via SockRef)` con buffer thread-local da 64 KiB. Si invia con `listener.try_send_to`; se dà `WouldBlock` si copia in un `Vec` e si fa `send_to().await` in select con cancel. Il borrow thread-local non attraversa mai un `.await`. Con batch pieno si fa `yield_now()`. | supervisore (R2, R3) | — |
| D7a | Come D7, ma la lettura è `recv_upstream`: prima `try_io(Interest::READABLE, raw read)`, su `WouldBlock` `try_io(Interest::ERROR, raw read)`. `try_io` di tokio 1.53.1 onora solo interest a flag singolo (`Interest::mask()` mappa un interest combinato su `Ready::EMPTY` → closure mai eseguita → spin). Ogni `try_io` azzera la propria readiness su `WouldBlock`; l'ICMP solo-EPOLLERR viene consumato dal ramo ERROR. Vale anche per lo scheletro B di 3.1. | supervisore (R3 rev 2; evidenza 2.1) | D7 |
| D8 | Un unico classificatore `classify_udp_err(&io::Error) -> UdpErr {WouldBlock, Transient, Icmp(IcmpKind), Fatal}`: Interrupted→Transient; ConnectionRefused\|ConnectionReset→Icmp(PortUnreachable); HostUnreachable→Icmp(HostUnreachable); NetworkUnreachable→Icmp(NetworkUnreachable). Uso per percorso in D8a. | supervisore + advisor | — |
| D8a | Uso del classificatore per percorso:<br>• recv listener: Transient e Icmp = ignora (Windows WSAECONNRESET riguarda i client); Fatal = log rate-limited + backoff 50 ms.<br>• send upstream: ogni errore = drop contato; Icmp anche `mark_udp_icmp_error`; Fatal = debug, la sessione resta.<br>• recv upstream: Icmp = `mark_udp_icmp_error` e continua; Fatal = sessione `dead`.<br>• send verso client: WouldBlock = slow path; altro = drop contato. | supervisore | — |
| D9 | Eviction fatta solo dal listener, con uno sweep fisso ogni 1 s (`tokio::time::interval`). Il timeout si legge live dai knobs a ogni sweep. Si rimuove la sessione se idle ≥ timeout o se `dead`. | supervisore | — |
| D10 | Per i remote hostname, un refresher DNS ogni 1 s via `resolve_targets` (cache hickory, query reale solo a scadenza TTL) pubblica su `watch::Sender<Arc<[SocketAddr]>>`.<br>• Su errore: tiene l'ultimo set buono, warn rate-limited 30 s, `mark_connect_failed`.<br>• `observe_addrs` → alert DnsChanged.<br>• Quando il set cambia, il listener chiude le sessioni il cui remote non è più nel set; le nuove sessioni usano `first()`. Differenza voluta rispetto al pinning TCP. | supervisore | — |
| D11 | Contratto di stop:<br>1. cancel del token padre (le sessioni hanno token figli);<br>2. `close_all`;<br>3. `TaskTracker.wait()` limitato a 2 s;<br>4. drop dell'ultimo `Arc<UdpSocket>`, così la porta è libera al ritorno (rebind sicuro).<br>Niente SO_REUSEADDR sul listener UDP. Le sessioni UDP non stanno nel `conn_tracker` globale: SIGTERM non attende il drain TCP di 10 s e un datagramma in volo si perde. | supervisore | — |
| D12 | `udp_session_timeout_secs` = 60, hot-reloadable. `udp_max_sessions` = 16384, solo all'avvio, range 1..=1_048_576: semaforo globale tra tutti i forwarder UDP, separato da `max_connections`. Flag CLI `--udp-session-timeout`, `--udp-max-sessions`. | utente Q4 | — |
| D13 | Stats: si riusano `ForwarderStats` (bytes_sent/received, active_connections = sessioni attive, rejected_connections = sessioni rifiutate dal cap) e si aggiungono `datagrams_sent/received/dropped` e `protocol`.<br>• Prometheus: nuove famiglie `rfw_datagrams_{sent,received,dropped}_total` emesse solo se esiste almeno una label UDP e solo per le label UDP. Nessuna label `protocol` sulle serie esistenti, così l'identità delle serie non cambia.<br>• JSON `/stats`: campo additivo `"protocol"`, più i tre campi datagram solo per le voci UDP. | supervisore | — |
| D14 | Monitor UDP passivo: nessun pacchetto viene mai inviato al target.<br>• Probe = solo DNS.<br>• Verdetto = funzione pura `udp_verdict` (truth table in phase_03.md), su evidenze ICMP, reply e `datagrams_sent`. Il ripristino richiede evidenza positiva; senza traffico lo stato si congela (Hold, nessun alert).<br>• Lo startup report scrive "resolved to … (udp: passive check)".<br>• Le soglie Unreachable/Recovered sono le stesse del TCP. | utente Q3 + advisor | — |
| D15 | Logging:<br>• i percorsi per-datagramma non loggano mai sopra `debug`;<br>• apertura e chiusura sessione a `debug` (client, local upstream, remote, motivo, durata, contatori);<br>• drop, cap ed EMFILE: warn/error rate-limited 10 s con `suppressed`, più i contatori;<br>• a info: `Listening (udp) on …` e il riepilogo dei cambi DNS con il numero di sessioni chiuse;<br>• il traffic report ha righe UDP dedicate. | supervisore | — |
| D16 | Nuovo evento `Event::UdpSessionLimitReached {label, rejected_total}`:<br>• kind `udp_session_limit_reached`, severità Warning;<br>• throttle `limit:{label}` con THROTTLE_LIMIT;<br>• testo `*UDP session limit reached*`.<br>`Stopping` conta solo le connessioni TCP (le sole che drenano). | supervisore + advisor | — |
| D17 | Sicurezza dell'ordinamento con commit WIP/autonomi: dalla 1.1 alla 2.2 esclusa, `reject_unsupported_protocols` rifiuta ogni forwarder UDP dopo il load, da tutte le sorgenti (env, CLI, YAML) e al reload. Un UDP non viene mai avviato come TCP. Lo rimuove la 2.2 insieme al dispatch di monitor e probe. | supervisore + advisor | — |
| D18 | Fast path GRO/GSO opt-in, solo Linux:<br>• `udp_gro_gso: bool` = false (YAML), `--udp-gro-gso` (CLI), knob hot-reloadable;<br>• il listener applica il toggle al tick di sweep successivo, le nuove sessioni alla creazione;<br>• su piattaforme non Linux è ignorato con un warn all'avvio.<br>Le prestazioni prima e dopo vanno documentate con un benchmark. | utente Q2 | — |
| D19 | Modulo `offload` scritto a mano (libc `recvmsg`/`sendmsg` + cmsg), sullo schema del modulo `splice`. quinn-udp è scartato (R4). Cap GSO a 64 segmenti. Su EINVAL/EIO/EMSGSIZE della send GSO: split in userspace del buffer corrente e GSO disabilitato in modo sticky per quel socket, con un warn. | supervisore (R1, R4) | — |
| D20 | Fix B1: `ReloadPolicy {cli: CliArgs, source: ForwarderSource}`. Al reload: settings YAML + `apply_cli_overrides`, così le flag CLI vincono sempre. `sync` avviene solo se `source == File`; altrimenti va un info log e la lista del file viene ignorata. | supervisore (bug confermato) | — |
| D21 | Buffer di ricezione UDP fisso a 64 KiB: payload max 65507 su IPv4 e 65527 su IPv6; con un buffer più piccolo Linux tronca in silenzio. `buffer_bytes` non si applica a UDP. Nella fase 3 il buffer listener/sessione con GRO sale a 128 KiB, con controllo MSG_TRUNC. | supervisore | — |
| D22 | `socket_buffer_bytes` si applica al listener UDP al bind e ai nuovi socket upstream, best effort. Il listener rilegge SO_RCVBUF e, se effettivo < richiesto, emette un warn "raise net.core.rmem_max". | supervisore | — |
| D23 | Upstream socket: `socket2::Socket::new(Domain::for_address(remote), DGRAM, UDP)` → `set_nonblocking` → buffer → `connect` → `UdpSocket::from_std`. Nessun bind esplicito (auto-bind effimero). | supervisore | — |
| D24 | Budget fd (unix): all'avvio, se esiste almeno un UDP, servono `udp_max_sessions + 256` fd. Se il soft limit è insufficiente lo si alza fino a `min(hard, needed)`; se non basta ancora, warn. La matematica è una funzione pura testata. | supervisore + advisor | — |
| D25 | Bind wildcard multi-homed: la risposta può partire da un IP sorgente diverso da quello contattato. È una limitazione documentata (senza IP_PKTINFO). | supervisore | — |
| D26 | Nessun bump di versione: il test `e2e_webhook` verifica `rfw 1.0.6` nel footer. Release e tag restano decisioni dell'utente. | supervisore | — |
| D27 | Harness di test:<br>• `free_udp_port`, `free_dual_port` e i backend UDP condividono il claim set di `tests/common`;<br>• `spawn_rfw_logged` cattura stdout/stderr senza ANSI e forza `RUST_LOG=info`, per le asserzioni sui log. | supervisore | — |
| D28 | Contatori a batch: una `fetch_add` per batch e per contatore, saltata se vale zero.<br>• `touch` di last_activity solo se il ms è cambiato;<br>• `mark_connect_ok_at` throttled a 100 ms, ma scrive sempre se esiste un ICMP più recente dell'ultima reply registrata.<br>Il clock (`stats.now_ms()`) si legge una volta per batch. | supervisore | — |
| D29 | I datagrammi di lunghezza zero sono validi e vengono inoltrati (test dedicato). | supervisore | — |
| D30 | G-XPLAT: check cross-target in Docker (immagine `rfw-xcheck` da `Dockerfile.build`), con target dir in un volume Docker.<br>• `cargo check --all-targets` su Windows GNU;<br>• `cargo zigbuild --release` per i due Darwin.<br>Non si usa `build.sh`, che sovrascrive `dist/`. Gate attivo a P2 e P3. | supervisore + advisor | — |
| D31 | Un buffer ricevuto è "coalescato" (burst GRO) solo se `0 < segment < len`; lo split è `chunks(segment)`. Ogni altro buffer è un singolo datagramma e segue il percorso della fase 2. | supervisore (R1) | — |
| D32a | UDP_GRO non viene mai tolto da un socket (verificato in 3.1, kernel 7.0: dopo `set_gro(false)` uno skb accodato con GRO arriva fuso e senza cmsg). Knob off ⇒ solo le nuove sessioni restano senza GRO; il listener tiene UDP_GRO fino al restart. Errore non-ENOPROTOOPT su `set_gro` del listener → latch con un warn. | supervisore (evidenza 3.1) | D32 (spegnimento) |
| D32 | Ricezione GRO-aware sticky: un socket che ha avuto UDP_GRO attivo riceve sempre con `recvmsg` + parsing cmsg, perché gli skb già accodati restano coalescati anche dopo lo spegnimento.<br>• Listener: `gro_aware` diventa true prima del setsockopt e non torna false.<br>• Sessione: la modalità è fissata alla creazione. | supervisore (inferenza da R1) | — |
| D33 | GSO solo su ciò che GRO ha coalescato, con lo stesso `segment`. rfw non coalescia mai in userspace. | supervisore | — |
| D34 | Chunking della send GSO: al massimo `segment × clamp(65507 / segment, 1, 64)` byte per send, con chunk allineati al segmento; la coda ≤ segment va senza cmsg. | supervisore (R1) | — |
| D35 | Percorsi per direzione:<br>• client→target: raw `sendmsg` sull'upstream dal listener, con `gso_ok` per sessione;<br>• target→client: `try_io(WRITABLE)` sul listener, con `Arc<AtomicBool>` condiviso; su WouldBlock il residuo va in `Pending{data, segment}` e lo slow path lo divide senza GSO. | supervisore | — |
| D36 | Con GRO i contatori `datagrams_*` contano segmenti. Un burst di N segmenti da un client nuovo rifiutato conta N drop e N `rejected_connections` (`add_rejected(n)`). | supervisore | — |
| D37 | Kernel senza UDP_GRO (ENOPROTOOPT): un solo warn per forwarder, poi nessun nuovo tentativo; il flag non ha effetto. | supervisore | — |

## Domande aperte

Nessuna. Le quattro decisioni di prodotto (sintassi, fast path, monitor, default) sono state risposte dall'utente
il 2026-10-18. Tutto il resto è una scelta tecnica autorizzata del supervisore.

## Architettura

```
                ┌──────────────────── run_udp_forwarder (task, 1 per forwarder UDP) ───────────────────┐
client ──dgram──▶ listener UdpSocket (Arc) ── try_recv_from batch 64 ──▶ Engine.sessions: HashMap<client, SessionEntry>
                │        ▲                                                │ hit: SockRef::send(raw) ──▶ upstream (connected) ──▶ target
                │        │ try_send_to / send_to().await (slow path)      │ miss: open_session (sync): target=watch.first(),
                │        │                                                │       permit=udp_limit.try_acquire, socket2 connect,
                │   run_session task (1 per sessione, TaskTracker) ◀──────┘       spawn run_session, insert
                │     ready(READABLE|ERROR) → try_io(READABLE) poi try_io(ERROR) batch 64 ◀── reply dal target / ICMP (sk_err)
                │   sweep ogni 1s: idle ≥ knobs.udp_session_timeout() o dead → cancel + remove
                │   watch<Arc<[SocketAddr]>> ◀── run_dns_refresher (1s, solo hostname) → DnsChanged
                └─ stop: cancel → close_all → tracker.wait(≤2s) → drop listener → ListeningGuard off
manager.start_one: dispatch per cfg.protocol (Tcp → run_forwarder, Udp → run_udp_forwarder) + run_monitor (dispatch interno)
```

Ownership:
- `SessionShared` (in `Arc`) contiene upstream socket, client, remote, `last_activity_ms`, `dead`,
  `rx_datagrams`, `ConnGuard` (gauge attivi) e `OwnedSemaphorePermit`. Guard e permit si rilasciano quando
  cade l'ultimo Arc, cioè quando si chiude il socket.
- `SessionEntry` è posseduto dal listener: Arc, token figlio, `opened`, `tx_datagrams` (contatore plain).

## Interfacce e compatibilità

| Superficie | Nome/tipo esatto | Default | Errori/conflitti | Compatibilità |
|------------|------------------|---------|------------------|---------------|
| CLI/env forwarder | `lh:lp:rh:rp[/tcp\|/udp]` | tcp | suffisso sconosciuto → errore (D1a); env malformato → warn e skip (policy esistente) | stringhe senza suffisso invariate |
| YAML forwarder | `protocol: tcp\|udp` | tcp | valore sconosciuto → errore di parse | file esistenti invariati |
| Setting | `udp_session_timeout_secs: u64` | 60 | 0 → errore all'avvio, clamp a 1 al reload | additivo |
| Setting | `udp_max_sessions: usize` | 16384 | fuori da 1..=1_048_576 → errore all'avvio, clamp al reload; solo avvio | additivo |
| Setting | `udp_gro_gso: bool` (fase 3) | false | warn se non Linux | additivo |
| CLI | `--udp-session-timeout SECS`, `--udp-max-sessions N`, `--udp-gro-gso` | — | validazione come i settings | additivo |
| `/stats` JSON | `"protocol":"tcp"\|"udp"`; solo UDP: `"datagrams_sent"`, `"datagrams_received"`, `"datagrams_dropped"` | — | — | campi additivi |
| `/metrics` | `rfw_datagrams_sent_total`, `rfw_datagrams_received_total`, `rfw_datagrams_dropped_total` (label UDP) | — | — | serie esistenti invariate |
| Webhook | `UdpSessionLimitReached`; Started UDP "resolved to"; Stopping conta solo TCP | — | — | additivo |
| Reload (B1) | flag CLI riapplicate; forwarder env/CLI "pinned" | — | — | correzione rispetto al README ("CLI flags take precedence") |

## Phase map

| Fase logica | File | Sub-phase | Dipende da | Assegnazione |
|-------------|------|-----------|------------|--------------|
| 0 | [phase_01.md](phase_01.md) | 0.1 | nessuna | agent-2:sonnet |
| 1 | [phase_02.md](phase_02.md) | 1.1, 1.2 | P0 | 1.1 agent-2:sonnet; 1.2 agent-3:haiku |
| 2 | [phase_03.md](phase_03.md) | 2.1, 2.2, 2.3 | P1 | 2.1, 2.2 agent-2:sonnet; 2.3 agent-3:haiku |
| 3 | [phase_04.md](phase_04.md) | 3.1, 3.2 | P2 | agent-2:sonnet |

Questa tabella non riporta stato live. Le chiusure P0…P3 spettano ad agent-1:opus.

## Reuse map

| Bisogno | Path e simbolo | Contratto da preservare |
|---------|----------------|-------------------------|
| Bind retry e alert | `src/forwarder.rs` — `run_forwarder` (loop di bind), `BIND_RETRY_DELAY`, `Event::BindFailed/BindRecovered` | retry 5 s, alert a ogni fallimento (throttle nel notifier) |
| Health flag | `src/forwarder.rs` — `ListeningGuard` | `listening` è true solo mentre il listener vive |
| Gauge attivi | `src/forwarder.rs` — `ConnGuard` | inc/dec RAII, anche in caso di panic |
| Sleep cancellabile | `src/forwarder.rs` — `cancelled()` | true se cancellato |
| DNS | `src/forwarder.rs` — `resolve_targets` | literal → nessun resolver; Ok non vuoto |
| DNS change | `src/stats.rs` — `ForwarderStats::observe_addrs` | condiviso col monitor: un solo alert |
| Monitor | `src/monitor.rs` — `run_monitor`, `Downtime`, `DOWN_PROBE_INTERVAL` | soglia e alert una sola volta |
| Fake DNS | `src/testutil.rs` — `spawn_fake_dns`, `resolver_for` | TTL controllabile, flip dell'IP |
| Test e2e | `tests/common/mod.rs` — `claimed_ports`, `ChildGuard`, `rfw_command`, `TempConfig`, `http_get`, `wait_for_stats` | claim set condiviso tra test paralleli |
| Linux raw syscall | `src/forwarder.rs` — `mod splice` | schema `#[cfg(target_os = "linux")]` + fallback portabile |

## Ricerca ed evidenze

Domande esterne materiali: la semantica kernel di GSO/GRO e ICMP su UDP, le API di readiness di tokio/mio,
la libreria alternativa (quinn-udp), le costanti libc e le API di socket2, la fattibilità del cross-compile.

| ID | Domanda / obbligatoria? | Stato | Decisioni / unità / test |
|----|-------------------------|-------|--------------------------|
| R1 | Formato cmsg e limiti di UDP_SEGMENT/UDP_GRO; errori della send GSO (obbligatoria per la fase 3) | CONFIRMED | D18, D19, D21; 3.1; test offload |
| R2 | Come Linux consegna ICMP su UDP connesso (poll/recv/send) (obbligatoria) | CONFIRMED | D7, D8, D14; 2.1, 2.2 |
| R3 | Readiness di tokio 1.53.1 e mio 1.2.4 (Interest::ERROR, try_io) (obbligatoria) | CONFIRMED (corretta in rev 2) | D6, D7a; 2.1, 3.1 |
| R4 | quinn-udp come alternativa (opzionale) | CONFIRMED (scartato) | D19 |
| R5 | Costanti libc 0.2.186 e API socket2 0.6.5 (obbligatoria) | CONFIRMED | 2.1, 3.1 |
| R6 | WSAECONNRESET di Winsock su UDP dopo ICMP (opzionale) | INFERENCE (non riletta) | D8 |
| R7 | Cross-compile locale (obbligatoria per G-XPLAT) | CONFIRMED (locale) | D30; G-XPLAT |

### R1 — UDP_SEGMENT / UDP_GRO
- Fonti: kernel master `include/uapi/linux/udp.h`, `include/linux/udp.h`, `include/net/udp.h`, `net/ipv4/udp.c`
  (`udp_send_skb`, `udp_cmsg_send`), `net/ipv4/udp_offload.c` (GRO). Fetch del 2026-10-18.
- Applicabilità: Linux ≥ 5.0 per UDP_GRO, ≥ 4.18 per UDP_SEGMENT; host di sviluppo kernel 7.0.0-38.
- Fatto documentato:
  - cmsg SOL_UDP/UDP_SEGMENT con payload `u16` e `cmsg_len == CMSG_LEN(2)`;
  - il cmsg UDP_GRO in ricezione porta un `int` gso_size;
  - `UDP_MAX_SEGMENTS` vale 128 in master (64 nei kernel più vecchi);
  - la send GSO fallisce con EMSGSIZE/EINVAL (segmento > fragsize, datalen > gso_size × max, `no_check_tx`) e con EIO (route xfrm);
  - impostare UDP_GRO abilita anche ACCEPT_L4, quindi su loopback gli skb GSO arrivano intatti con il cmsg.
- Inferenza del supervisore: GRO coalescia ≤ 64 segmenti (UDP_GRO_CNT_MAX), per cui un buffer GRO entra
  sempre in una sola send GSO; con un cap a 64 si è sicuri anche sui kernel vecchi.
- Check locale: nessuno ancora; i test loopback della 3.1 lo verificano.
- Impatto: D19 (fallback sticky su qualunque dei tre errori), cap a 64, buffer 128 KiB con MSG_TRUNC.

### R2 — ICMP su UDP connesso
- Fonti: kernel `net/core/datagram.c` `datagram_poll`, `net/ipv4/udp.c` `__udp4_lib_err`, `sock_error`. Fetch del 2026-10-18.
- Fatto documentato:
  - un errore ICMP imposta `sk_err` e il poll riporta **solo EPOLLERR**, non EPOLLIN;
  - l'errore viene consumato dalla prima `recv` o `send` (`sock_error` usa xchg);
  - si riporta solo se il socket è connesso (harderr: port e host unreachable; net unreachable no, senza IP_RECVERR);
  - su loopback non c'è rate-limit ICMP; altrove circa 1 ICMP/s per destinazione con burst 6.
- Inferenza: un waiter solo-READABLE non si sveglia mai su ICMP, e `try_recv` di tokio (interest READABLE)
  non consumerebbe l'errore, producendo un busy loop. Servono quindi `Interest::ERROR` sul wait e un `try_io` con
  interest ERROR (D7a, rev 2: non un interest combinato). L'errore può arrivare anche sulla `send` (advisor), da qui il classificatore unico.
- Impatto: D7, D8, D8a e la truth table di D14 (finestra quiet di 3 s ≥ 2 periodi di refill ICMP).

### R3 — readiness di tokio e mio
- Fonti locali: `~/.cargo/registry/.../tokio-1.53.1`:
  - `src/runtime/io/registration.rs:183` `try_io`;
  - `src/io/ready.rs:238` `from_interest`;
  - `src/net/udp.rs:435` `ready`, `:1480` `try_io`.
  Più `mio-1.2.4/src/sys/unix/selector/epoll.rs:160`. Letti il 2026-10-18.
- Fatto:
  - `try_io` non esegue la closure se la readiness filtrata è vuota e azzera la readiness solo su WouldBlock;
  - `Interest::ERROR` mappa `Ready::ERROR`;
  - **rev 2 (correzione)**: `Interest::mask()` (`src/io/interest.rs`) è un `match` sui flag singoli; un interest
    combinato (READABLE|ERROR) mappa `Ready::EMPTY`, quindi `try_io` combinato non esegue mai la closure.
    `ready()` invece onora gli interest combinati. Osservato in 2.1: spin al 100 % CPU. Da qui D7a;
  - in mio `is_readable` = EPOLLIN|EPOLLPRI (EPOLLERR escluso) e `is_error` = EPOLLERR.
- Inferenza: un socket appena registrato ha readiness vuota, quindi `try_send` darebbe WouldBlock senza
  syscall. Da qui D6 (send raw per l'upstream).
- Impatto: D6, D7; oracolo `first_datagram_of_a_new_session_is_never_dropped`.

### R4 — quinn-udp 0.5.16
- Fonte: sorgente quinn-udp `src/unix.rs`, fetch del 2026-10-18.
- Fatto:
  - `UdpSocketState::new` forza IP_PMTUDISC_PROBE / IPV6_DONTFRAG, il che rompe i datagrammi frammentati
    (es. risposte DNS grandi);
  - `send` converte gli errori non-WouldBlock in Ok, il che impedisce di contare i drop.
- Impatto: D19, modulo scritto a mano.

### R5 — libc 0.2.186 e socket2 0.6.5
- Fonti locali: registry cargo.
  - libc espone `UDP_SEGMENT`=103, `UDP_GRO`=104, `SOL_UDP`, `CMSG_*`, `MSG_TRUNC`, `SO_NO_CHECK`.
  - socket2 espone:
    - `SockAddr::try_init`, `as_ptr() -> *const SockAddrStorage`, `len()`, `as_socket()`;
    - `Domain::for_address`;
    - `impl Read for &Socket`;
    - `From<Socket> for std::net::UdpSocket`;
    - `SockRef: From<&S: AsFd/AsSocket>`.
- Applicabilità: `msghdr.msg_iovlen` e `msg_controllen` hanno tipi diversi tra gnu e musl, quindi vanno castati con `as _`.

### R6 — Windows WSAECONNRESET
- Fonte: comportamento Winsock noto (SIO_UDP_CONNRESET attivo di default), non riletto in questa sessione.
- Stato: INFERENCE, opzionale. Classificare ConnectionReset come Icmp sull'upstream e come transitorio sul
  listener è sicuro in entrambi i casi. Nessun test runtime Windows disponibile; G-XPLAT verifica solo la compilazione.

### R7 — Cross-compile
- Check locale del 2026-10-18:
  - `cargo check --target x86_64-pc-windows-gnu` fallisce (manca `x86_64-w64-mingw32-gcc`, richiesto da ring);
  - fallisce anche darwin;
  - Docker 29.9.1 è presente; `Dockerfile.build` installa mingw, zig e cargo-zigbuild e l'SDK macOS 11.3;
  - `build.sh` scrive in `dist/`, dove ci sono binari locali del 2026-09-18.
- Impatto: D30, con G-XPLAT eseguito in Docker con target dir su volume e senza toccare `dist/`.

## Invarianti

| ID | Significato | Test di guardia |
|----|-------------|-----------------|
| I-1 | Label TCP byte-identiche a v1.0.6 | `config::tests::tcp_label_is_byte_identical_to_v1_0_6`, `test_label` |
| I-2 | Confini dei datagrammi preservati 1:1, inclusa lunghezza 0 e 65507 | `udp::tests::datagram_boundaries_are_preserved_for_all_sizes`; fase 3: `gro_gso_path_preserves_datagram_boundaries` |
| I-3 | Nessun cross-talk: le risposte arrivano solo al client proprietario della sessione | `udp::tests::each_client_gets_its_own_session_and_only_its_replies` |
| I-4 | Il listener non attende mai su un upstream lento; i drop sono contati | review 2.1 (nessun `.await` nel percorso client→upstream) + `forward_upstream` |
| I-5 | Sessioni ≤ udp_max_sessions; gauge = sessioni vive; torna a 0 dopo l'idle | `session_cap_drops_and_counts_new_flows`, `idle_sessions_expire_and_release_their_slot`, e2e `idle_session_expires` |
| I-6 | Lo stop di un forwarder UDP ritorna solo a porta rilasciata | `cancel_releases_the_port_before_returning`, `manager::tests::stop_one_releases_the_udp_port` |
| I-7 | Nessuno stato committato avvia un UDP come TCP | `udp_forwarders_are_rejected_until_the_engine_lands` (1.1→2.2), e2e env/CLI |
| I-8 | I percorsi per-datagramma non loggano sopra debug; i drop loggano rate-limited | review + e2e `udp_drops_are_rate_limited_in_logs` |
| I-9 | Hot reload: timeout live; il retarget fa rebind senza EADDRINUSE; TCP e UDP sulla stessa porta indipendenti | `session_timeout_is_read_live`, e2e `yaml_protocol_udp_and_hot_reload_retarget`, `tcp_and_udp_share_a_port_independently` |
| I-10 | Reload: le flag CLI vincono, i forwarder env/CLI restano pinned | `manager::tests::reload_*`, e2e `reload_keeps_*` |
| I-11 | SIGTERM esce in ≤ 3 s con sessioni UDP idle | e2e `sigterm_exits_fast_with_idle_udp_sessions` |
| I-12 | GRO/GSO spento di default; acceso, produce la stessa sequenza di datagrammi | fase 3 `gro_gso_path_preserves_datagram_boundaries`, e2e `udp_gro_gso_flag_*` |
| I-13 | Le serie Prometheus TCP restano invariate; le famiglie datagram esistono solo per UDP | `metrics::tests::tcp_only_exposition_has_no_datagram_families` |
| I-14 | Il primo datagramma di una sessione nuova non si perde mai | `udp::tests::first_datagram_of_a_new_session_is_never_dropped` (100×) |
| I-15 | Il monitor UDP non segnala ripristino senza evidenza positiva | `monitor::tests::udp_verdict_truth_table` |

## Rischi

| Rischio | Mitigazione di design | Verifica |
|---------|----------------------|----------|
| Esaurimento fd con molte sessioni | cap globale, gestione EMFILE rate-limited, raise e warn RLIMIT (D24), README | unit `fd_budget_math`, review |
| Flood con sorgenti spoofate | cap, SipHash, eviction 1 s, `open_session` sincrono ~4 syscall | test cap, review |
| SO_RCVBUF limitato dal kernel | readback + warn (D22), README `net.core.rmem_max` | review dei log |
| Cambio DNS con sessioni verso un IP rimosso | chiusura delle sessioni non nel set (D10) | `sessions_follow_a_dns_change_and_old_ones_close` |
| ICMP diverso per OS | classificatore unico (D8), ConnectionReset come Icmp | `classify_udp_err_covers_every_kind` |
| Flapping del monitor | truth table pura, Hold senza alert (D14) | `udp_verdict_truth_table`, e2e webhook |
| Compile non-Linux | G-XPLAT a P2 e P3 | log del gate |
| Chiave YAML sbagliata (`protcol`) → TCP | documentato nel README (spelling esatto) | — |
| Regressione TCP | percorso TCP intatto, solo plumbing; G-PERF-TCP | G-PERF-TCP mediana ≤ 1.15× baseline |
| Bind wildcard multi-homed | limitazione documentata | README |

## Strategia di verifica

- G-BASE (già pass a c50db1f) e G-PERF-TCP-BASE (da catturare all'apertura di 0.1, prima di ogni edit) sono la baseline.
- Ogni sub-phase esegue G-HYG (fmt + clippy) e i gate focalizzati G-U<id>, con conteggio dei test scoperti.
- Ogni closure P<N> esegue G-PHASE (fmt, clippy, `cargo test --all` debug e release).
- P2 aggiunge G-XPLAT, G-PERF-TCP e G-UDP-BENCH (evidenza numerica).
- P3 aggiunge G-GSO-BENCH, G-PERF-TCP, G-XPLAT e G-FINAL, che comprende lo scenario di riferimento
  (`reference_scenario_tcp_and_udp_side_by_side`, esteso in 3.2 con la variante `--udp-gro-gso`).
- STATE.md §3 è il registro dei comandi.

## Riepilogo assegnazioni

| Unità | Implementatore | Review del supervisore richiesta |
|-------|----------------|----------------------------------|
| 0.1 | agent-2:sonnet | agent-1:opus dopo il diff (percorso di reload) |
| 1.1 | agent-2:sonnet | agent-1:opus dopo il diff (compatibilità di config e label) |
| 1.2 | agent-3:haiku | agent-1:opus dopo il diff (serie metriche) |
| 2.1 | agent-2:sonnet | agent-1:opus **prima** (dispatch bundle) e **dopo** il diff (concorrenza e lifecycle) |
| 2.2 | agent-2:sonnet | agent-1:opus dopo il diff (monitor, un-gate) |
| 2.3 | agent-3:haiku | agent-1:opus a P2 (accuratezza dei doc) |
| 3.1 | agent-2:sonnet | agent-1:opus dopo il diff (codice unsafe) |
| 3.2 | agent-2:sonnet | agent-1:opus dopo il diff |
| P0–P3 | agent-1:opus | review di fase completa |
