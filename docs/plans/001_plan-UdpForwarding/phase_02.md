# Fase 1 — Protocollo nella configurazione e plumbing di stats, metriche e alert

Intent:
- `protocol` (tcp|udp) esiste in CLI, env e YAML, con label, duplicati e nuovi settings UDP;
- stats, metriche, JSON, traffic report e startup report sanno distinguere i forwarder UDP.

Ogni forwarder UDP viene però **rifiutato** dal gate D17: nessun comportamento UDP è ancora eseguibile e il TCP resta identico.
Prerequisiti: P0 DONE.
Chiusura di fase: P1; review di agent-1:opus.

## Contratto di stato e ownership
Leggere prima STATE.md §0–1.
- Aprire l'unità prima degli edit; chiudere con evidenze.
- Un worker delegato lavora solo nella sua unità OPEN.
- Il design mancante va ad agent-1:opus.

## Contesto di design locale
Plan revision: 1.

**Decisioni**
- **D1/D1a — suffisso CLI/env.** Grammatica di `parse_forwarder(s)`:
  1. `s.rsplit_once('/')`. Se `None`, corpo = `s` e protocollo = Tcp.
  2. Altrimenti `(corpo, suffix)`. `suffix.eq_ignore_ascii_case("tcp")` → Tcp;
     `suffix.eq_ignore_ascii_case("udp")` → Udp; qualsiasi altro, incluso il vuoto →
     `Err(format!("Unknown protocol suffix '/{suffix}' in '{s}': expected /tcp or /udp"))`.
  3. `corpo.split(':')` deve dare 4 parti, altrimenti
     `Err(format!("Invalid forwarder format '{s}': expected local_host:local_port:remote_host:remote_port[/tcp|/udp]"))`.
     Nota: il messaggio mostra `s` intero.
  4. Errori di porta: testi invariati (`Invalid local port '…': …`, `Invalid remote port '…': …`).

  | Input | Esito |
  |---|---|
  | `a:1:b:2` | Tcp |
  | `a:1:b:2/udp` | Udp |
  | `a:1:b:2/UDP` | Udp |
  | `a:1:b:2/Tcp` | Tcp |
  | `a:1:b:2/sctp` | `Unknown protocol suffix '/sctp' in 'a:1:b:2/sctp': expected /tcp or /udp` |
  | `a:1:b:53/` | `Unknown protocol suffix '/' in 'a:1:b:53/': …` |
  | `/udp` | `Invalid forwarder format '/udp': …` |
  | `a:b:2/udp` | `Invalid forwarder format 'a:b:2/udp': …` |

  Per l'env vale la policy esistente: una voce invalida produce un warn "Ignoring invalid env forwarder" e
  viene saltata.
- **D2 — YAML.** `protocol: tcp|udp`, con default tcp e alias `TCP`/`UDP`. Un valore sconosciuto è un errore di
  parse serde, che rientra nel percorso di errore esistente.
- **D3 — label.**
  - TCP: `"{lh}:{lp}->{rh}:{rp}"`, byte-identica a v1.0.6 (I-1).
  - UDP: `"{lh}:{lp}->{rh}:{rp}/udp"`.
- **D4 — duplicati.** La chiave è `(protocol, local_addr())`.
  - Messaggio TCP invariato: `Duplicate local address '{addr}'. Each forwarder needs a unique local endpoint.`
  - Messaggio UDP: `Duplicate local address '{addr}/udp'. Each forwarder needs a unique local endpoint.`
  - TCP e UDP sulla stessa porta: OK.
- **D12 — settings.**
  - `udp_session_timeout_secs: u64`, default 60: validate `>= 1`, clamp al reload a minimo 1.
  - `udp_max_sessions: usize`, default 16384: validate `1..=MAX_UDP_SESSIONS` con
    `pub const MAX_UDP_SESSIONS: usize = 1_048_576`; clamp al reload nel range con warn.
  - CLI: `--udp-session-timeout <SECS>`, `--udp-max-sessions <N>`.
- **D17 — gate.** `fn reject_unsupported_protocols(fwd: &[ForwarderConfig]) -> Result<(), anyhow::Error>`:
  - errore se c'è almeno un forwarder Udp, con messaggio
    `UDP forwarder '{label}' is not supported by this build yet; refusing to start it`;
  - chiamato in `load_forwarders_with_source` dopo `validate_no_duplicates` (copre env, CLI e file);
  - chiamato in `load_yaml_config` dopo `validate_no_duplicates` (copre il reload → `ConfigReloadFailed`, si
    tiene la config vecchia);
  - **lo rimuove la 2.2**, insieme ai suoi test.
- **D13 — stats/metriche.**
  - `ForwarderStats` porta il protocollo e tre contatori pubblici `datagrams_sent/received/dropped`.
  - Prometheus: famiglie `rfw_datagrams_{sent,received,dropped}_total` emesse in coda all'esposizione,
    **solo** se c'è almeno uno snapshot UDP e solo per gli snapshot UDP. Nessuna label `protocol`.
  - JSON: `"protocol"` subito dopo `"label"` per tutte le voci; per le voci UDP, in coda, i tre campi datagram.
- **D14 (solo render)** — `ProbeReport.protocol`; il render di `Started` per UDP Ok è
  `• {label} ✅ resolved to {addr} ({ms} ms, udp: passive check)`.

**Invarianti**
- I-1: label TCP byte-identiche (`tcp_label_is_byte_identical_to_v1_0_6`).
- I-7: nessun UDP avviato (`udp_forwarders_are_rejected_until_the_engine_lands`, e2e `udp_forwarder_is_refused_until_supported`).
- I-13: serie TCP invariate e famiglie datagram solo per UDP (`tcp_only_exposition_has_no_datagram_families`).

**Regola dead-code** (crate binario + `clippy -D warnings`): ogni simbolo nuovo deve essere usato da codice non
di test nella stessa unità; gli helper usati solo dai test vanno marcati `#[cfg(test)]`. I simboli UDP del motore
(knob, contatori in scrittura) arrivano solo in 2.1.

**R**: N/A per la fase 1 (config e plumbing interni).

## Sub-phase

### 1.1 Protocollo in config: tipo, parsing, label, duplicati, settings UDP, gate
- **Model:** agent-2:sonnet
- **Assignment:** implementazione + test; agent-1:opus fa la review dopo il diff (compatibilità della config,
  label TCP, gate su tutte le sorgenti).
- **Files:**
  - READ: `src/config.rs` (`ForwarderConfig`, `ConfigFile`, `Settings`, `validate`, `clamp_in_place`,
    `CliArgs`, `apply_cli_overrides` [da 0.1], `parse_forwarder`, `load_yaml_config`,
    `load_forwarders_with_source` [da 0.1], `validate_no_duplicates`, mod `tests`).
  - WRITE:
    - `src/config.rs`;
    - letterali `ForwarderConfig { … }` nei test: `src/forwarder.rs` (4 siti in mod tests), `src/monitor.rs`
      (3 siti), `src/manager.rs` (helper `cfg`), `src/config.rs` (test);
    - `tests/e2e_lifecycle.rs`.
  - NEW: nessuno.
- **Change:**
  Preconditions: P0 DONE; `apply_cli_overrides` e `load_forwarders_with_source` esistono.
  Contract: vedi D1/D1a, D2, D3, D4, D12, D17.
  Scope:
  - Nessun cambio al percorso TCP né a `manager`/`forwarder`, salvo i letterali dei test.
  - Nessun knob runtime UDP (arriva in 2.1).
  - Non aggiungere `#[serde(default)]` a `forwarders`: una lista vuota fermerebbe tutto al reload.
  Steps:
  1. S1 — In `src/config.rs`, sopra `ForwarderConfig`:
     ```rust
     /// Transport of a forwarder. TCP is the default everywhere (CLI, env, YAML).
     #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
     #[serde(rename_all = "lowercase")]
     pub enum Protocol {
         #[default]
         #[serde(alias = "TCP")]
         Tcp,
         #[serde(alias = "UDP")]
         Udp,
     }
     impl Protocol {
         pub fn as_str(self) -> &'static str {
             match self { Protocol::Tcp => "tcp", Protocol::Udp => "udp" }
         }
     }
     ```
     - In `ForwarderConfig` aggiungere come **ultimo** campo `#[serde(default)] pub protocol: Protocol,`, con doc
       comment "`tcp` (default) or `udp`".
     - `label()`: `let base = format!("{}:{}->{}:{}", …);`. Per Tcp → `base`; per Udp →
       `format!("{base}/{}", self.protocol.as_str())`.
     - Expected: compila dopo S2.
  2. S2 — Aggiungere `protocol: Protocol::Tcp` a ogni letterale `ForwarderConfig { … }` già esistente.
     Siti noti (hint):
     - `src/config.rs`: `parse_forwarder` (sostituito in S3) e test ~941, ~947, ~959;
     - `src/forwarder.rs`: test ~895, ~935, ~1107, ~1217;
     - `src/manager.rs`: helper `cfg` ~585;
     - `src/monitor.rs`: ~317, ~441, ~447.

     Trovarli con `tokensave_search {"query":"ForwarderConfig {","literal":true}`.
     Expected: `cargo build --all-targets` ok.
  3. S3 — Riscrivere `parse_forwarder` secondo D1a. Aggiornare il doc comment di `CliArgs::forwarders` in
     "Forwarders in format local_host:local_port:remote_host:remote_port[/tcp|/udp]".
     Expected: tutti i casi della tabella D1a.
  4. S4 — `validate_no_duplicates` con chiave `(f.protocol, f.local_addr())` e messaggi D4.
  5. S5 — Settings:
     - campi con doc comment:
       ```rust
       /// Seconds a UDP session (one client address) may stay idle, no datagram
       /// in either direction, before its upstream socket is closed. Live on reload.
       #[serde(default = "default_udp_session_timeout")]
       pub udp_session_timeout_secs: u64,
       /// Upper bound on concurrent UDP sessions across all UDP forwarders.
       /// Each session holds one socket (one file descriptor). Startup-only.
       #[serde(default = "default_udp_max_sessions")]
       pub udp_max_sessions: usize,
       ```
     - `fn default_udp_session_timeout() -> u64 { 60 }`, `fn default_udp_max_sessions() -> usize { 16384 }`,
       `pub const MAX_UDP_SESSIONS: usize = 1_048_576;`, voci in `impl Default for Settings`.
     - `validate()`, prima del blocco webhook:
       ```rust
       if self.udp_session_timeout_secs == 0 { anyhow::bail!("udp_session_timeout_secs must be >= 1"); }
       if !(1..=MAX_UDP_SESSIONS).contains(&self.udp_max_sessions) {
           anyhow::bail!("udp_max_sessions must be between 1 and {MAX_UDP_SESSIONS} (got {})", self.udp_max_sessions);
       }
       ```
     - `clamp_in_place()`: `floor_u64("udp_session_timeout_secs", &mut self.udp_session_timeout_secs, 1);` più il
       clamp di `udp_max_sessions` a `1..=MAX_UDP_SESSIONS`, con lo stesso warn
       `field/got/clamped_to "Invalid setting clamped"` usato per `buffer_bytes`.
  6. S6 — CLI:
     - in `CliArgs`, prima di `log_dir`:
       ```rust
       /// Seconds a UDP session may stay idle before it is closed (default 60)
       #[arg(long = "udp-session-timeout", value_name = "SECS")]
       pub udp_session_timeout_secs: Option<u64>,
       /// Max concurrent UDP sessions across all UDP forwarders (default 16384)
       #[arg(long = "udp-max-sessions", value_name = "N")]
       pub udp_max_sessions: Option<usize>,
       ```
     - `None` in `impl Default for CliArgs`;
     - due `if let Some(v)` in `apply_cli_overrides`;
     - `about` del comando invariato fino alla 2.2.
  7. S7 — Gate D17: `reject_unsupported_protocols` (privato, con doc comment "Temporary until sub-phase 2.2:
     …"). Chiamarlo:
     - in `load_forwarders_with_source` dopo `validate_no_duplicates(&forwarders)?`;
     - in `load_yaml_config` dopo `validate_no_duplicates(&cf.forwarders)?`.
  8. S8 — Test (sotto), poi G-HYG + G-U1.1.

  Recovery boundary: nessuno oltre l'apertura e la chiusura dell'unità.
  Failure handling:
  - se un test esistente su `parse_forwarder` o sui duplicati cambia esito, la grammatica o il messaggio non
    rispettano D1a/D4: correggere il codice, non il test;
  - se serve modificare un'asserzione esistente, escalare.
- **Unit tests:** in `config::tests`.
  - `parse_forwarder_accepts_protocol_suffixes`: le 4 righe Ok della tabella D1a; `local_port`/`remote_port`/host
    corretti per `a:1:b:2/udp`.
  - `parse_forwarder_rejects_unknown_or_empty_suffix`: `/sctp`, `53/`, `/udp`, `a:b:2/udp`, con le stringhe
    d'errore esatte della tabella (asserire con `==` sull'`Err`).
  - `tcp_label_is_byte_identical_to_v1_0_6`:
    `ForwarderConfig{local_host:"0.0.0.0", local_port:5432, remote_host:"db.example.com", remote_port:5432, protocol:Tcp}.label() == "0.0.0.0:5432->db.example.com:5432"`.
  - `udp_label_has_the_udp_suffix`: stesso con Udp → `"0.0.0.0:5432->db.example.com:5432/udp"`.
  - `yaml_protocol_defaults_aliases_and_rejects_unknown`, tre file con `write_temp`:
    - nessuna chiave → Tcp; `protocol: udp` → Udp; `protocol: UDP` → Udp. Leggerli con
      `serde_saphyr::from_str::<ConfigFile>` per non passare dal gate.
    - `protocol: sctp` → `load_yaml_config` dà `Err`.
  - `duplicates_are_keyed_by_protocol_and_address`, con `validate_no_duplicates`:
    - tcp+udp sulla stessa `local_addr` → Ok;
    - udp+udp → Err contenente `Duplicate local address '0.0.0.0:53/udp'`;
    - tcp+tcp → Err contenente `Duplicate local address '0.0.0.0:8080'`.
  - `udp_settings_defaults_validate_and_clamp`:
    - default 60 e 16384;
    - `validate` rifiuta timeout 0, max 0 e max `MAX_UDP_SESSIONS+1`, e accetta `MAX_UDP_SESSIONS`;
    - `clamp_in_place` porta timeout 0 → 1, max 0 → 1 e `MAX+5` → `MAX`.
  - `cli_udp_flags_override_settings`:
    `CliArgs::try_parse_from(["rfw","--udp-session-timeout","5","--udp-max-sessions","10","127.0.0.1:1:127.0.0.1:2/udp"])`
    → con `apply_cli_overrides` su `Settings::default()` si ottiene 5 e 10. `cli.forwarders` contiene la stringa con `/udp`.
  - `udp_forwarders_are_rejected_until_the_engine_lands` (**rimosso in 2.2**), sotto `ENV_LOCK`:
    - `load_forwarders_with_source` con CLI `…/udp` → Err contenente "is not supported by this build yet";
    - con env `RFW_FORWARDER_1=…/udp` → stesso Err;
    - `load_yaml_config` su file con `protocol: udp` → stesso Err.
  - Regressione: `test_label`, `test_validate_duplicates`, `test_parse_forwarder_*`,
    `shipped_example_config_parses_and_validates` invariati e verdi.
  - Gate: G-U1.1. Discovery: i 9 nomi nuovi compaiono nell'output.
- **e2e tests:** in `tests/e2e_lifecycle.rs`, accanto a `malformed_forwarder_argument_is_rejected`.
  - `invalid_protocol_suffix_is_rejected` (**permanente**):
    `rfw_command(&["127.0.0.1:1:127.0.0.1:2/sctp"])` con stderr piped → exit non zero, stderr contiene
    `Unknown protocol suffix '/sctp'`.
  - `udp_forwarder_is_refused_until_supported` (**rimosso in 2.2**), due casi nello stesso test:
    - (a) argomento `127.0.0.1:{free_port()}:127.0.0.1:9/udp`;
    - (b) `rfw_command(&[])` con `env_remove` di ogni `RFW_FORWARDER_*` ereditato e
      `.env("RFW_FORWARDER_1", "127.0.0.1:{p}:127.0.0.1:9/udp")`.

    Ognuno: `.output()` → exit non zero entro 10 s, stderr contiene "is not supported by this build yet".
  - Gate: G-U1.1. Discovery: 2 test.
- **Done:**
  - tabella D1a, D3, D4, D12 e D17 implementati;
  - G-HYG e G-U1.1 pass con discovery;
  - review di agent-1:opus registrata;
  - unità chiusa, commit `sub-phase(1.1)`.

### 1.2 Plumbing: stats, Prometheus, JSON, traffic report, startup report
- **Model:** agent-3:haiku
- **Assignment:** implementazione + test meccanici a partire dai contratti esatti qui sotto; agent-1:opus fa la
  review dopo il diff (I-13: output Prometheus TCP invariato).
- **Files:**
  - READ: `src/stats.rs` (`ForwarderStats`, `new`, `snapshot`, `StatsSnapshot`, `StatsRegistry::register`);
    `src/metrics.rs` (`push_metric`, `render_prometheus`, `render_json`, mod tests `snap()`);
    `src/forwarder.rs` (`stats_reporter`); `src/monitor.rs` (`probe_all`); `src/notify.rs` (`ProbeReport`,
    `Event::render` ramo `Started`, test helper `report`); `src/manager.rs` (`start_one`).
  - WRITE: gli stessi file.
  - NEW: nessuno.
- **Change:**
  Preconditions: 1.1 DONE (`Protocol`, `ForwarderConfig::protocol`).
  Contract:
  - `src/stats.rs`:
    - `use crate::config::Protocol;`.
    - Campi nuovi di `ForwarderStats`:
      - `protocol: Protocol` (privato);
      - `pub datagrams_sent: AtomicU64` (datagrammi client→remote inoltrati);
      - `pub datagrams_received: AtomicU64` (datagrammi remote→client inoltrati);
      - `pub datagrams_dropped: AtomicU64` (datagrammi scartati in una qualsiasi direzione).
    - `pub fn with_protocol(label: String, protocol: Protocol) -> Self`: il corpo dell'attuale `new` più i
      quattro campi a 0 e `protocol`.
    - `#[cfg(test)] pub fn new(label: String) -> Self { Self::with_protocol(label, Protocol::Tcp) }`.
    - `StatsSnapshot` ottiene `pub protocol: Protocol, pub datagrams_sent: u64, pub datagrams_received: u64, pub datagrams_dropped: u64`;
      `snapshot()` li riempie (load `Relaxed`).
    - `StatsRegistry::register_with_protocol(&self, label: &str, protocol: Protocol) -> Arc<ForwarderStats>`:
      il corpo dell'attuale `register`, con `with_protocol`. È idempotente per label: un re-register restituisce
      l'esistente e ne conserva il protocollo.
    - `#[cfg(test)] pub async fn register(&self, label: &str) -> Arc<ForwarderStats> { self.register_with_protocol(label, Protocol::Tcp).await }`.
  - `src/manager.rs` `start_one`: entrambe le chiamate `self.stats_registry.register(&label)` diventano
    `register_with_protocol(&label, cfg.protocol)`.
  - `src/metrics.rs`:
    - `render_prometheus`: **dopo** il blocco `rfw_target_reachable` e prima di `out`:
      ```rust
      let udp: Vec<StatsSnapshot> = snaps.iter().filter(|s| s.protocol == Protocol::Udp).cloned().collect();
      if !udp.is_empty() {
          push_metric(&mut out, "rfw_datagrams_sent_total", "Lifetime UDP datagrams forwarded client->remote.", "counter", &udp, |s| s.datagrams_sent);
          push_metric(&mut out, "rfw_datagrams_received_total", "Lifetime UDP datagrams forwarded remote->client.", "counter", &udp, |s| s.datagrams_received);
          push_metric(&mut out, "rfw_datagrams_dropped_total", "Lifetime UDP datagrams dropped (socket buffer full, session limit, target not resolved, send error).", "counter", &udp, |s| s.datagrams_dropped);
      }
      ```
    - `render_json`: l'oggetto diventa `{"label":"…","protocol":"tcp|udp","bytes_sent":…` (resto invariato fino a
      `"target_reachable":…`). Per `Protocol::Udp`, prima di `}`, aggiungere
      `,"datagrams_sent":N,"datagrams_received":N,"datagrams_dropped":N`.
    - Test: aggiornare il letterale `snap()` (`protocol: Protocol::Tcp`, i tre contatori a 0); gli altri usano `..snap()`.
  - `src/forwarder.rs`:
    - estrarre `fn traffic_row(s: &StatsSnapshot) -> Option<String>` (privato, usato da `stats_reporter`):
      - restituisce `None` se `bytes_sent == 0 && bytes_received == 0 && active_connections == 0 && datagrams_dropped == 0`;
      - per Tcp restituisce **la stessa** stringa di oggi:
        `format!("  {:<45} sent={:>10}  recv={:>10}  tx={:>10}/s  rx={:>10}/s  active={}", …)`;
      - per Udp restituisce
        `format!("  {:<45} sent={:>10}  recv={:>10}  tx={:>10}/s  rx={:>10}/s  sessions={}  dgrams tx={} rx={} drop={}", label, sent, recv, tx_rate, rx_rate, s.active_connections, s.datagrams_sent, s.datagrams_received, s.datagrams_dropped)`.
    - `stats_reporter` fa `if let Some(row) = traffic_row(s) { info!("{row}"); }`.
  - `src/notify.rs`: `ProbeReport` ottiene `pub protocol: Protocol`.
    - Nel render di `Started`, ramo `Ok((addr, latency))`: se `f.protocol == Protocol::Udp` →
      `format!("• {} ✅ resolved to {} ({} ms, udp: passive check)", mono(&f.label), mono(&addr.to_string()), latency.as_millis())`;
      altrimenti la stringa TCP invariata.
    - Il test helper `report()` imposta `protocol: Protocol::Tcp`.
  - `src/monitor.rs` `probe_all`: `ProbeReport { …, protocol: cfg.protocol }`. Nessun dispatch UDP qui: arriva in 2.2.

  Scope: nessun cambio di comportamento per il TCP. Output TCP di Prometheus, JSON (escluso il nuovo campo
  `"protocol"`), traffic report e Started identici.
  Steps:
  1. S1 — stats.rs + manager.rs (contratti sopra). Expected: `cargo build --all-targets` ok.
  2. S2 — metrics.rs (render + `snap()`). Expected: i test metrics esistenti sono verdi.
  3. S3 — forwarder.rs `traffic_row`, notify.rs, monitor.rs.
  4. S4 — test sotto, G-HYG, G-U1.2.

  Recovery boundary: nessuno.
  Failure handling: un warning dead-code su un simbolo nuovo indica che manca il suo uso non-test (rileggere il
  contratto); non aggiungere `#[allow(dead_code)]`. In caso di dubbio escalare ad agent-1:opus.
- **Unit tests:**
  - `stats::tests::registry_records_the_protocol`:
    - `reg.register_with_protocol("a:1->b:2/udp", Protocol::Udp)` → `snapshot().protocol == Udp`;
    - un secondo `register_with_protocol` con la stessa label e `Tcp` → `Arc::ptr_eq`, protocollo ancora Udp,
      `collect().len() == 1`.
  - `stats::tests::snapshot_carries_datagram_counters`: `fetch_add` 3/2/1 sui tre contatori → snapshot 3/2/1.
  - `metrics::tests::tcp_only_exposition_has_no_datagram_families`: `render_prometheus(&[snap()])` non contiene
    `rfw_datagrams_` né `protocol=` e contiene `rfw_bytes_sent_total{label="a:1->b:2"} 100`.
  - `metrics::tests::udp_forwarders_expose_datagram_counters`:
    - snaps = `[snap(), StatsSnapshot{label:"a:1->b:2/udp", protocol:Udp, datagrams_sent:5, datagrams_received:4, datagrams_dropped:1, ..snap()}]`;
    - contiene `# TYPE rfw_datagrams_sent_total counter`, `rfw_datagrams_sent_total{label="a:1->b:2/udp"} 5`,
      `rfw_datagrams_received_total{label="a:1->b:2/udp"} 4`, `rfw_datagrams_dropped_total{label="a:1->b:2/udp"} 1`;
    - **non** contiene `rfw_datagrams_sent_total{label="a:1->b:2"}`;
    - contiene `rfw_bytes_sent_total{label="a:1->b:2/udp"} 100`.
  - `metrics::tests::json_carries_protocol_and_udp_datagram_fields`, stessi snaps:
    - l'output contiene `{"label":"a:1->b:2","protocol":"tcp","bytes_sent":100`;
    - contiene `"protocol":"udp"` e `"datagrams_sent":5,"datagrams_received":4,"datagrams_dropped":1}`;
    - la prima voce (prima di `},{`) non contiene `datagrams`.
  - `forwarder::tests::traffic_row_formats_tcp_unchanged_and_udp_with_datagrams`:
    - snapshot TCP con byte > 0: la riga è uguale alla `format!` TCP costruita nel test con gli stessi argomenti;
    - snapshot Udp: contiene `sessions=1  dgrams tx=5 rx=4 drop=1`;
    - snapshot a zero ma con `datagrams_dropped: 2` → `Some`; tutto zero → `None`.
  - `notify::tests::started_reports_udp_targets_as_resolved`: `ProbeReport{label:"a:1->b:53/udp", target:"b:53", protocol:Udp, outcome:Ok((10.0.0.5:53, 3ms))}`.
    - `Event::Started{forwarders: vec![r]}.render(&instance, 0)` contiene
      ``• `a:1->b:53/udp` ✅ resolved to `10.0.0.5:53` (3 ms, udp: passive check)``;
    - non contiene "reachable at". Usare lo stesso `InstanceInfo` dei test esistenti.
  - Gate: G-U1.2. Discovery: i 7 nomi compaiono nell'output.
- **e2e tests:** N/A. UDP è ancora bloccato dal gate; il JSON TCP è coperto dagli e2e esistenti
  (`wait_for_stats` usa sottostringhe) che girano a P1.
- **Done:**
  - contratti implementati;
  - G-HYG e G-U1.2 pass con discovery;
  - review di agent-1:opus (I-13) registrata;
  - unità chiusa, commit `sub-phase(1.2)`.

## Gate di fase e chiusura
- Gate richiesti: G-PHASE.
- Asserzioni:
  - tutti i test di baseline e di P0 verdi;
  - nuovi test di 1.1 e 1.2 scoperti ed eseguiti;
  - `e2e_lifecycle` include `invalid_protocol_suffix_is_rejected` e `udp_forwarder_is_refused_until_supported`.
- Obbligo README: **nessuna modifica in questa fase**, per scelta. UDP non è ancora utilizzabile e il campo JSON
  additivo `"protocol"`, le flag `--udp-*` e la sintassi `/udp` vengono documentati tutti insieme nella 2.3.
  P1 registra "README unchanged by design; obligation carried by 2.3".
- agent-1:opus fa la review di I-1, I-7, I-13, verifica il gate su tutte e tre le sorgenti e che nessun simbolo
  sia dead code.
- Aprire P1, registrare review e risultato, segnare la fase DONE, commit `phase(1)`.
