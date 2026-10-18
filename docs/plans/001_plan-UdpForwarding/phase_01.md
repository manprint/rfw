# Fase 0 — Fix B1: precedenza della configurazione al hot-reload (TCP)

Intent: al hot-reload le flag CLI continuano a vincere sul blocco `settings:` del file, e i forwarder che
all'avvio venivano da env (`RFW_FORWARDER_N`) o dalla CLI posizionale restano quelli. La lista `forwarders:`
del file viene sincronizzata solo quando era la sorgente dei forwarder all'avvio.
Prerequisiti: nessuno (baseline `c50db1f`, G-BASE pass).
Chiusura di fase: P0; review di agent-1:opus.

## Contratto di stato e ownership
Leggere prima STATE.md §0–1 (scope, ownership, recovery, check, regole di commit).
- Aprire l'unità prima di qualsiasi edit e fare checkpoint al confine di recovery indicato; chiudere con evidenze.
- Un worker delegato lavora solo nella sua unità OPEN.
- Il design mancante va ad agent-1:opus: non inventarlo.

## Contesto di design locale
Plan revision: 1.

**Bug B1 (confermato leggendo il sorgente a `c50db1f`)**
- `src/manager.rs` — `watch_config_file`, ramo `Ok(loaded)`:
  - chiama `manager.update_knobs(&loaded.settings)`: settings del file **senza** override CLI;
  - chiama `manager.sync(&loaded.forwarders)`: **sempre**.
- Conseguenze:
  - `rfw -f cfg.yml --buffer-bytes 2048 …`: al primo reload `buffer_bytes` torna al valore del file. Lo stesso vale per `--splice`, `--probe-interval` ecc.
  - `RFW_FORWARDER_1=… rfw -f cfg.yml` (oppure forwarder posizionali + `-f`): al primo reload i forwarder env/CLI vengono fermati e sostituiti da quelli del file.
  - Il README dice "CLI flags take precedence" e "env vars … completely replace CLI args and YAML config": il comportamento contraddice la documentazione.
  - Il deploy di produzione (env, senza `-f`) non è affetto, perché il watcher parte solo con `-f`.

**D20 — politica di reload**
- Al reload: settings = settings del file + `apply_cli_overrides(&mut settings, &policy.cli)`, poi `update_knobs`.
- `sync` della lista solo se `policy.source == ForwarderSource::File`. Altrimenti un info log esatto e nessuna modifica ai forwarder.

**I-10**: reload → le flag CLI vincono e i forwarder env/CLI restano pinned.
Guardie: `manager::tests::reload_*` ed e2e `reload_keeps_*`.

**D27 (parte e2e)**: helper `spawn_rfw_logged` che cattura stdout e stderr del figlio, con codici ANSI rimossi e
`RUST_LOG=info` forzato, per asserire sulle righe di log. Lo useranno anche le fasi 2 e 3.

**R**: N/A, lavoro interno sul codice esistente.

### Contratti esatti

`src/config.rs` (NUOVI simboli pubblici, tutti usati da codice non di test):

```rust
/// Where the running forwarder list came from at startup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForwarderSource {
    Env,
    Cli,
    File,
    None,
}

impl std::fmt::Display for ForwarderSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            ForwarderSource::Env => "RFW_FORWARDER_N environment variables",
            ForwarderSource::Cli => "command-line arguments",
            ForwarderSource::File => "config file",
            ForwarderSource::None => "nowhere",
        })
    }
}

/// Apply every CLI flag the operator actually passed over `settings`.
/// Shared by startup (`load_settings`) and hot-reload, so CLI flags win in both.
pub fn apply_cli_overrides(settings: &mut Settings, cli: &CliArgs);

pub fn load_forwarders_with_source(
    cli: &CliArgs,
) -> Result<(Vec<ForwarderConfig>, ForwarderSource), anyhow::Error>;
```

- `apply_cli_overrides` contiene **esattamente** il blocco di override oggi dentro `load_settings`, da
  `if let Some(addr) = &cli.metrics_addr` fino a `unreachable_after_secs`, inclusi `webhook_url` (vuoto → None)
  e `instance_name`. Nessun cambio di semantica.
- `load_settings` diventa: parse del file (codice invariato) → `apply_cli_overrides(&mut settings, cli)` →
  `settings.validate()?` → `Ok(settings)`.
- `load_forwarders_with_source` ha la stessa logica e lo stesso ordine di errori di oggi `load_forwarders`:
  1. parse dei forwarder CLI (errore subito), altrimenti YAML se `-f` (errore subito), altrimenti vuoto;
  2. poi `load_env_forwarders()`: se non vuoto, sostituisce la lista;
  3. poi `validate_no_duplicates`.

  La sorgente è quella della lista effettivamente usata:

  | Lista usata | Sorgente |
  |---|---|
  | env non vuoto | `Env` |
  | altrimenti `cli.forwarders` non vuoto | `Cli` |
  | altrimenti `cli.config_file.is_some()` | `File` (anche con lista vuota) |
  | altrimenti | `None` |

  Se tutte le voci env sono invalide, `load_env_forwarders` restituisce vuoto e quindi vale la sorgente successiva.
- `load_forwarders` resta come wrapper **solo test**:
  `#[cfg(test)] pub fn load_forwarders(cli: &CliArgs) -> Result<Vec<ForwarderConfig>, anyhow::Error> { Ok(load_forwarders_with_source(cli)?.0) }`.
  I test esistenti che la usano restano invariati. In un crate binario, un `pub fn` usato solo dai test fa
  fallire `clippy -D warnings` per dead code: da qui il `#[cfg(test)]`.
- `CliArgs`: `#[derive(Parser, Debug)]` diventa `#[derive(Parser, Debug, Clone)]`.
- **Sicurezza**: `CliArgs` può contenere `webhook_url`, che è una credenziale. Non loggarlo mai con `{:?}`, né
  direttamente né dentro `ReloadPolicy`.

`src/manager.rs`:

```rust
/// What a hot-reload may change, fixed at startup.
#[derive(Debug, Clone)]
pub struct ReloadPolicy {
    /// Re-applied over the file's `settings:` on every reload, so CLI flags keep winning.
    pub cli: CliArgs,
    /// Where the running forwarders came from. Only `File` lets a reload sync the list.
    pub source: ForwarderSource,
}

pub async fn watch_config_file(
    config_path: PathBuf,
    manager: Arc<ForwarderManager>,
    cancel: CancellationToken,
    policy: ReloadPolicy,
);
```

Il ramo `Ok(loaded)` diventa:

```rust
Ok(loaded) => {
    let before: std::collections::HashSet<String> =
        manager.running_labels().await.into_iter().collect();
    let mut settings = loaded.settings;
    apply_cli_overrides(&mut settings, &policy.cli);
    manager.update_knobs(&settings);
    if policy.source == ForwarderSource::File {
        manager.sync(&loaded.forwarders).await;
    } else {
        info!(
            source = %policy.source,
            "Forwarders are pinned by {}: the file's forwarders list is ignored on reload (settings applied)",
            policy.source
        );
    }
    let after: std::collections::HashSet<String> =
        manager.running_labels().await.into_iter().collect();
    // added/removed: codice invariato
    info!(added = ?added, removed = ?removed, "Config reloaded: {} forwarders active", after.len());
    notifier.notify(Alert::ConfigReloaded { active: after.len(), added, removed });
}
```

- Il messaggio usa ora `after.len()`, non `loaded.forwarders.len()`: con forwarder pinned il numero del file sarebbe falso.
- Il ramo `Err` resta invariato.

`src/main.rs`:
- `let (forwarders, source) = config::load_forwarders_with_source(&cli)?;` sostituisce `config::load_forwarders(&cli)?`.
- Subito dopo il ciclo che logga `"  {} -> {}:{}"`, aggiungere:

  ```rust
  if let Some(path) = &cli.config_file {
      if source != config::ForwarderSource::File {
          info!(
              "Forwarders come from {source}; the forwarders list in {} is ignored (its settings block still applies and hot-reloads)",
              path.display()
          );
      }
  }
  ```
- Allo spawn del watcher: `let policy = manager::ReloadPolicy { cli: cli.clone(), source };` va mosso nel task e
  passato come quarto argomento di `watch_config_file`.

## Sub-phase

### 0.1 Reload: override CLI riapplicati e forwarder env/CLI pinned
- **Model:** agent-2:sonnet
- **Assignment:** implementa codice, test e README della fase; agent-1:opus fa la review dopo il diff
  (percorso di reload: nessun cambio con `source == File`, nessuna regressione sul ramo `Err`).
- **Files:**
  - READ: `src/config.rs` (`CliArgs`, `load_settings`, `load_forwarders`, `load_env_forwarders`,
    `validate_no_duplicates`, mod `tests`: `ENV_LOCK`, `ForwarderEnvGuard`, `write_temp`);
    `src/manager.rs` (`watch_config_file`, mod `tests`: `cfg`, `free_port`, `wait_listening`, `manager`);
    `src/main.rs`; `tests/common/mod.rs`; `tests/e2e_hot_reload.rs` (`probe`, `config_with`).
  - WRITE: `src/config.rs`, `src/manager.rs`, `src/main.rs`, `tests/common/mod.rs`,
    `tests/e2e_hot_reload.rs`, `README.md` (sezioni "Configuration override behavior" e "Hot-reload").
  - NEW: nessun file.
- **Change:**
  Preconditions: HEAD = baseline del piano o successivo senza modifiche sui file sopra; G-BASE pass.
  Contract: vedi "Contratti esatti".
  Scope:
  - Con `source == File` il comportamento di reload è byte-identico a oggi: sync, knob e alert.
  - Restano invariati: startup, ramo `Err`, debounce e `ConfigReloaded`, salvo `active`, che già usava `after.len()`.
  - Fuori scope: nessuna modifica al parsing dei forwarder.
  Steps:
  1. S0 — Prima di qualsiasi edit, eseguire **G-PERF-TCP-BASE** (STATE.md §3) e registrare in STATE.md §7 i tre
     tempi e la mediana. Expected: tre run pass, mediana registrata.
  2. S1 — Test prima (devono fallire o non compilare sul codice vecchio):
     - scrivere in `tests/common/mod.rs` `LogTail`/`spawn_rfw_logged`/`strip_ansi`;
     - scrivere i due test e2e e i tre test `manager::tests::reload_*` (vedi Unit tests ed e2e tests).
     - Verificare che gli e2e falliscano sul codice vecchio:
       `cargo test --test e2e_hot_reload reload_keeps -- --nocapture` → 2 test eseguiti, 2 FAIL sull'asserzione
       di attesa del log.
     - Registrare l'output rosso nel checkpoint §6. I test unit di manager non compilano finché non esiste
       `ReloadPolicy`, ed è un rosso accettabile.
  3. S2 — `src/config.rs`:
     - derive `Clone` su `CliArgs`;
     - aggiungere `ForwarderSource` + `Display`;
     - estrarre `apply_cli_overrides` (spostare il blocco, non copiarlo) e riscrivere `load_settings` su di esso;
     - aggiungere `load_forwarders_with_source`;
     - rendere `load_forwarders` un wrapper `#[cfg(test)]`.
     Expected: `cargo test --bin rfw config::tests::` è verde, con i nuovi test inclusi.
  4. S3 — `src/manager.rs`: `ReloadPolicy`, nuova firma di `watch_config_file` e ramo `Ok(loaded)` come da
     contratto. Import: `use crate::config::{apply_cli_overrides, load_yaml_config, CliArgs, ForwarderConfig, ForwarderSource, RuntimeKnobs, Settings};`.
     Expected: compila.
  5. S4 — `src/main.rs`: source, log di avvio, policy al watcher. Expected: `cargo build` ok, e2e S1 verdi.
  6. S5 — README (in inglese, stile esistente):
     - "Configuration override behavior": aggiungere il punto 5: "On hot-reload the same precedence holds:
       command-line flags are re-applied over the file's `settings:` block, and when the forwarders come from
       `RFW_FORWARDER_N` or positional arguments the file's `forwarders:` list is ignored (logged at startup and
       on every reload); only its settings are applied."
     - "Hot-reload": nella nota, dopo la frase sulle chiavi live, aggiungere la frase "CLI flags still win over
       the reloaded `settings:` block."; aggiungere anche il bullet "**Pinned forwarders**: if the forwarders
       were given via env vars or positional arguments, the `forwarders:` list in the file is not applied on
       reload".
     - Correggere la frase "(CLI flags take precedence)" in "(CLI flags take precedence, at startup and on
       hot-reload)".
  7. S6 — Eseguire G-HYG, G-U0.1 e registrare i conteggi.
  Recovery boundary: dopo S1 (test rossi registrati) fare checkpoint §6 con l'elenco dei test scritti e
  dell'output rosso.
  Failure handling:
  - se un test esistente `config::tests::*` cambia esito dopo S2, l'estrazione non è equivalente: ripristinare
    l'ordine originale degli override;
  - un e2e che flappa per timing di watcher non va risolto allungando i timeout oltre 15 s, ma escalando ad agent-1:opus.
- **Unit tests:** tutti nei moduli `tests` esistenti.
  - `config::tests::apply_cli_overrides_overrides_only_given_flags` — input:
    `let mut s = Settings { buffer_bytes: 4096, probe_interval_secs: 30, ..Settings::default() };` e
    `let cli = CliArgs { buffer_bytes: Some(2048), use_splice: true, ..CliArgs::default() };`.
    - `apply_cli_overrides(&mut s, &cli)` → `s.buffer_bytes == 2048`, `s.use_splice`,
      `s.probe_interval_secs == 30`, `s.keepalive_idle_secs == 60`.
    - Con `CliArgs::default()` su un `Settings` clonato: tutti i campi numerici restano uguali; confrontare
      `buffer_bytes`, `probe_interval_secs`, `connect_timeout_secs`, `use_splice`.
  - `config::tests::load_forwarders_reports_its_source` — sotto `ENV_LOCK`:
    - (a) `ForwarderEnvGuard::set(&[("RFW_FORWARDER_1","127.0.0.1:1:127.0.0.1:2")])` e CLI default → `Env`, una voce;
    - (b) `ForwarderEnvGuard::set(&[])` con `cli.forwarders = vec!["127.0.0.1:3:127.0.0.1:4".into()]` → `Cli`;
    - (c) guard vuoto, `cli.config_file = Some(write_temp("src-file", yaml con 1 forwarder))` → `File`;
    - (d) guard vuoto e CLI default → `(vec![], ForwarderSource::None)`;
    - (e) le quattro stringhe `Display` esatte del contratto.
  - `config::tests::load_settings_equals_file_plus_overrides` — `write_temp` con
    `settings: {buffer_bytes: 4096, probe_interval_secs: 30}` e almeno un forwarder; `cli.config_file` = quel
    path; `cli.buffer_bytes = Some(2048)`.
    - `load_settings(&cli)` → buffer 2048, probe 30.
    - `let mut m = load_yaml_config(&path).unwrap().settings; apply_cli_overrides(&mut m, &cli);` → stessi valori.
  - `manager::tests::reload_reapplies_cli_overrides_over_file_settings` — `#[tokio::test]`:
    - helper NEW di test `fn temp_yaml(name: &str, body: &str) -> PathBuf`, che crea una dir unica in
      `std::env::temp_dir()` con pid + nanos e scrive `forwarders.yml`; più `fn yaml(fwd: &ForwarderConfig, settings: &str) -> String`.
    - Fixture: file con `cfg(p, r)` e `settings:\n  buffer_bytes: 4096\n  probe_interval_secs: 30\n`.
      `cli = CliArgs { config_file: Some(path), buffer_bytes: Some(2048), ..Default::default() }`;
      `settings = load_settings(&cli)`; `mgr = Arc::new(ForwarderManager::new(&settings, Notifier::disabled()))`;
      `start_all(&[cfg(p, r)])`.
    - Spawn `watch_config_file(path, mgr.clone(), cancel.clone(), ReloadPolicy { cli: cli.clone(), source: ForwarderSource::File })`.
    - Ogni 500 ms riscrivere il file con `buffer_bytes: 8192` e `probe_interval_secs: 7`, finché
      `mgr.knobs.probe_interval() == Duration::from_secs(7)` (massimo 30 tentativi, altrimenti panic
      "reload never applied").
    - Assert `mgr.knobs.buffer_bytes() == 2048`. Infine `cancel.cancel()` e `mgr.shutdown_all().await`.
  - `manager::tests::reload_does_not_sync_pinned_forwarders` — manager con `cfg(pa, r)` avviato e `wait_listening(pa)`.
    - Il file contiene invece `cfg(pb, r)` e, alla riscrittura, `probe_interval_secs: 7`.
    - Policy `{ cli: CliArgs::default(), source: ForwarderSource::Cli }`; stessa attesa di probe 7 s.
    - Assert: `running_labels() == vec![cfg(pa, r).label()]`; `TcpStream::connect(("127.0.0.1", pb))` fallisce.
  - `manager::tests::reload_syncs_file_forwarders` (controllo positivo) — come sopra con `source: File`.
    Attendere (ripetendo la riscrittura) che `running_labels()` contenga `cfg(pb, r).label()`. Assert che la
    label `pa` non ci sia più e `wait_listening(pb)`.
  - Gate: G-U0.1 (unit). Discovery: `config::tests::` ≥ 3 nuovi nomi nell'output; `manager::tests::reload_` = 3 test.
- **e2e tests:** in `tests/common/mod.rs` e `tests/e2e_hot_reload.rs`.
  - Helper NEW in `tests/common/mod.rs`:
    ```rust
    pub struct LogTail { lines: std::sync::Arc<std::sync::Mutex<Vec<String>>> }
    impl LogTail {
        /// Poll every 50ms until a captured line contains `needle`; false on timeout.
        pub async fn wait_for(&self, needle: &str, within: Duration) -> bool;
        pub fn count(&self, needle: &str) -> usize;
        pub fn lines(&self) -> Vec<String>;
    }
    /// Remove ANSI CSI sequences: ESC '[' … final byte in 0x40..=0x7E.
    pub fn strip_ansi(s: &str) -> String;
    /// Like `spawn_rfw`, but stdout+stderr are piped and captured line by line (ANSI stripped).
    /// Forces RUST_LOG=info; removes inherited RFW_FORWARDER_*, RFW_WEBHOOK_URL and
    /// RFW_INSTANCE_NAME (so a developer shell can never leak a real webhook into a test),
    /// then applies `envs`.
    pub fn spawn_rfw_logged(args: &[&str], envs: &[(&str, &str)]) -> (ChildGuard, LogTail);
    ```
    - Implementazione: `rfw_command(args)` poi `.stdout(Stdio::piped()).stderr(Stdio::piped())`;
      `env_remove` per ogni chiave ereditata con prefisso `RFW_FORWARDER_`, più `RFW_WEBHOOK_URL` e
      `RFW_INSTANCE_NAME`; `.env("RUST_LOG","info")`; poi `envs`.
    - Un `std::thread` per pipe con `BufReader::lines()` → `strip_ansi` → push nel `Vec`.
    - I thread terminano a EOF, cioè quando `ChildGuard` uccide il figlio.
  - `reload_keeps_cli_forwarders_and_ignores_the_files_list`:
    - `cli_port = free_port()`, `file_port = free_port()`, `a = spawn_tagged(b"A")`, `b = spawn_tagged(b"B")`;
      `TempConfig::new("pinned-cli", &config_with(file_port, b, ""))`;
      `spawn_rfw_logged(&["-f", &path, &format!("127.0.0.1:{cli_port}:127.0.0.1:{a}")], &[])`.
    - `wait_port(cli_port)`; `assert!(log.wait_for("Forwarders come from command-line arguments", 5s))`;
      `assert!(log.wait_for("Watching config file", 5s))`.
    - `cfg.rewrite(&config_with(file_port, b, "settings:\n  probe_interval_secs: 7\n"))`;
      `assert!(log.wait_for("forwarders list is ignored on reload", 15s), "{:?}", log.lines())`;
      `assert!(log.wait_for("Config reloaded: 1 forwarders active", 5s))`.
    - Poi `assert_eq!(probe(cli_port).await.as_deref(), Some("A"))` e
      `assert!(TcpStream::connect(("127.0.0.1", file_port)).await.is_err())`.
  - `reload_keeps_env_forwarders`: come sopra, ma forwarder via
    `envs = &[("RFW_FORWARDER_1", fwd.as_str())]` e argomenti solo `["-f", &path]`; needle di avvio
    "Forwarders come from RFW_FORWARDER_N environment variables".
  - Rosso atteso sul codice vecchio: entrambi falliscono su `wait_for("forwarders list is ignored on reload")`
    (prima ancora, il needle di avvio non esiste).
  - Gate: G-U0.1 (e2e). Discovery: `reload_keeps` = 2 test.
- **Done:**
  - le precedenze funzionano come da contratto; README aggiornato;
  - G-PERF-TCP-BASE registrato; G-HYG e G-U0.1 pass con i conteggi attesi;
  - review di agent-1:opus registrata in §7;
  - unità chiusa in STATE.md e commit di completamento `sub-phase(0.1)`, con trailer e file del piano inclusi
    (vedi STATE.md §0 punto 6). I gate di fase completi girano a P0.

## Gate di fase e chiusura
- Gate richiesti: G-PHASE. Asserzioni: tutti i test esistenti verdi (144 unit + 31 e2e alla baseline), più i nuovi
  (≥ 6 unit, 2 e2e); nessun test scoperto in meno della baseline.
- Obbligo README: unità 0.1, sezioni "Configuration override behavior", "Hot-reload" e la frase in
  "Invalid settings are rejected at startup". P0 verifica che corrispondano al codice.
- Ogni sub-phase deve essere DONE. Poi si eseguono i gate di fase.
- agent-1:opus fa la review di I-10, dell'invarianza con `source == File` e dell'assenza di log `{:?}` di `CliArgs`.
- Aprire P0, registrare review e risultato, segnare la fase DONE e chiudere con commit `phase(0)` (full-autonomous).
