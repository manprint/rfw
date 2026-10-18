//! rfwbench — load generator and echo backend used by the rfw benchmark suite.
//!
//! Every client mode prints exactly one JSON object per phase on stdout so the
//! Python harness can consume it; diagnostics go to stderr.

mod echo;
mod hist;
mod tcp;
mod udp;
mod util;

const USAGE: &str = "usage: rfwbench <echo|rr|cps|hold|integrity|udprr|udpflood|udphold|pin|pipeprobe> [--key value]...";

fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let Some(mode) = argv.first() else {
        util::die(USAGE)
    };
    let a = util::Args::parse(&argv[1..]);
    match mode.as_str() {
        "echo" => echo::run(&a),
        "rr" => tcp::rr(&a),
        "cps" => tcp::cps(&a),
        "hold" => tcp::hold(&a),
        "integrity" => tcp::integrity(&a),
        "udprr" => udp::udprr(&a),
        "udpflood" => udp::udpflood(&a),
        "udphold" => udp::udphold(&a),
        "pin" => util::pin(&a),
        "pipeprobe" => util::pipeprobe(&a),
        _ => util::die(USAGE),
    }
}
