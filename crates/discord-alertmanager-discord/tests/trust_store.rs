//! Guards the patched trust store the gateway socket verifies `gateway.discord.gg` against.
//!
//! serenity 0.12 opens the gateway through tokio-tungstenite 0.21, which builds its rustls
//! verifier from `webpki_roots::TLS_SERVER_ROOTS`. Cargo resolves every copy of webpki-roots 1.x
//! in the graph to the same crate, so the store this test reads is the one the gateway uses.

/// The DER-encoded organisation name of the eMudhra (emSign) hierarchy, as it appears inside a
/// trust anchor's subject.
const EMUDHRA: &[u8] = b"eMudhra Technologies Limited";

// RUSTSEC-2026-0382: rustls-webpki 0.102, the line the gateway uses, accepts paths through
// emSign intermediates whose private keys are public. deny.toml ignores the advisory on the
// strength of the vendored store dropping the anchor, so this fails if the patch ever stops
// applying while the ignore is still in place.
#[test]
fn trusts_no_emsign_anchor() {
    let emsign = webpki_roots::TLS_SERVER_ROOTS
        .iter()
        .filter(|anchor| {
            anchor
                .subject
                .as_ref()
                .windows(EMUDHRA.len())
                .any(|window| window == EMUDHRA)
        })
        .count();
    assert_eq!(emsign, 0, "the trust store still carries an emSign anchor");
}
