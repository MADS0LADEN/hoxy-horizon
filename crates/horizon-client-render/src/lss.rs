//! Client for LOD Server Support (LSS, published as "Voxy Server Side"): far
//! terrain streamed from a Java server that runs it, drawn by Horizon's own far
//! terrain (`lod`) instead of Voxy.
//!
//! Protocol 20 of <https://github.com/VoX/lod-server-support> (MIT): `lss:*`
//! custom payloads in the play phase.
//!
//! - `wire`: payload codecs.
//! - `section`: a column's section bytes (the v20 identity-dictionary layout)
//!   to a `Chunk`, which `lod` summarizes into far-terrain columns.
//! - `session`: the client's want-set: which columns to ask for, with which
//!   timestamps, and what the answers mean.
pub mod section;
pub mod session;
pub mod wire;
