use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Largest payload one frame may carry. A header past this is a protocol
/// violation, not a bigger buffer.
pub const MAX_FRAME_LEN: usize = 64 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum FramingError {
    #[error("frame of {0} bytes exceeds the {MAX_FRAME_LEN} byte limit")]
    TooLarge(u64),
    #[error("peer closed the stream before any frame")]
    Closed,
    #[error("peer closed the stream in the middle of a frame")]
    Truncated,
    #[error(transparent)]
    Io(std::io::Error),
    #[error("frame payload is not the expected JSON: {0}")]
    Json(#[from] serde_json::Error),
}

impl From<std::io::Error> for FramingError {
    fn from(err: std::io::Error) -> Self {
        if err.kind() == std::io::ErrorKind::UnexpectedEof {
            Self::Truncated
        } else {
            Self::Io(err)
        }
    }
}

/// Reads one `u32` little-endian length-prefixed frame.
pub async fn read_frame<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Vec<u8>, FramingError> {
    let mut prefix = [0u8; 4];
    let mut filled = 0;
    while filled < prefix.len() {
        let n = reader.read(&mut prefix[filled..]).await?;
        if n == 0 {
            return Err(if filled == 0 {
                FramingError::Closed
            } else {
                FramingError::Truncated
            });
        }
        filled += n;
    }
    let len = u32::from_le_bytes(prefix);
    if len as usize > MAX_FRAME_LEN {
        return Err(FramingError::TooLarge(u64::from(len)));
    }
    let mut payload = vec![0u8; len as usize];
    reader.read_exact(&mut payload).await?;
    Ok(payload)
}

pub async fn write_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    payload: &[u8],
) -> Result<(), FramingError> {
    if payload.len() > MAX_FRAME_LEN {
        return Err(FramingError::TooLarge(payload.len() as u64));
    }
    let len = u32::try_from(payload.len()).expect("bounded by MAX_FRAME_LEN");
    writer.write_all(&len.to_le_bytes()).await?;
    writer.write_all(payload).await?;
    writer.flush().await?;
    Ok(())
}

pub async fn read_json<T: DeserializeOwned, R: AsyncRead + Unpin>(
    reader: &mut R,
) -> Result<T, FramingError> {
    let payload = read_frame(reader).await?;
    Ok(serde_json::from_slice(&payload)?)
}

pub async fn write_json<T: Serialize, W: AsyncWrite + Unpin>(
    writer: &mut W,
    value: &T,
) -> Result<(), FramingError> {
    let payload = serde_json::to_vec(value)?;
    write_frame(writer, &payload).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn frames_roundtrip_back_to_back() {
        let (mut client, mut server) = tokio::io::duplex(1024);
        write_frame(&mut client, b"first").await.unwrap();
        write_frame(&mut client, b"").await.unwrap();
        write_frame(&mut client, b"third").await.unwrap();

        assert_eq!(read_frame(&mut server).await.unwrap(), b"first");
        assert_eq!(read_frame(&mut server).await.unwrap(), b"");
        assert_eq!(read_frame(&mut server).await.unwrap(), b"third");
    }

    #[tokio::test]
    async fn a_length_prefix_over_the_limit_is_refused_before_reading_the_payload() {
        let (mut client, mut server) = tokio::io::duplex(1024);
        let too_big = (MAX_FRAME_LEN as u32 + 1).to_le_bytes();
        client.write_all(&too_big).await.unwrap();

        assert!(matches!(
            read_frame(&mut server).await,
            Err(FramingError::TooLarge(n)) if n == MAX_FRAME_LEN as u64 + 1
        ));
    }

    #[tokio::test]
    async fn writing_a_payload_over_the_limit_is_refused() {
        let (mut client, _server) = tokio::io::duplex(1024);
        let payload = vec![0u8; MAX_FRAME_LEN + 1];
        assert!(matches!(
            write_frame(&mut client, &payload).await,
            Err(FramingError::TooLarge(_))
        ));
    }

    #[tokio::test]
    async fn a_stream_closed_mid_frame_is_truncated_not_closed() {
        let (mut client, mut server) = tokio::io::duplex(1024);
        client.write_all(&8u32.to_le_bytes()).await.unwrap();
        client.write_all(b"abc").await.unwrap();
        drop(client);
        assert!(matches!(
            read_frame(&mut server).await,
            Err(FramingError::Truncated)
        ));

        let (mut client, mut server) = tokio::io::duplex(1024);
        client.write_all(&[1, 0]).await.unwrap();
        drop(client);
        assert!(matches!(
            read_frame(&mut server).await,
            Err(FramingError::Truncated)
        ));
    }

    #[tokio::test]
    async fn a_stream_closed_before_any_byte_is_closed() {
        let (client, mut server) = tokio::io::duplex(1024);
        drop(client);
        assert!(matches!(
            read_frame(&mut server).await,
            Err(FramingError::Closed)
        ));
    }

    #[tokio::test]
    async fn json_helpers_roundtrip_a_value() {
        let (mut client, mut server) = tokio::io::duplex(1024);
        write_json(&mut client, &serde_json::json!({ "accepted": true }))
            .await
            .unwrap();
        let value: serde_json::Value = read_json(&mut server).await.unwrap();
        assert_eq!(value, serde_json::json!({ "accepted": true }));
    }
}
