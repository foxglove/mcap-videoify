use anyhow::{Context, Result};
use camino::Utf8Path;
use clap::Parser;
use image::io::Reader as ImageReader;
use memmap::Mmap;
use openh264::encoder::{Encoder, EncoderConfig};
use openh264::formats::YUVBuffer;
use protobuf::descriptor::FileDescriptorSet;
use protobuf::reflect::FileDescriptor;
use protobuf::Message;
use std::borrow::Cow;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufWriter;
use std::io::Cursor;
use std::sync::Arc;
use std::path::PathBuf;
use std::fs;

mod foxglove {
    include!(concat!(env!("OUT_DIR"), "/generated_protos/mod.rs"));
}

/// Parse ROS 2 sensor_msgs/msg/CompressedImage using rosbags-rs CDR deserialization.
fn parse_ros2_compressed_image(
    data: &[u8],
) -> anyhow::Result<(i32, u32, String, String, Vec<u8>)> {
    use rosbags_rs::cdr::CdrDeserializer;
    use rosbags_rs::messages::{FromCdr, Header};

    let mut cdr = CdrDeserializer::new(data)?;
    let header = Header::from_cdr(&mut cdr)?;
    let format = cdr.read_string()?;
    let image_data = cdr.read_byte_sequence()?;
    Ok((
        header.stamp.sec,
        header.stamp.nanosec,
        header.frame_id,
        format,
        image_data,
    ))
}

/// Parse ROS 2 sensor_msgs/msg/Image using rosbags-rs CDR deserialization.
fn parse_ros2_image(data: &[u8]) -> anyhow::Result<rosbags_rs::messages::Image> {
    use rosbags_rs::cdr::CdrDeserializer;
    use rosbags_rs::messages::{FromCdr, Image};

    let mut cdr = CdrDeserializer::new(data)?;
    Image::from_cdr(&mut cdr).map_err(Into::into)
}

/// Convert raw ROS 2 Image buffer to RGB8 (row-major, 3 bytes per pixel) for the encoder.
fn ros2_image_to_rgb8(
    data: &[u8],
    width: u32,
    height: u32,
    encoding: &str,
    is_bigendian: u8,
) -> anyhow::Result<Vec<u8>> {
    let w = width as usize;
    let h = height as usize;
    let n = w * h * 3;
    let mut rgb = vec![0u8; n];
    let enc = encoding.to_lowercase();

    match enc.as_str() {
        "rgb8" => {
            if data.len() < n {
                anyhow::bail!("rgb8 data len {} < {}", data.len(), n);
            }
            rgb.copy_from_slice(&data[..n]);
        }
        "bgr8" => {
            if data.len() < n {
                anyhow::bail!("bgr8 data len {} < {}", data.len(), n);
            }
            for (i, chunk) in data.chunks_exact(3).take(w * h).enumerate() {
                rgb[i * 3] = chunk[2];
                rgb[i * 3 + 1] = chunk[1];
                rgb[i * 3 + 2] = chunk[0];
            }
        }
        "mono8" => {
            if data.len() < w * h {
                anyhow::bail!("mono8 data len {} < {}", data.len(), w * h);
            }
            for (i, &g) in data.iter().take(w * h).enumerate() {
                rgb[i * 3] = g;
                rgb[i * 3 + 1] = g;
                rgb[i * 3 + 2] = g;
            }
        }
        "rgba8" => {
            if data.len() < w * h * 4 {
                anyhow::bail!("rgba8 data len {} < {}", data.len(), w * h * 4);
            }
            for (i, chunk) in data.chunks_exact(4).take(w * h).enumerate() {
                rgb[i * 3] = chunk[0];
                rgb[i * 3 + 1] = chunk[1];
                rgb[i * 3 + 2] = chunk[2];
            }
        }
        "bgra8" => {
            if data.len() < w * h * 4 {
                anyhow::bail!("bgra8 data len {} < {}", data.len(), w * h * 4);
            }
            for (i, chunk) in data.chunks_exact(4).take(w * h).enumerate() {
                rgb[i * 3] = chunk[2];
                rgb[i * 3 + 1] = chunk[1];
                rgb[i * 3 + 2] = chunk[0];
            }
        }
        "mono16" | "16uc1" => {
            // 16-bit single channel (grayscale or depth); 16UC1 is common for depth images
            let len = w * h * 2;
            if data.len() < len {
                anyhow::bail!("16-bit image data len {} < {}", data.len(), len);
            }
            for (i, chunk) in data.chunks_exact(2).take(w * h).enumerate() {
                let lo = chunk[0] as u16;
                let hi = chunk[1] as u16;
                let v = if is_bigendian != 0 {
                    (hi << 8) | lo
                } else {
                    (lo << 8) | hi
                };
                let g = (v >> 8) as u8;
                rgb[i * 3] = g;
                rgb[i * 3 + 1] = g;
                rgb[i * 3 + 2] = g;
            }
        }
        _ => anyhow::bail!("unsupported ROS 2 Image encoding: {}", encoding),
    }
    Ok(rgb)
}


#[derive(Parser, Debug)]
#[command(name = "mcap-videoify")]
#[command(about = "Convert foxglove.CompressedImage messages to foxglove.CompressedVideo")]
struct Cli {
    /// Input MCAP file path
    input: PathBuf,

    /// Output MCAP file path (default: compressed_video.mcap)
    #[arg(short, long)]
    output: Option<PathBuf>,

    /// Keep all non-CompressedImage topics in the output unchanged (preserves message order)
    #[arg(long)]
    preserve_other_topics: bool,
}

fn map_mcap<P: AsRef<Utf8Path>>(p: P) -> Result<Mmap> {
    let fd = fs::File::open(p.as_ref()).context("Couldn't open MCAP file")?;
    unsafe { Mmap::map(&fd) }.context("Couldn't map MCAP file")
}

fn run(cli: &Cli) -> Result<()> {
    let input_path = Utf8Path::from_path(cli.input.as_path())
        .context("Input path must be valid UTF-8")?;
    let mapped = map_mcap(input_path)?;

    let output_path = cli
        .output
        .as_deref()
        .unwrap_or(std::path::Path::new("compressed_video.mcap"));
    let output_path_utf8 = Utf8Path::from_path(output_path)
        .context("Output path must be valid UTF-8")?;

    let mut set = FileDescriptorSet::new();
    set.file
        .push(foxglove::CompressedVideo::file_descriptor().proto().clone());
    set.file.push(
        ::protobuf::well_known_types::timestamp::file_descriptor()
            .proto()
            .clone(),
    );

    let cow = Cow::from(set.write_to_bytes().unwrap());

    let compressed_video_schema = mcap::Schema {
        name: "foxglove.CompressedVideo".to_string(),
        encoding: "protobuf".to_string(),
        data: cow.clone(),
    };

    // Map of topic -> channel for video topics
    let mut topic_channels: HashMap<String, mcap::Channel> = HashMap::new();
    // Map of channel key -> channel for passthrough (when preserve_other_topics)
    let mut passthrough_channels: HashMap<String, mcap::Channel> = HashMap::new();

    let mut encoders_by_topic: HashMap<String, Encoder> = HashMap::new();

    let mut out_mcap = mcap::Writer::new(BufWriter::new(
        File::create(output_path_utf8.as_std_path()).context("Couldn't create output MCAP file")?,
    ))
    .context("Couldn't create MCAP writer")?;

    for message in mcap::MessageStream::new(&mapped)? {
        let full_message = message.unwrap();

        let schema = match full_message.channel.schema.as_ref() {
            Some(s) => s.clone(),
            None => {
                // Channels without schema are not preserved (Writer semantics / edge case)
                continue;
            }
        };

        let is_foxglove_compressed_image =
            schema.name == "foxglove.CompressedImage" && schema.encoding == "protobuf";
        let is_ros2_compressed_image = (schema.name.contains("sensor_msgs")
            && schema.name.contains("CompressedImage"))
            && full_message.channel.message_encoding == "cdr";
        let is_ros2_image = schema.name.contains("sensor_msgs")
            && schema.name.contains("Image")
            && !schema.name.contains("CompressedImage")
            && full_message.channel.message_encoding == "cdr";

        if !is_foxglove_compressed_image && !is_ros2_compressed_image && !is_ros2_image {
            if cli.preserve_other_topics {
                let channel_key = format!(
                    "{}|{}",
                    full_message.channel.topic,
                    full_message.channel.message_encoding
                );
                let channel = passthrough_channels
                    .entry(channel_key.clone())
                    .or_insert_with_key(|_| {
                        let ch = (*full_message.channel).clone();
                        out_mcap.add_channel(&ch).expect("Couldn't write channel");
                        ch
                    });
                let out_message = mcap::Message {
                    channel: Arc::new(channel.clone()),
                    data: full_message.data.clone(),
                    log_time: full_message.log_time,
                    publish_time: full_message.publish_time,
                    sequence: full_message.sequence,
                };
                out_mcap.write(&out_message).unwrap();
            }
            continue;
        }

        if is_ros2_compressed_image {
            let (stamp_sec, stamp_nanosec, frame_id, _format, image_data) =
                match parse_ros2_compressed_image(&full_message.data) {
                    Ok(t) => t,
                    Err(e) => {
                        eprintln!("Skip ROS 2 CompressedImage (parse error): {}", e);
                        continue;
                    }
                };
            let reader = ImageReader::new(Cursor::new(&image_data))
                .with_guessed_format()
                .context("ROS 2 CompressedImage: guess format")?;
            let img = reader.decode().context("ROS 2 CompressedImage: decode image")?;
            let rgb8 = img.to_rgb8();
            let width = usize::try_from(rgb8.width()).unwrap();
            let height = usize::try_from(rgb8.height()).unwrap();
            let topic = format!("{}_video", full_message.channel.topic);
            let encoder = encoders_by_topic.entry(topic.clone()).or_insert_with(|| {
                let config =
                    EncoderConfig::new(rgb8.width(), rgb8.height()).set_bitrate_bps(10_000_000);
                Encoder::with_config(config).unwrap()
            });
            let yuv = YUVBuffer::with_rgb(width, height, &rgb8);
            let bitstream = encoder.encode(&yuv).unwrap();
            let mut out_msg = foxglove::CompressedVideo::CompressedVideo::new();
            out_msg.timestamp.mut_or_insert_default().seconds = stamp_sec as i64;
            out_msg.timestamp.mut_or_insert_default().nanos = stamp_nanosec as i32;
            out_msg.frame_id = frame_id;
            out_msg.format = "h264".to_string();
            out_msg.data = bitstream.to_vec();
            let out_bytes: Vec<u8> = out_msg.write_to_bytes().unwrap();
            let channel = topic_channels.entry(topic.clone()).or_insert_with_key(|key| {
                let new_channel = mcap::Channel {
                    schema: Some(Arc::new(compressed_video_schema.to_owned())),
                    topic: key.to_string(),
                    message_encoding: "protobuf".to_string(),
                    metadata: std::collections::BTreeMap::new(),
                };
                out_mcap
                    .add_channel(&new_channel)
                    .expect("Couldn't write channel");
                new_channel
            });
            let message = mcap::Message {
                channel: Arc::new(channel.to_owned()),
                data: Cow::from(out_bytes.clone()),
                log_time: full_message.log_time,
                publish_time: full_message.publish_time,
                sequence: full_message.sequence,
            };
            if !out_msg.data.is_empty() {
                out_mcap.write(&message).unwrap();
            }
            continue;
        }

        if is_ros2_image {
            let img_msg = match parse_ros2_image(&full_message.data) {
                Ok(m) => m,
                Err(e) => {
                    eprintln!("Skip ROS 2 Image (parse error): {}", e);
                    continue;
                }
            };
            let rgb8_bytes = match ros2_image_to_rgb8(
                &img_msg.data,
                img_msg.width,
                img_msg.height,
                &img_msg.encoding,
                img_msg.is_bigendian,
            ) {
                Ok(b) => b,
                Err(e) => {
                    eprintln!("Skip ROS 2 Image (unsupported encoding): {}", e);
                    continue;
                }
            };
            let width = img_msg.width as usize;
            let height = img_msg.height as usize;
            let topic = format!("{}_video", full_message.channel.topic);
            let encoder = encoders_by_topic.entry(topic.clone()).or_insert_with(|| {
                let config =
                    EncoderConfig::new(img_msg.width, img_msg.height).set_bitrate_bps(10_000_000);
                Encoder::with_config(config).unwrap()
            });
            let yuv = YUVBuffer::with_rgb(width, height, &rgb8_bytes);
            let bitstream = encoder.encode(&yuv).unwrap();
            let mut out_msg = foxglove::CompressedVideo::CompressedVideo::new();
            out_msg.timestamp.mut_or_insert_default().seconds = img_msg.header.stamp.sec as i64;
            out_msg.timestamp.mut_or_insert_default().nanos = img_msg.header.stamp.nanosec as i32;
            out_msg.frame_id = img_msg.header.frame_id.clone();
            out_msg.format = "h264".to_string();
            out_msg.data = bitstream.to_vec();
            let out_bytes: Vec<u8> = out_msg.write_to_bytes().unwrap();
            let channel = topic_channels.entry(topic.clone()).or_insert_with_key(|key| {
                let new_channel = mcap::Channel {
                    schema: Some(Arc::new(compressed_video_schema.to_owned())),
                    topic: key.to_string(),
                    message_encoding: "protobuf".to_string(),
                    metadata: std::collections::BTreeMap::new(),
                };
                out_mcap
                    .add_channel(&new_channel)
                    .expect("Couldn't write channel");
                new_channel
            });
            let message = mcap::Message {
                channel: Arc::new(channel.to_owned()),
                data: Cow::from(out_bytes.clone()),
                log_time: full_message.log_time,
                publish_time: full_message.publish_time,
                sequence: full_message.sequence,
            };
            if !out_msg.data.is_empty() {
                out_mcap.write(&message).unwrap();
            }
            continue;
        }

        let set_proto = FileDescriptorSet::parse_from_bytes(&schema.data)?;
        let descriptors = FileDescriptor::new_dynamic_fds(set_proto.file, &[]).unwrap();

        // fixme - why index 1?
        let msg = descriptors[1]
            .message_by_full_name(".foxglove.CompressedImage")
            .unwrap();

        let parsed = msg.parse_from_bytes(&full_message.data)?;

        println!("{:?}", msg);
        let timestamp = msg
            .field_by_name("timestamp")
            .unwrap()
            .get_singular_field_or_default(parsed.as_ref());

        let frame_id = msg
            .field_by_name("frame_id")
            .unwrap()
            .get_singular_field_or_default(parsed.as_ref());

        let data = msg
            .field_by_name("data")
            .unwrap()
            .get_singular_field_or_default(parsed.as_ref());

        let reader = ImageReader::new(Cursor::new(data.to_bytes().unwrap()))
            .with_guessed_format()
            .expect("Cursor io never fails");

        let img = reader.decode()?;

        let rgb8 = &img.to_rgb8();

        let width = usize::try_from(rgb8.width()).unwrap();
        let height = usize::try_from(rgb8.height()).unwrap();

        let topic = std::format!("{topic}_video", topic = full_message.channel.topic);

        let encoder = encoders_by_topic.entry(topic.clone()).or_insert_with(||{
            // fixme - command line argument for bitrate
            let config =
                EncoderConfig::new(rgb8.width(), rgb8.height()).set_bitrate_bps(10_000_000);
            return Encoder::with_config(config).unwrap();
        });
         
        let yuv = YUVBuffer::with_rgb(width, height, &rgb8);
        let bitstream = encoder.encode(&yuv).unwrap();

        let mut out_msg = foxglove::CompressedVideo::CompressedVideo::new();

        let bytes = timestamp
            .to_message()
            .unwrap()
            .write_to_bytes_dyn()
            .unwrap();
        let time =
            protobuf::well_known_types::timestamp::Timestamp::parse_from_bytes(bytes.as_slice())
                .unwrap();
        out_msg.timestamp.mut_or_insert_default().seconds = time.seconds;
        out_msg.timestamp.mut_or_insert_default().nanos = time.nanos;

        out_msg.frame_id = frame_id.to_string();
        out_msg.format = "h264".to_string();
        out_msg.data = bitstream.to_vec();

        let out_bytes: Vec<u8> = out_msg.write_to_bytes().unwrap();

        let channel = topic_channels.entry(topic.clone()).or_insert_with_key(|key| {
            let new_channel = mcap::Channel {
                schema: Some(Arc::new(compressed_video_schema.to_owned())),
                topic: key.to_string(),
                message_encoding: "protobuf".to_string(),
                metadata: std::collections::BTreeMap::new(),
            };

            out_mcap
                .add_channel(&new_channel)
                .expect("Couldn't write channel");

            return new_channel;
        });

        let message = mcap::Message {
            channel: Arc::new(channel.to_owned()),
            data: Cow::from(out_bytes),
            log_time: full_message.log_time,
            publish_time: full_message.publish_time,
            sequence: full_message.sequence,
        };

        // fixme - why would out_bytes be 0? if the frame did not change?
        if out_msg.data.len() > 0 {
            out_mcap.write(&message).unwrap();
        }
    }

    out_mcap.finish().unwrap();
    Ok(())
}

fn main() {
    let cli = Cli::parse();
    run(&cli).unwrap();
}
