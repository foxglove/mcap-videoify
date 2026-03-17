use anyhow::{Context, Result};
use camino::Utf8Path;
use clap::Parser;
use image::imageops::FilterType;
use image::io::Reader as ImageReader;
use image::ImageBuffer;
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

    let mut cdr = CdrDeserializer::new(data).map_err(|e| anyhow::anyhow!("{}", e))?;
    Image::from_cdr(&mut cdr).map_err(|e| anyhow::anyhow!("{}", e))
}

/// Parent namespace of a topic (e.g. "/alphasense_front_center/image_raw" -> "alphasense_front_center").
/// Leading slash is stripped so "/ns/img" and "ns/camera_info" both yield "ns".
fn topic_parent_namespace(topic: &str) -> String {
    let p = topic.rsplit_once('/').map(|(parent, _)| parent).unwrap_or("");
    p.strip_prefix('/').unwrap_or(p).to_string()
}

/// Namespace to use when registering scaled image topics for CameraInfo matching.
/// Image topics often look like ".../front_center/image_raw/compressed" or ".../front_center/image_raw";
/// the matching CameraInfo is ".../front_center/camera_info", so we must derive "front_center" namespace
/// by stripping the image suffix, not just the last path segment.
fn image_topic_namespace_for_camera_info(topic: &str) -> String {
    let prefix = topic
        .strip_suffix("/image_raw/compressed")
        .or_else(|| topic.strip_suffix("/image_raw"))
        .or_else(|| topic.strip_suffix("/image_rect_raw"))
        .or_else(|| topic.strip_suffix("/compressed"));
    match prefix {
        Some(s) => s.strip_prefix('/').unwrap_or(s).to_string(),
        None => topic_parent_namespace(topic),
    }
}

/// Parsed ROS 2 sensor_msgs/msg/CameraInfo (fields needed for scaling).
struct Ros2CameraInfo {
    header: rosbags_rs::messages::Header,
    height: u32,
    width: u32,
    distortion_model: String,
    d: Vec<f64>,
    k: [f64; 9],
    r: [f64; 9],
    p: [f64; 12],
    binning_x: u32,
    binning_y: u32,
    roi_x_offset: u32,
    roi_y_offset: u32,
    roi_height: u32,
    roi_width: u32,
    roi_do_rectify: bool,
}

/// Parse ROS 2 sensor_msgs/msg/CameraInfo using CDR.
fn parse_ros2_camera_info(data: &[u8]) -> anyhow::Result<Ros2CameraInfo> {
    use rosbags_rs::cdr::CdrDeserializer;
    use rosbags_rs::messages::{FromCdr, Header};

    let mut cdr = CdrDeserializer::new(data).map_err(|e| anyhow::anyhow!("{}", e))?;
    let header = Header::from_cdr(&mut cdr).map_err(|e| anyhow::anyhow!("{}", e))?;
    let height = cdr.read_u32().map_err(|e| anyhow::anyhow!("{}", e))?;
    let width = cdr.read_u32().map_err(|e| anyhow::anyhow!("{}", e))?;
    let distortion_model = cdr.read_string().map_err(|e| anyhow::anyhow!("{}", e))?;
    let d = cdr
        .read_sequence(|d| d.read_f64())
        .map_err(|e| anyhow::anyhow!("{}", e))?;
    let k = cdr.read_f64_array().map_err(|e| anyhow::anyhow!("{}", e))?;
    let r = cdr.read_f64_array().map_err(|e| anyhow::anyhow!("{}", e))?;
    let p = cdr.read_f64_array().map_err(|e| anyhow::anyhow!("{}", e))?;
    let binning_x = cdr.read_u32().map_err(|e| anyhow::anyhow!("{}", e))?;
    let binning_y = cdr.read_u32().map_err(|e| anyhow::anyhow!("{}", e))?;
    // ROI (and do_rectify): some bags have shorter CameraInfo; only read if 16+ bytes remain
    let (roi_x_offset, roi_y_offset, roi_height, roi_width, roi_do_rectify) =
        if cdr.has_remaining(16) {
            (
                cdr.read_u32().map_err(|e| anyhow::anyhow!("{}", e))?,
                cdr.read_u32().map_err(|e| anyhow::anyhow!("{}", e))?,
                cdr.read_u32().map_err(|e| anyhow::anyhow!("{}", e))?,
                cdr.read_u32().map_err(|e| anyhow::anyhow!("{}", e))?,
                false, // do_rectify not read to support 364-byte messages
            )
        } else {
            (0, 0, 0, 0, false)
        };
    Ok(Ros2CameraInfo {
        header,
        height,
        width,
        distortion_model,
        d,
        k,
        r,
        p,
        binning_x,
        binning_y,
        roi_x_offset,
        roi_y_offset,
        roi_height,
        roi_width,
        roi_do_rectify,
    })
}

/// Scale camera intrinsics and dimensions by factor s.
fn scale_camera_info(ci: &Ros2CameraInfo, s: f64) -> Ros2CameraInfo {
    let (new_width, new_height) = scaled_dims(ci.width, ci.height, s);
    let mut k_new = ci.k;
    k_new[0] = ci.k[0] * s; // fx
    k_new[2] = ci.k[2] * s; // cx
    k_new[4] = ci.k[4] * s; // fy
    k_new[5] = ci.k[5] * s; // cy
    let mut p_new = ci.p;
    p_new[0] = ci.p[0] * s;  // fx'
    p_new[2] = ci.p[2] * s;  // cx'
    p_new[3] = ci.p[3] * s;  // Tx
    p_new[5] = ci.p[5] * s;  // fy'
    p_new[6] = ci.p[6] * s;  // cy'
    p_new[7] = ci.p[7] * s;  // Ty
    Ros2CameraInfo {
        header: ci.header.clone(),
        height: new_height,
        width: new_width,
        distortion_model: ci.distortion_model.clone(),
        d: ci.d.clone(),
        k: k_new,
        r: ci.r,
        p: p_new,
        binning_x: ci.binning_x,
        binning_y: ci.binning_y,
        roi_x_offset: (ci.roi_x_offset as f64 * s).round() as u32,
        roi_y_offset: (ci.roi_y_offset as f64 * s).round() as u32,
        roi_height: (ci.roi_height as f64 * s).round() as u32,
        roi_width: (ci.roi_width as f64 * s).round() as u32,
        roi_do_rectify: ci.roi_do_rectify,
    }
}

/// Minimal CDR (little-endian) serialization for CameraInfo.
/// Kept for potential future option to also emit ROS2 CameraInfo.
#[allow(dead_code)]
fn serialize_ros2_camera_info(ci: &Ros2CameraInfo) -> Vec<u8> {
    let mut out = Vec::new();
    // CDR encapsulation header (little endian)
    out.extend_from_slice(&[0x00, 0x01, 0x00, 0x00]);
    fn align_to(buf: &mut Vec<u8>, alignment: usize) {
        while buf.len() % alignment != 0 {
            buf.push(0);
        }
    }
    fn write_u32(buf: &mut Vec<u8>, v: u32) {
        align_to(buf, 4);
        buf.extend_from_slice(&v.to_le_bytes());
    }
    fn write_i32(buf: &mut Vec<u8>, v: i32) {
        align_to(buf, 4);
        buf.extend_from_slice(&v.to_le_bytes());
    }
    fn write_f64(buf: &mut Vec<u8>, v: f64) {
        align_to(buf, 8);
        buf.extend_from_slice(&v.to_le_bytes());
    }
    fn write_string(buf: &mut Vec<u8>, s: &str) {
        let bytes = s.as_bytes();
        let len = (bytes.len() + 1) as u32; // +1 for null terminator
        write_u32(buf, len);
        buf.extend_from_slice(bytes);
        buf.push(0);
    }
    // Header
    write_i32(&mut out, ci.header.stamp.sec);
    write_u32(&mut out, ci.header.stamp.nanosec);
    write_string(&mut out, &ci.header.frame_id);
    write_u32(&mut out, ci.height);
    write_u32(&mut out, ci.width);
    write_string(&mut out, &ci.distortion_model);
    write_u32(&mut out, ci.d.len() as u32);
    align_to(&mut out, 8);
    for &v in &ci.d {
        write_f64(&mut out, v);
    }
    for &v in &ci.k {
        write_f64(&mut out, v);
    }
    for &v in &ci.r {
        write_f64(&mut out, v);
    }
    for &v in &ci.p {
        write_f64(&mut out, v);
    }
    write_u32(&mut out, ci.binning_x);
    write_u32(&mut out, ci.binning_y);
    write_u32(&mut out, ci.roi_x_offset);
    write_u32(&mut out, ci.roi_y_offset);
    write_u32(&mut out, ci.roi_height);
    write_u32(&mut out, ci.roi_width);
    align_to(&mut out, 1);
    out.push(if ci.roi_do_rectify { 1 } else { 0 });
    out
}

/// Build a Foxglove CameraCalibration message from parsed (and possibly scaled) ROS2 CameraInfo.
fn ros2_camera_info_to_foxglove_calibration(
    ci: &Ros2CameraInfo,
) -> foxglove::CameraCalibration::CameraCalibration {
    let mut msg = foxglove::CameraCalibration::CameraCalibration::new();
    msg.timestamp.mut_or_insert_default().seconds = ci.header.stamp.sec as i64;
    msg.timestamp.mut_or_insert_default().nanos = ci.header.stamp.nanosec as i32;
    msg.frame_id = ci.header.frame_id.clone();
    msg.width = ci.width;
    msg.height = ci.height;
    msg.distortion_model = ci.distortion_model.clone();
    msg.D = ci.d.clone();
    msg.K = ci.k.to_vec();
    msg.R = ci.r.to_vec();
    msg.P = ci.p.to_vec();
    msg
}

/// Convert raw ROS 2 Image buffer to RGB8 (row-major, 3 bytes per pixel) for the encoder.
/// For 16-bit depth (mono16/16uc1), `depth_range_max` is the value that maps to white (255).
/// Use None for default 10000 (depth in mm); set e.g. 65535 for full 16-bit range.
fn ros2_image_to_rgb8(
    data: &[u8],
    width: u32,
    height: u32,
    encoding: &str,
    is_bigendian: u8,
    depth_range_max: Option<u32>,
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
            // 16-bit single channel (grayscale or depth); 16UC1 is common for depth images.
            // Use a fixed range so scaling is consistent across frames (avoids flicker/artifacts).
            // Default 10000 = depth in mm (0–10 m); values above range clamp to white.
            let range_max = depth_range_max.unwrap_or(10_000) as u64;
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
                let v_clamp = (v as u64).min(range_max);
                let g = if range_max == 0 {
                    0u8
                } else {
                    ((v_clamp * 255) / range_max).min(255) as u8
                };
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

    /// For 16-bit depth (mono16/16uc1): value that maps to white (255). Default 10000 (depth in mm).
    /// Use 65535 for full 16-bit range.
    #[arg(long)]
    depth_range_max: Option<u32>,

    /// Scale resolution by this factor (e.g. 0.25 = quarter resolution). Default 1.0 (no scaling).
    #[arg(long, default_value_t = 1.0)]
    scale: f64,

    /// Emit a keyframe every N frames (frame indices 0, N, 2N, …). Omit or 0 = use encoder default.
    #[arg(long)]
    keyframe_period: Option<u32>,
}

/// Compute scaled width/height (even dimensions for H.264). Returns (new_w, new_h).
fn scaled_dims(w: u32, h: u32, scale: f64) -> (u32, u32) {
    if scale <= 0.0 || (scale - 1.0).abs() < 1e-9 {
        return (w, h);
    }
    let new_w = (w as f64 * scale).round() as u32;
    let new_h = (h as f64 * scale).round() as u32;
    // H.264 requires even dimensions
    ((new_w / 2) * 2, (new_h / 2) * 2)
}

fn map_mcap<P: AsRef<Utf8Path>>(p: P) -> Result<Mmap> {
    let fd = fs::File::open(p.as_ref()).context("Couldn't open MCAP file")?;
    unsafe { Mmap::map(&fd) }.context("Couldn't map MCAP file")
}

/// If keyframe_period is set and > 0 and frame_count % period == 0, forces the next encoded
/// frame to be an IDR (keyframe). Safe: we only call ForceIntraFrame, the intended API for this.
fn maybe_force_keyframe(encoder: &mut Encoder, keyframe_period: Option<u32>, frame_count: u64) {
    if let Some(period) = keyframe_period {
        if period > 0 && frame_count % (period as u64) == 0 {
            unsafe {
                encoder.raw_api().force_intra_frame(true);
            }
        }
    }
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
        foxglove::CameraCalibration::file_descriptor().proto().clone(),
    );
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

    let camera_calibration_schema = mcap::Schema {
        name: "foxglove.CameraCalibration".to_string(),
        encoding: "protobuf".to_string(),
        data: cow.clone(),
    };

    // Map of topic -> channel for video topics
    let mut topic_channels: HashMap<String, mcap::Channel> = HashMap::new();
    // Map of topic -> channel for Foxglove CameraCalibration (when preserve_other_topics and we see CameraInfo)
    let mut camera_calibration_channels: HashMap<String, mcap::Channel> = HashMap::new();
    // Map of channel key -> channel for passthrough (when preserve_other_topics)
    let mut passthrough_channels: HashMap<String, mcap::Channel> = HashMap::new();

    let mut encoders_by_topic: HashMap<String, Encoder> = HashMap::new();
    let mut frame_count_by_topic: HashMap<String, u64> = HashMap::new();
    // When scaling images, we rewrite CameraInfo in the same namespace. Key = topic parent path.
    let mut scaled_namespace_scale: HashMap<String, f64> = HashMap::new();

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
        let is_ros2_camera_info = schema.name.contains("sensor_msgs")
            && schema.name.contains("CameraInfo")
            && full_message.channel.message_encoding == "cdr";

        if !is_foxglove_compressed_image && !is_ros2_compressed_image && !is_ros2_image {
            if cli.preserve_other_topics {
                if is_ros2_camera_info {
                    // Convert to Foxglove CameraCalibration only; do not passthrough ROS2 CameraInfo
                    // so the topic has a single schema (Foxglove can't show multiple schemas per topic).
                    let ns = topic_parent_namespace(&full_message.channel.topic);
                    match parse_ros2_camera_info(&full_message.data) {
                        Ok(ci) => {
                            let ci = if let Some(&scale) = scaled_namespace_scale.get(&ns) {
                                let scaled = scale_camera_info(&ci, scale);
                                eprintln!(
                                    "[videoify] CameraInfo scaled: topic={} {}x{} -> {}x{}",
                                    full_message.channel.topic,
                                    ci.width,
                                    ci.height,
                                    scaled.width,
                                    scaled.height
                                );
                                scaled
                            } else {
                                ci
                            };
                            let cal_topic = full_message.channel.topic.clone();
                            let cal_channel = camera_calibration_channels
                                .entry(cal_topic.clone())
                                .or_insert_with_key(|key| {
                                    let new_channel = mcap::Channel {
                                        schema: Some(Arc::new(camera_calibration_schema.clone())),
                                        topic: key.to_string(),
                                        message_encoding: "protobuf".to_string(),
                                        metadata: std::collections::BTreeMap::new(),
                                    };
                                    out_mcap
                                        .add_channel(&new_channel)
                                        .expect("Couldn't write CameraCalibration channel");
                                    new_channel
                                });
                            let cal_msg = ros2_camera_info_to_foxglove_calibration(&ci);
                            let cal_bytes = cal_msg.write_to_bytes().unwrap();
                            let cal_message = mcap::Message {
                                channel: Arc::new(cal_channel.clone()),
                                data: Cow::from(cal_bytes),
                                log_time: full_message.log_time,
                                publish_time: full_message.publish_time,
                                sequence: full_message.sequence,
                            };
                            out_mcap.write(&cal_message).unwrap();
                        }
                        Err(e) => {
                            eprintln!("[videoify] CameraInfo parse error (skipping): {}", e);
                        }
                    }
                } else {
                    // Non-camera_info: passthrough with original schema
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
            let (enc_width, enc_height) =
                scaled_dims(rgb8.width(), rgb8.height(), cli.scale);
            let rgb8_final: ImageBuffer<image::Rgb<u8>, Vec<u8>> =
                if (cli.scale - 1.0).abs() > 1e-9 {
                    image::imageops::resize(
                        &rgb8,
                        enc_width,
                        enc_height,
                        FilterType::Triangle,
                    )
                } else {
                    rgb8.clone()
                };
            let width = enc_width as usize;
            let height = enc_height as usize;
            let topic = full_message.channel.topic.clone();
            if (cli.scale - 1.0).abs() > 1e-9 {
                scaled_namespace_scale.insert(image_topic_namespace_for_camera_info(&topic), cli.scale);
            }
            let encoder = encoders_by_topic.entry(topic.clone()).or_insert_with(|| {
                let config =
                    EncoderConfig::new(enc_width, enc_height).set_bitrate_bps(10_000_000);
                Encoder::with_config(config).unwrap()
            });
            let frame_count = *frame_count_by_topic.get(&topic).unwrap_or(&0);
            maybe_force_keyframe(encoder, cli.keyframe_period, frame_count);
            let yuv = YUVBuffer::with_rgb(width, height, rgb8_final.as_raw());
            let bitstream = encoder.encode(&yuv).unwrap();
            frame_count_by_topic
                .entry(topic.clone())
                .and_modify(|c| *c += 1)
                .or_insert(1);
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
                cli.depth_range_max,
            ) {
                Ok(b) => b,
                Err(e) => {
                    eprintln!("Skip ROS 2 Image (unsupported encoding): {}", e);
                    continue;
                }
            };
            let (enc_width, enc_height) =
                scaled_dims(img_msg.width, img_msg.height, cli.scale);
            let rgb_img = ImageBuffer::<image::Rgb<u8>, Vec<u8>>::from_raw(
                img_msg.width,
                img_msg.height,
                rgb8_bytes,
            )
            .context("ROS 2 Image: invalid dimensions for buffer")?;
            let rgb8_final: ImageBuffer<image::Rgb<u8>, Vec<u8>> =
                if (cli.scale - 1.0).abs() > 1e-9 {
                    image::imageops::resize(
                        &rgb_img,
                        enc_width,
                        enc_height,
                        FilterType::Triangle,
                    )
                } else {
                    rgb_img
                };
            let width = enc_width as usize;
            let height = enc_height as usize;
            let topic = full_message.channel.topic.clone();
            if (cli.scale - 1.0).abs() > 1e-9 {
                scaled_namespace_scale.insert(image_topic_namespace_for_camera_info(&topic), cli.scale);
            }
            let encoder = encoders_by_topic.entry(topic.clone()).or_insert_with(|| {
                let config =
                    EncoderConfig::new(enc_width, enc_height).set_bitrate_bps(10_000_000);
                Encoder::with_config(config).unwrap()
            });
            let frame_count = *frame_count_by_topic.get(&topic).unwrap_or(&0);
            maybe_force_keyframe(encoder, cli.keyframe_period, frame_count);
            let yuv = YUVBuffer::with_rgb(width, height, rgb8_final.as_raw());
            let bitstream = encoder.encode(&yuv).unwrap();
            frame_count_by_topic
                .entry(topic.clone())
                .and_modify(|c| *c += 1)
                .or_insert(1);
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

        let rgb8 = img.to_rgb8();
        let (enc_width, enc_height) = scaled_dims(rgb8.width(), rgb8.height(), cli.scale);
        let rgb8_final: ImageBuffer<image::Rgb<u8>, Vec<u8>> =
            if (cli.scale - 1.0).abs() > 1e-9 {
                image::imageops::resize(
                    &rgb8,
                    enc_width,
                    enc_height,
                    FilterType::Triangle,
                )
            } else {
                rgb8.clone()
            };
        let width = enc_width as usize;
        let height = enc_height as usize;

        let topic = full_message.channel.topic.clone();
        if (cli.scale - 1.0).abs() > 1e-9 {
            scaled_namespace_scale.insert(image_topic_namespace_for_camera_info(&topic), cli.scale);
        }
        let encoder = encoders_by_topic.entry(topic.clone()).or_insert_with(|| {
            let config =
                EncoderConfig::new(enc_width, enc_height).set_bitrate_bps(10_000_000);
            Encoder::with_config(config).unwrap()
        });
        let frame_count = *frame_count_by_topic.get(&topic).unwrap_or(&0);
        maybe_force_keyframe(encoder, cli.keyframe_period, frame_count);
        let yuv = YUVBuffer::with_rgb(width, height, rgb8_final.as_raw());
        let bitstream = encoder.encode(&yuv).unwrap();
        frame_count_by_topic
            .entry(topic.clone())
            .and_modify(|c| *c += 1)
            .or_insert(1);

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
