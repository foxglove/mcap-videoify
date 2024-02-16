use anyhow::{Context, Result};
use camino::Utf8Path;
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
use std::{env, fs};

mod foxglove {
    include!(concat!(env!("OUT_DIR"), "/generated_protos/mod.rs"));
}

fn map_mcap<P: AsRef<Utf8Path>>(p: P) -> Result<Mmap> {
    let fd = fs::File::open(p.as_ref()).context("Couldn't open MCAP file")?;
    unsafe { Mmap::map(&fd) }.context("Couldn't map MCAP file")
}

fn read_it() -> Result<()> {
    let args: Vec<String> = env::args().collect();

    let mapped = map_mcap(&args[1])?;

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

    // Map of topic -> channel for the topic
    let mut topic_channels: HashMap<String, mcap::Channel> = HashMap::new();

    let mut encoders_by_topic: HashMap<String, Encoder> = HashMap::new();

    let mut video_mcap = mcap::Writer::new(BufWriter::new(
        File::create("compressed_video.mcap").unwrap(),
    ))
    .unwrap();

    for message in mcap::MessageStream::new(&mapped)? {
        let full_message = message.unwrap();
        let schema = full_message.channel.schema.as_ref().unwrap().clone();

        if schema.name.ne("foxglove.CompressedImage") || schema.encoding.ne("protobuf") {
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

            video_mcap
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
            video_mcap.write(&message).unwrap();
        }
    }

    video_mcap.finish().unwrap();
    Ok(())
}

fn main() {
    read_it().unwrap();
}
