# mcap-videoify

Convert compressed image topics to foxglove.CompressedVideo (H.264).

**Supported input types:**

- **foxglove.CompressedImage** (protobuf)
- **ROS 2** `sensor_msgs/msg/CompressedImage` (CDR), e.g. `/boxi/hdr/front/image_raw/compressed`
- **ROS 2** `sensor_msgs/msg/Image` (CDR), e.g. `/anymal/depth_camera/front_upper/depth/image_rect_raw` — encodings: `rgb8`, `bgr8`, `mono8`, `mono16`, `16UC1`, `rgba8`, `bgra8`

All ROS 2 types are parsed via [rosbags-rs](https://github.com/amin-abouee/rosbags-rs). Each input topic becomes a new `{topic}_video` channel in the output.

## Usage

Default: output contains only the converted video topics (one `*_video` topic per CompressedImage topic). Writes to `compressed_video.mcap` unless `-o` is set.

```bash
cargo run -r -- /path/to/file.mcap
```

With `--preserve-other-topics`: all non-CompressedImage topics are copied to the output unchanged, and CompressedImage topics are converted to CompressedVideo. Message order is preserved. Use `-o` to choose the output file.

```bash
cargo run -r -- --preserve-other-topics -o out.mcap /path/to/file.mcap
```

## Options

- `INPUT` – Input MCAP file path (positional).
- `-o, --output PATH` – Output MCAP file path (default: `compressed_video.mcap`).
- `--preserve-other-topics` – Keep all other topics in the output unchanged; only compressed-image streams are converted to CompressedVideo.
