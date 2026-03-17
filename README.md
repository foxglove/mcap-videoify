# mcap-videoify

Convert compressed image topics to foxglove.CompressedVideo (H.264).

**Supported input types:**

- **foxglove.CompressedImage** (protobuf)
- **ROS 2** `sensor_msgs/msg/CompressedImage` (CDR), e.g. `/boxi/hdr/front/image_raw/compressed`
- **ROS 2** `sensor_msgs/msg/Image` (CDR), e.g. `/anymal/depth_camera/front_upper/depth/image_rect_raw` — encodings: `rgb8`, `bgr8`, `mono8`, `mono16`, `16UC1`, `rgba8`, `bgra8`

All ROS 2 types are parsed via [rosbags-rs](https://github.com/amin-abouee/rosbags-rs). Converted video is written to the **same topic name** as the input image topic.

## Usage

Default: output contains only the converted video topics (one output topic per input image topic, same name). Writes to `compressed_video.mcap` unless `-o` is set.

```bash
cargo run -r -- /path/to/file.mcap
```

With `--preserve-other-topics`: all non-CompressedImage topics are copied to the output unchanged, and CompressedImage topics are converted to CompressedVideo. Message order is preserved. Use `-o` to choose the output file.

```bash
cargo run -r -- --preserve-other-topics -o out.mcap /path/to/file.mcap
```

With `--scale 0.25`: downscale images to quarter resolution before encoding (e.g. 1920×1080 → 480×270). Use with `--preserve-other-topics` so that matching CameraInfo topics are updated to the scaled resolution.

```bash
cargo run -r -- --preserve-other-topics --scale 0.25 -o out.mcap /path/to/file.mcap
```

## Options

- `INPUT` – Input MCAP file path (positional).
- `-o, --output PATH` – Output MCAP file path (default: `compressed_video.mcap`).
- `--preserve-other-topics` – Keep all other topics in the output unchanged; only compressed-image streams are converted to CompressedVideo.
- `--scale FACTOR` – Scale resolution by this factor (e.g. `0.25` = quarter resolution, 4× downscale). Default 1.0 (no scaling). Output dimensions are rounded to even values for H.264.
- `--keyframe-period N` – Emit a keyframe every N frames (frame indices 0, N, 2N, …). Omit or 0 = use encoder default. Useful for seeking and smaller random-access cost.
- `--depth-range-max N` – For 16-bit depth (mono16/16uc1), the raw value that maps to white (255). Default 10000 (depth in mm, 0–10 m). Use 65535 for full 16-bit range. Reduces flicker/artifacts by keeping scaling consistent across frames.

When `--scale` is used with `--preserve-other-topics`, any ROS 2 `sensor_msgs/msg/CameraInfo` topic in the same namespace as a scaled image topic (e.g. `/camera/camera_info` for `/camera/image_raw`) is rewritten so that `width`, `height`, and the camera matrices (K, P) and ROI reflect the scaled resolution.
