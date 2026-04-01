# Video Build Report: volva_demo_v2.mp4

## Task
Fix `build_final_video.py` ASS path quoting issues and generate `volva_demo_v2.mp4`.

## Bugs Fixed

### 1. Literal `\n` instead of actual newlines (Line 58)
**Root Cause**: `f.write("\\n".join(lines))` wrote literal backslash-n characters instead of newline characters, corrupting ASS file format.

**Fix**: Changed to `f.write("\n".join(lines))`

### 2. Excessive escaping in dialogue text (Line 55)
**Root Cause**: `line.replace("\\\\", "\\\\\\\\").replace("{", "\\\\{").replace("}", "\\\\}")` was over-aggressive.

**Fix**: Simplified to `line.replace("{", "\\{").replace("}", "\\}")` (only brace escaping needed for ASS)

### 3. Unquoted ASS path in FFmpeg commands (Lines 73, 122)
**Root Cause**: `-vf ass={ass_file}` and `-vf ass=title.ass` lacked quotes around the path.

**Fix**: Changed to `-vf ass="{ass_file}"` and `-vf ass="title.ass"`

### 4. Format string KeyError in title segment (Line 135)
**Root Cause**: `.format(dur=dur)` conflicted with `{int(dur):02d}` curly braces in the ASS template string.

**Fix**: Used simpler placeholders `{{dur_sec}}` and `{{dur_cs}}` with computed values.

### 5. Literal `\n` in segments.txt (Lines 245-249)
**Root Cause**: `f.write("file 'seg1_title.mp4'\\n")` wrote literal `\n` instead of actual newlines.

**Fix**: Changed all to `f.write("file 'seg1_title.mp4'\n")`

### 6. Literal `\n` in print statements (cosmetic)
**Root Cause**: `print("\\n[1/5]...")` printed literal `\n` instead of newline.

**Fix**: Changed to `print("\n[1/5]...")`

## Verification

### ffprobe output for volva_demo_v2.mp4:
```
codec_name=h264
width=1920
height=1080
codec_name=aac
duration=130.770667
size=2437790
```

### Acceptance Criteria:
- [x] Duration > 120s: **130.77s ✓**
- [x] Resolution 1920x1080: **✓**
- [x] Video generated successfully

## Output Files

- **Fixed script**: `submission/video_build/build_final_video.py`
- **Generated video**: `submission/video_build/volva_demo_v2.mp4`
