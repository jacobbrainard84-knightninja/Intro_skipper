# CLAUDE.md — AI Assistant Guide for Intro Skipper

## Project Overview

**Intro Skipper** is a Python + Lua tool that automatically detects and skips TV episode intro/outro segments in VLC Media Player. It uses energy-based audio fingerprinting with graph-consensus algorithms to find repeating audio patterns across episodes.

**Version**: 4.4.0
**License**: GPL v3
**Two independent but complementary systems:**
1. **Credit Skipper** — Profile-based fixed start/stop trimming (Lua only)
2. **Intro/Outro Skipper** — Auto-detection with JSON-driven real-time seeking (Python + Lua)

---

## Repository Structure

```
/
├── skip_intro 4.4.0.py                  # Current Python detection engine
├── skip_intro 4.3.0.py                  # Previous version (reference only)
├── skip_intro 04.2.0.py                 # Older version (reference only)
├── skip_intro 04.02.py                  # Oldest version (reference only)
├── credit_Intro_outro_skipper.lua        # Current VLC Lua extension
├── credit_Intro_outro_skipper 4.2.0.lua  # Previous Lua version
├── credit_Intro_outro_skipper 4.1.0.lua  # Older Lua version
├── credit_Intro_outro_skipper 1.0.0.lua  # Oldest Lua version
└── LICENSE                              # GPL v3
```

**Active source files** are the version-less names or highest-versioned variants:
- `skip_intro 4.4.0.py` — **primary Python source**
- `credit_Intro_outro_skipper.lua` — **primary Lua source**

Older versioned files are kept for reference/rollback; do not modify them unless explicitly asked.

---

## Technology Stack

| Component | Technology |
|-----------|-----------|
| Detection engine | Python 3 |
| VLC extension | Lua |
| Audio extraction | FFmpeg / FFprobe (external) |
| Fingerprint cache | SQLite (stdlib `sqlite3`) |
| Numerical processing | `numpy` |
| Parallelism | `threading` |
| CLI | `argparse` |

**Required external tools** (must be installed separately):
- `ffmpeg` — audio extraction
- `ffprobe` — duration/metadata queries
- `vlc` — execution host for the Lua extension

---

## Running the Tool

There is **no build step**. Run directly:

```bash
# Basic usage (requires >= 2 video files in dir)
python3 "skip_intro 4.4.0.py" --video-dir /path/to/show/season1

# With show type profile
python3 "skip_intro 4.4.0.py" --video-dir /path/to/show/season1 --show-type anime

# Dry run (no output written)
python3 "skip_intro 4.4.0.py" --video-dir /path/to/show --dry-run

# Show effective config
python3 "skip_intro 4.4.0.py" --video-dir /path/to/show --show-config

# Install VLC extension
python3 "skip_intro 4.4.0.py" --install-vlc-extension

# Cache management
python3 "skip_intro 4.4.0.py" --clear-cache
python3 "skip_intro 4.4.0.py" --clear-cache-days 30
```

> Note: filenames contain spaces — always quote them on the command line.

---

## CLI Reference

| Flag | Default | Description |
|------|---------|-------------|
| `--video-dir PATH` | — | **Required.** Directory containing video files |
| `--show-type TYPE` | `standard` | Detection profile (see Show Types below) |
| `--recursive` | off | Search subdirectories recursively |
| `--import-timestamps FILE` | — | Load manual timestamps instead of auto-detecting |
| `--output FILE` | auto | Custom output path for skip_data.json |
| `--similarity-threshold FLOAT` | profile | Correlation threshold (0.0–1.0) |
| `--min-intro-duration FLOAT` | profile | Minimum intro length (seconds) |
| `--max-intro-duration FLOAT` | profile | Maximum intro length (seconds) |
| `--intro-search-start FLOAT` | profile | Start of intro search window (seconds) |
| `--intro-search-end FLOAT` | profile | End of intro search window (seconds) |
| `--outro-search-duration FLOAT` | profile | Outro search window duration (seconds) |
| `--batch-size INT` | profile | Episodes per processing batch (>= 2) |
| `--max-fingerprint-mb FLOAT` | profile | Memory limit for fingerprint storage |
| `--ffmpeg-timeout INT` | 300 | FFmpeg process timeout (seconds) |
| `--no-graph` | off | Disable graph consensus algorithm |
| `--force` | off | Reprocess all files ignoring cache |
| `--no-parallel` | off | Disable parallel audio extraction |
| `--use-full-path` | off | Use full file paths as JSON keys |
| `--dry-run` | off | Preview without writing any output |
| `--show-config` | off | Print effective configuration and exit |
| `--export-format FORMAT` | — | Export to `plex` or `jellyfin` format |
| `--export-path PATH` | — | Destination for export file |
| `--clear-cache` | — | Clear all cached fingerprints |
| `--clear-cache-days INT` | — | Clear fingerprints older than N days |
| `--generate-conf` | — | Generate legacy VLC config file |
| `--version` | — | Show version and exit |

---

## Show Type Profiles

The `--show-type` parameter selects a tuned detection configuration:

| Type | Best For |
|------|----------|
| `standard` | Most live-action TV series |
| `anime` | Anime (wider intro search window) |
| `sitcom` | 22-minute comedies |
| `comedy` | Comedy with variable intros |
| `drama` | Hour-long dramas |
| `sci_fi` | Sci-fi series |
| `horror` | Horror series |
| `reality` | Reality TV |
| `animated` | Western animated shows |
| `cold_open` | Shows with cold opens before intro |
| `late_intro` | Shows where intro appears mid-episode |
| `documentary` | Documentary series |

Each profile sets: `intro_search_start`, `intro_search_end`, `outro_search_duration`, `min_segment_duration`, `max_segment_duration`, `similarity_threshold`, `sample_rate` (22050 Hz), `n_bands` (8).

---

## Data Directories and Output Files

**Platform-specific config/data directory:**
- Linux: `~/.config/intro_skipper/`
- macOS: `~/Library/Application Support/intro_skipper/`
- Windows: `%APPDATA%\intro_skipper\`

**Generated files:**

| File | Purpose |
|------|---------|
| `cache.db` | SQLite fingerprint cache (WAL mode) |
| `skip_data.json` | Flat JSON: filename → `{intro_start, intro_end, outro_start, outro_end}` |
| `intro_skipper.log` | Operational log |

**VLC extension install paths (Linux):**
- `~/.local/share/vlc/lua/extensions/intro-skipper.lua`
- `~/.local/share/vlc/lua/intf/intro_skipper_intf.lua`

---

## Architecture and Key Algorithms

### Python Detection Pipeline

1. **Audio extraction** — FFmpeg extracts mono audio at 22050 Hz
2. **Fingerprinting** — FFT-based spectral energy in N frequency bands per 1-second window
3. **Caching** — SQLite stores fingerprints keyed by `(filepath, config_hash)` to avoid recomputation
4. **Pairwise comparison** — All episode pairs compared using windowed Pearson correlation
5. **Graph consensus** — Episodes are nodes; strong correlations form edges; consensus timestamps emerge from graph analysis
6. **Output** — `skip_data.json` written atomically via `os.replace()` on a temp file

### Lua VLC Extension

- Presents a dialog UI for manual control and profile selection
- Reads `skip_data.json` at playback start
- Background thread polls playback position every ~500ms
- Triggers seeks when position enters intro/outro windows
- Copy-on-write on `skip_data` dict ensures thread safety

---

## Key Conventions

### Filename Normalization (CRITICAL — must match between Python and Lua)

Both components normalize filenames identically for JSON key lookup:
1. Lowercase the entire filename
2. Collapse ASCII separators (spaces, tabs, `.`, `-`, `_`) to single dot
3. Replace multi-byte dashes (en-dash U+2013, em-dash U+2014) with dot
4. For filenames > 200 chars: truncate to 150 chars + `_` + SHA-256 hash (first 16 hex chars)

Any change to normalization logic **must be applied to both Python and Lua simultaneously**.

### Atomic File Writes

Always write output files via temp file + `os.replace()`:
```python
with tempfile.NamedTemporaryFile('w', dir=out_dir, delete=False, suffix='.tmp') as f:
    json.dump(data, f, indent=2)
    tmp = f.name
os.replace(tmp, final_path)
```

### SQLite Cache

- Schema version: `2`
- Two tables: `fingerprints` (audio data + config_hash) and `skip_segments` (cached results)
- WAL mode enabled for concurrent read access
- Always use `timeout=30` on connections
- Schema migrations must increment the version constant and handle upgrades

### Threading

- Python: `threading.Thread` for parallel FFmpeg extraction; use `threading.Event` for shutdown signaling
- Lua: VLC's built-in coroutine/timer model; never block the main VLC thread
- Shared state in Lua uses copy-on-write pattern

### Supported Video Formats

```
mp4, mkv, avi, mov, m4v, wmv, flv, webm, ts, mpg, mpeg, 3gp, ogv, vob,
mts, m2ts, divx, asf, f4v, rmvb, rm, ogm
```

---

## Testing

**There is no automated test suite.** Validation approaches:
- Use `--dry-run` to preview detections without writing files
- Use `--show-config` to verify parameter resolution
- Check `intro_skipper.log` for debug output
- Enable verbose logging by inspecting log file at the platform data directory
- Manually verify detections by loading a show in VLC with the extension active

When making changes, test manually with:
1. A directory of 2–4 similar TV episodes
2. Both `--dry-run` mode (verify no crash) and a full run (verify JSON output)
3. VLC extension load after `--install-vlc-extension`

---

## Development Guidelines

### Making Changes to Detection Logic

- The primary source is `skip_intro 4.4.0.py` (~3,244 lines)
- Show type profiles are defined in a `SHOW_CONFIGS` dict near the top of the file
- The `DEFAULT_CONFIG` dict sets fallback values for all tunable parameters
- Fingerprinting logic is in the `AudioFingerprinter` class
- Graph consensus is in `build_consensus_timestamps()` or equivalent function
- Output serialization uses `write_skip_data()` (atomic write)

### Making Changes to VLC Extension

- Primary source: `credit_Intro_outro_skipper.lua` (~1,797 lines)
- The `descriptor()` function defines VLC extension metadata
- `activate()` / `deactivate()` manage extension lifecycle
- JSON loading happens in the init sequence; key normalization must match Python
- The background polling loop is the core skip-trigger mechanism

### Version Bumping

When releasing a new version:
1. Update the version constant in the Python script
2. Update the version comment/header in the Lua file
3. Rename the active files to include the new version number (keep old versioned copy)
4. Update the Lua CHANGELOG block at the top of the file

### Adding a New Show Type

1. Add an entry to `SHOW_CONFIGS` in the Python script with all required keys
2. Add the type name to the `--show-type` argparse choices list
3. Document the new type in this file under "Show Type Profiles"

---

## Common Pitfalls

1. **Filenames with spaces** — The main Python script file has spaces in its name; always quote it
2. **Minimum 2 episodes required** — Auto-detection compares pairs; a single file produces no output
3. **FFmpeg must be on PATH** — The script does not bundle FFmpeg; if missing, extraction silently fails
4. **Cache invalidation** — After changing show type or thresholds, use `--force` or `--clear-cache` to avoid stale cached segments
5. **Key mismatch** — If the Lua extension doesn't skip, the most common cause is filename normalization mismatch between the JSON keys and the currently playing file's name
6. **VLC manual entry limitation** — VLC's manual timestamp entry uses a different code path; see Lua comments for details

---

## No CI/CD

There are no automated pipelines, linters, or formatters configured. Code style follows the existing conventions in each file (PEP 8-ish for Python, standard Lua indentation).
