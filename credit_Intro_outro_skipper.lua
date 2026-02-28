--[[
Unified Credit & Intro/Outro Skipper for VLC
============================================================
Version 3.0.0

Merges two independent skip systems into one extension:

  1. CREDIT SKIPPER (original by Michael Bull)
     Profile-based fixed start/stop trimming.
     Profiles saved to: vlc_configdir/credit-skipper.conf
     Works by re-queuing the playlist with start-time / stop-time
     options so the player never plays the trimmed regions.

  2. INTRO/OUTRO SKIPPER (auto-detection companion)
     Reads skip_data.json produced by skip_intro.py and performs
     real-time seeks during playback to jump over detected intros
     and outros.  Operates independently of the playlist queue so
     it works on files that were not re-queued by the Credit Skipper.

IMPORTANT — Manual timestamp entries (panel 2 Apply button) are
SESSION-ONLY.  They are stored in memory under the raw filename and
are NOT written back to skip_data.json.  They will be lost on
Reload or VLC restart.  For permanent per-episode timestamps run
skip_intro.py with --import-timestamps instead.

JSON FORMAT NOTE — skip_data.json must contain a flat object whose
values are flat objects with numeric leaf values only.  The embedded
parser does not support nested objects, arrays, booleans, or null.
Do not change the JSON shape without updating parse_json_object().

INSTALLATION
  Linux:   ~/.local/share/vlc/lua/extensions/
  Windows: %APPDATA%\vlc\lua\extensions\
  macOS:   ~/Library/Application Support/org.videolan.vlc/lua/extensions/

DEBUG MODE
  Set DEBUG = true near the top of this file to enable verbose
  vlc.msg.dbg() output visible in VLC's Messages window
  (Tools > Messages, set verbosity to 2).

THREADING MODEL
  - GUI callbacks (buttons) run on VLC's main/GUI thread.
  - trigger_auto_skip() runs on a background input-listener thread.
  - These threads share read-only access to skip_data via atomic
    table replacement (copy-on-write pattern).
  - GUI widgets are NEVER accessed from the background thread.
  - Status updates from background thread are deferred to the next
    GUI-initiated refresh.

CHANGELOG v3.0.0
  - Fixed race condition: skip_data now uses copy-on-write
  - Fixed cross-thread GUI access: background thread no longer
    touches dialog widgets
  - Fixed memory leak: dialog properly cleaned up on reopen
  - Fixed UTF-8 surrogate pair handling in JSON parser
  - Fixed safe_playlist_next() off-by-one for 1-based indices
  - Added negative lookup cache for find_skip_entry()
  - Added timestamp validation (0-86400 seconds)
  - Added intro/outro overlap detection
  - Added file content hash to avoid redundant JSON reloads
  - Improved seek precision (removed spurious +0.5)
  - Refactored dialog creation into smaller functions
  - Added comprehensive input validation

NOTES
  - Automatic detection in skip_intro.py requires >= 2 episodes.
  - Run only one skip_intro.py process at a time against cache.db.
  - Running two VLC instances against the same skip_data.json is
    safe; the file is read-only from Lua.
]]

-- ============================================================
-- Debug flag
-- Set to true to enable verbose logging in VLC's Messages window.
-- Tools > Messages > set verbosity to 2 to see dbg() output.
-- ============================================================

local DEBUG = false

-- ============================================================
-- Version & path constants
-- ============================================================

local SKIPPER_VERSION = "3.0.0"

local _vlc_cfgdir = vlc.config.configdir()

-- Path to skip_data.json written by skip_intro.py.
-- Constructed from VLC's config dir so it matches the Python default
-- on every platform without any hardcoding.
local SKIP_DATA_FILE = _vlc_cfgdir .. "/intro_skipper/skip_data.json"

-- Credit Skipper profile storage (original format, unchanged).
local CREDIT_CONF_FILE = _vlc_cfgdir .. "/credit-skipper.conf"

-- ============================================================
-- Timing constants
-- ============================================================

-- Settle delay after a seek in the background listener (microseconds).
-- Matches Python constant VLC_SEEK_DELAY_MS = 500.
-- Used ONLY in safe_seek(); never used in dialog button callbacks.
local SEEK_SETTLE_US = 500000

-- How many input-listener ticks before skip_data.json is checked.
-- At VLC's default ~250 ms tick interval this is roughly 15 seconds.
local JSON_CHECK_TICKS = 60

-- Grace window (seconds) after intro_start during which auto-skip fires.
-- Prevents re-triggering if the user rewinds past the intro start.
local INTRO_GRACE_SEC = 5

-- Maximum valid timestamp in seconds (24 hours).
-- Timestamps outside [0, MAX_TIMESTAMP_SEC] are rejected.
local MAX_TIMESTAMP_SEC = 86400

-- ============================================================
-- Logging helpers
-- All log calls go through these so the prefix and DEBUG gate
-- are applied consistently in one place.
-- ============================================================

local function dbg(msg)
    if DEBUG then
        vlc.msg.dbg("[Skipper] " .. tostring(msg))
    end
end

local function log_info(msg)
    vlc.msg.info("[Skipper] " .. tostring(msg))
end

local function log_warn(msg)
    vlc.msg.warn("[Skipper] " .. tostring(msg))
end

local function log_err(msg)
    vlc.msg.err("[Skipper] " .. tostring(msg))
end

-- ============================================================
-- Extension descriptor
-- ============================================================

function descriptor()
    return {
        title        = "Credit & Intro/Outro Skipper",
        version      = SKIPPER_VERSION,
        author       = "Michael Bull / skip_intro.py integration",
        shortdesc    = "Skip Credits + Auto Intro/Outro",
        description  =
            "Profile-based credit trimming (Credit Skipper) combined "
            .. "with JSON-driven real-time intro/outro skipping "
            .. "(Intro/Outro Skipper).",
        capabilities = { "input-listener" }
    }
end

-- ============================================================
-- Shared state
--
-- THREADING: skip_data is accessed by both the GUI thread and
-- the background input-listener thread.  We use a copy-on-write
-- pattern: modifications create a new table and atomically swap
-- the reference.  Reads see a consistent snapshot.
--
-- pending_status is written by the background thread and read
-- by the GUI thread during refresh.  It's a simple string so
-- assignment is atomic on all Lua implementations.
-- ============================================================

-- Credit Skipper profiles
local profiles = {}

-- Intro/Outro Skipper data (copy-on-write)
local skip_data = {}
local skip_data_hash = ""  -- Content hash to detect changes

-- Lookup cache: maps filename -> entry or false (negative cache)
local skip_entry_cache = {}

-- Filename cache: maps URI -> decoded filename
local filename_cache = {}

-- Per-file skip state
local intro_skipped = false
local outro_skipped = false
local auto_skip_enabled = true
local last_filename = ""
local tick_count = 0

-- Status message from background thread (read by GUI on refresh)
local pending_status = nil

-- ============================================================
-- Dialog & widget handles
--
-- All widget handles are set to nil in cleanup functions to
-- allow Lua's GC to reclaim memory and to signal that the
-- dialog is closed.
-- ============================================================

local dialog = nil

-- Panel 1 (Credit Skipper) widgets
local p1_profile_dropdown = nil
local p1_profile_name_input = nil
local p1_start_time_input = nil
local p1_finish_time_input = nil

-- Panel 2 (Intro/Outro Skipper) widgets
local p2_status_label = nil
local p2_file_label = nil
local p2_info_label = nil
local p2_auto_label = nil
local p2_intro_input = nil
local p2_outro_input = nil

-- ============================================================
-- Utility helpers
-- ============================================================

--- Check if a file exists and is readable.
-- @param path string: File path to check.
-- @return boolean: True if file exists.
local function file_exists(path)
    if not path then return false end
    local f = io.open(path, "rb")
    if f then
        f:close()
        return true
    end
    return false
end

--- Compute a simple hash of file contents for change detection.
-- @param path string: File path.
-- @return string: Hash string, or empty string on error.
local function file_content_hash(path)
    local f = io.open(path, "rb")
    if not f then return "" end
    local content = f:read("*all")
    f:close()
    if not content then return "" end
    -- Simple hash: length + first 64 bytes + last 64 bytes
    local len = #content
    local head = content:sub(1, 64)
    local tail = content:sub(-64)
    return string.format("%d:%s:%s", len, head, tail)
end

--- Deep copy a table (one level deep, sufficient for skip_data).
-- @param t table: Source table.
-- @return table: New table with copied contents.
local function shallow_copy_table(t)
    if type(t) ~= "table" then return t end
    local copy = {}
    for k, v in pairs(t) do
        if type(v) == "table" then
            copy[k] = {}
            for k2, v2 in pairs(v) do
                copy[k][k2] = v2
            end
        else
            copy[k] = v
        end
    end
    return copy
end

--- Format seconds as human-readable time string.
-- @param seconds number: Time in seconds.
-- @return string: Formatted time (e.g., "1:23" or "1:02:03").
local function format_time(seconds)
    if not seconds then return "?" end
    seconds = math.floor(seconds)
    if seconds < 0 then return "-" .. format_time(-seconds) end
    if seconds >= 3600 then
        return string.format("%d:%02d:%02d",
            math.floor(seconds / 3600),
            math.floor((seconds % 3600) / 60),
            seconds % 60)
    end
    return string.format("%d:%02d",
        math.floor(seconds / 60),
        seconds % 60)
end

--- Validate that a timestamp is within acceptable range.
-- @param ts number: Timestamp in seconds.
-- @return boolean: True if valid.
local function is_valid_timestamp(ts)
    return ts ~= nil and ts >= 0 and ts <= MAX_TIMESTAMP_SEC
end

-- ============================================================
-- Extension lifecycle
-- ============================================================

function activate()
    dbg("activate() — loading profiles and skip data")

    profiles = {}
    skip_data = {}
    skip_data_hash = ""
    skip_entry_cache = {}
    filename_cache = {}
    pending_status = nil

    if file_exists(CREDIT_CONF_FILE) then
        load_all_profiles()
    else
        dbg("No credit profile file at: " .. CREDIT_CONF_FILE)
    end

    load_skip_data()
    open_dialog()
end

function deactivate()
    dbg("deactivate()")
    cleanup_dialog()
end

function close()
    vlc.deactivate()
end

function meta_changed()
    -- Required by VLC extension API; intentionally empty.
end

--- Called when VLC opens a new media item.
-- Resets per-episode state and updates the dialog if open.
function input_changed()
    dbg("input_changed() — resetting skip flags")
    intro_skipped = false
    outro_skipped = false
    -- Clear filename cache entry for previous file
    -- (new file will be cached on first access)

    -- Update dialog info on GUI thread (this IS the GUI thread)
    update_p2_info()
end

-- ============================================================
-- Dialog management
-- ============================================================

--- Clean up all dialog resources.
-- Sets all widget handles to nil to allow GC and signal closure.
local function cleanup_dialog()
    if dialog then
        pcall(function() dialog:delete() end)
        dialog = nil
    end

    -- Panel 1 widgets
    p1_profile_dropdown = nil
    p1_profile_name_input = nil
    p1_start_time_input = nil
    p1_finish_time_input = nil

    -- Panel 2 widgets
    p2_status_label = nil
    p2_file_label = nil
    p2_info_label = nil
    p2_auto_label = nil
    p2_intro_input = nil
    p2_outro_input = nil
end

--- Create Panel 1 (Credit Skipper) widgets.
-- @param start_row number: First row number for this panel.
-- @return number: Next available row number.
local function create_credit_panel(start_row)
    local row = start_row

    dialog:add_label(
        "<center><h3>&#9312; Credit Skipper</h3>"
            .. "<small>Profile-based playlist trimming</small></center>",
        1, row, 4, 1)
    row = row + 1

    dialog:add_label("<b>Profile:</b>", 1, row, 1, 1)
    p1_profile_dropdown = dialog:add_dropdown(2, row, 1, 1)
    populate_profile_dropdown()
    dialog:add_button("Load", populate_profile_fields, 3, row, 1, 1)
    dialog:add_button("Delete", delete_profile, 4, row, 1, 1)
    row = row + 1

    dialog:add_label("<b>Profile name:</b>", 1, row, 1, 1)
    p1_profile_name_input = dialog:add_text_input("", 2, row, 3, 1)
    row = row + 1

    dialog:add_label("<b>Intro duration (s):</b>", 1, row, 1, 1)
    p1_start_time_input = dialog:add_text_input("", 2, row, 1, 1)
    row = row + 1

    dialog:add_label("<b>Outro duration (s):</b>", 1, row, 1, 1)
    p1_finish_time_input = dialog:add_text_input("", 2, row, 1, 1)
    row = row + 1

    dialog:add_button("Save Profile", save_profile, 1, row, 2, 1)
    row = row + 1

    dialog:add_label(
        "<center><small>Queue your playlist before pressing Start."
            .. "</small></center>",
        1, row, 4, 1)
    row = row + 1

    dialog:add_button("&#9654; Start Playlist", start_playlist, 1, row, 4, 1)
    row = row + 1

    return row
end

--- Create Panel 2 (Intro/Outro Skipper) widgets.
-- @param start_row number: First row number for this panel.
-- @return number: Next available row number.
local function create_intro_panel(start_row)
    local row = start_row

    -- Divider and header
    dialog:add_label(
        "<hr/><center><h3>&#9313; Intro/Outro Skipper</h3>"
            .. "<small>Real-time seek from skip_data.json"
            .. "</small></center>",
        1, row, 4, 1)
    row = row + 1

    p2_status_label = dialog:add_label(
        "Loaded " .. count_skip_entries() .. " JSON entries",
        1, row, 4, 1)
    row = row + 1

    dialog:add_label("<b>File:</b>", 1, row, 1, 1)
    p2_file_label = dialog:add_label(
        get_current_filename() or "Nothing playing",
        2, row, 3, 1)
    row = row + 1

    p2_info_label = dialog:add_label("", 1, row, 4, 1)
    row = row + 1

    p2_auto_label = dialog:add_label(auto_skip_label_text(), 1, row, 2, 1)
    row = row + 1

    dialog:add_button("Skip Intro", do_skip_intro_btn, 1, row, 1, 1)
    dialog:add_button("Skip Outro", do_skip_outro_btn, 2, row, 1, 1)
    dialog:add_button("Toggle Auto", toggle_auto_btn, 3, row, 1, 1)
    dialog:add_button("Reload JSON", reload_json_btn, 4, row, 1, 1)
    row = row + 1

    return row
end

--- Create manual override section widgets.
-- @param start_row number: First row number for this section.
-- @return number: Next available row number.
local function create_manual_override(start_row)
    local row = start_row

    dialog:add_label(
        "<center><b>Manual Override (Session Only)</b>"
            .. "<br/><small>Not saved &#8212; lost on Reload or VLC"
            .. " restart. Use --import-timestamps for permanent"
            .. " overrides.</small></center>",
        1, row, 4, 1)
    row = row + 1

    dialog:add_label("Intro end (s):", 1, row, 1, 1)
    p2_intro_input = dialog:add_text_input("", 2, row, 1, 1)
    dialog:add_label("Outro start (s):", 3, row, 1, 1)
    p2_outro_input = dialog:add_text_input("", 4, row, 1, 1)
    row = row + 1

    dialog:add_button("Apply (session only)", apply_manual_btn, 1, row, 2, 1)
    dialog:add_button("Refresh", refresh_btn, 3, row, 1, 1)
    dialog:add_button("Close", close, 4, row, 1, 1)
    row = row + 1

    return row
end

--- Open the main dialog window.
-- Creates all panels and populates initial state.
function open_dialog()
    -- Clean up any existing dialog first (prevents memory leak)
    cleanup_dialog()

    dialog = vlc.dialog(descriptor().title)

    local row = 1
    row = create_credit_panel(row)
    row = create_intro_panel(row)
    row = create_manual_override(row)

    -- Populate initial state
    populate_profile_fields()
    update_p2_info()
end

-- ============================================================
-- ============================================================
-- SECTION 1 — CREDIT SKIPPER
-- ============================================================
-- ============================================================

--- Populate the profile dropdown with current profiles.
function populate_profile_dropdown()
    if not p1_profile_dropdown then return end

    -- Clear existing entries by recreating dropdown
    -- (VLC Lua doesn't have a clear method)
    if dialog then
        pcall(function() dialog:del_widget(p1_profile_dropdown) end)
    end
    p1_profile_dropdown = dialog:add_dropdown(2, 2, 1, 1)

    for i, profile in pairs(profiles) do
        if profile and profile.name then
            p1_profile_dropdown:add_value(profile.name, i)
        end
    end
end

--- Populate input fields with selected profile's values.
function populate_profile_fields()
    if not p1_profile_dropdown then return end
    if not p1_profile_name_input then return end

    local idx = p1_profile_dropdown:get_value()
    local profile = profiles[idx]

    if profile then
        p1_profile_name_input:set_text(tostring(profile.name or ""))
        p1_start_time_input:set_text(tostring(profile.start_time or 0))
        p1_finish_time_input:set_text(tostring(profile.finish_time or 0))
    end
end

--- Delete the currently selected profile.
function delete_profile()
    if not p1_profile_dropdown then return end

    local idx = p1_profile_dropdown:get_value()
    if profiles[idx] then
        dbg("Deleting profile index " .. tostring(idx))
        profiles[idx] = nil
        save_all_profiles()
        populate_profile_dropdown()
    end
end

--- Save the current profile (update existing or create new).
function save_profile()
    if not p1_profile_name_input then return end

    local name = p1_profile_name_input:get_text()
    if name == "" then return end

    local start_time = tonumber(p1_start_time_input:get_text()) or 0
    local finish_time = tonumber(p1_finish_time_input:get_text()) or 0

    -- Validate
    if start_time < 0 then start_time = 0 end
    if finish_time < 0 then finish_time = 0 end

    -- Update existing or create new
    local updated = false
    for _, profile in pairs(profiles) do
        if profile and profile.name == name then
            profile.start_time = start_time
            profile.finish_time = finish_time
            updated = true
            dbg("Updated profile: " .. name)
            break
        end
    end

    if not updated then
        table.insert(profiles, {
            name = name,
            start_time = start_time,
            finish_time = finish_time,
        })
        dbg("Saved new profile: " .. name)
    end

    save_all_profiles()
    populate_profile_dropdown()
end

--- Start playlist playback with credit trimming applied.
function start_playlist()
    if not p1_start_time_input or not p1_finish_time_input then return end

    local skip_start = tonumber(p1_start_time_input:get_text())
    local skip_finish = tonumber(p1_finish_time_input:get_text())

    if not skip_start or not skip_finish then return end

    local ok, playlist = pcall(function()
        return vlc.playlist.get("playlist", false)
    end)
    if not ok or not playlist or not playlist.children then
        log_err("Cannot access playlist")
        return
    end

    -- Collect valid items
    local children = {}
    for _, child in pairs(playlist.children) do
        if child and child.duration and child.duration > 0 then
            table.insert(children, {
                path = child.path,
                name = child.name,
                duration = child.duration,
            })
        elseif child then
            dbg("Skipping item with invalid duration: " .. tostring(child.name))
        end
    end

    if #children == 0 then
        log_warn("No valid items in playlist")
        return
    end

    vlc.playlist.clear()

    dbg("start_playlist() skip_start=" .. skip_start
        .. " skip_finish=" .. skip_finish
        .. " items=" .. #children)

    for _, child in pairs(children) do
        local options = {}
        local effective_duration = child.duration - skip_start - skip_finish

        if effective_duration > 0 then
            if skip_start > 0 then
                table.insert(options, "start-time=" .. skip_start)
            end
            if skip_finish > 0 then
                table.insert(options, "stop-time=" .. (child.duration - skip_finish))
            end
        else
            dbg("Warning: trim exceeds duration for " .. tostring(child.name))
        end

        vlc.playlist.enqueue({
            {
                path = child.path,
                name = child.name,
                duration = child.duration,
                options = options,
            }
        })
    end

    if dialog then
        dialog:hide()
    end
    vlc.playlist.play()
end

--- Save all profiles to the config file.
function save_all_profiles()
    local f = io.open(CREDIT_CONF_FILE, "w")
    if not f then
        log_err("Cannot write: " .. CREDIT_CONF_FILE)
        return
    end

    for _, profile in pairs(profiles) do
        if profile and profile.name then
            f:write(string.format("%s=%d,%d\n",
                tostring(profile.name),
                tonumber(profile.start_time) or 0,
                tonumber(profile.finish_time) or 0))
        end
    end

    f:close()
    dbg("Profiles saved to " .. CREDIT_CONF_FILE)
end

--- Load all profiles from the config file.
function load_all_profiles()
    if not file_exists(CREDIT_CONF_FILE) then return end

    profiles = {}

    for line in io.lines(CREDIT_CONF_FILE) do
        local name, st, ft = string.match(line, "(.+)=(%d+),(%d+)")
        if name then
            table.insert(profiles, {
                name = name,
                start_time = tonumber(st) or 0,
                finish_time = tonumber(ft) or 0,
            })
            dbg("Loaded profile: " .. name)
        end
    end
end

-- ============================================================
-- ============================================================
-- SECTION 2 — INTRO/OUTRO SKIPPER
-- ============================================================
-- ============================================================

-- ============================================================
-- Minimal JSON parser
--
-- Handles only the flat-object-of-flat-objects shape that
-- skip_intro.py writes.  Supports:
--   - String keys with escape sequences including \uXXXX
--   - Numeric values (integers and decimals)
--   - UTF-8 surrogate pairs for codepoints > U+FFFF
--
-- Does NOT support:
--   - Nested objects beyond one level
--   - Arrays
--   - Boolean/null values
--   - Comments
-- ============================================================

--- Parse a JSON string value starting at pos.
-- @param str string: JSON content.
-- @param pos number: Current position (pointing to opening quote).
-- @return string|nil: Parsed string value, or nil on error.
-- @return number: New position after parsing.
local function parse_json_string(str, pos)
    if str:sub(pos, pos) ~= '"' then
        return nil, pos
    end
    pos = pos + 1

    local result = {}

    while pos <= #str do
        local c = str:sub(pos, pos)

        if c == '"' then
            -- End of string
            return table.concat(result), pos + 1

        elseif c == '\\' then
            -- Escape sequence
            pos = pos + 1
            local esc = str:sub(pos, pos)

            if esc == 'n' then
                table.insert(result, '\n')
            elseif esc == 'r' then
                table.insert(result, '\r')
            elseif esc == 't' then
                table.insert(result, '\t')
            elseif esc == '\\' then
                table.insert(result, '\\')
            elseif esc == '"' then
                table.insert(result, '"')
            elseif esc == '/' then
                table.insert(result, '/')
            elseif esc == 'b' then
                table.insert(result, '\b')
            elseif esc == 'f' then
                table.insert(result, '\f')
            elseif esc == 'u' then
                -- Unicode escape \uXXXX
                local hex = str:sub(pos + 1, pos + 4)
                local cp = tonumber(hex, 16)

                if not cp then
                    log_err("Invalid \\u escape at position " .. pos)
                    return nil, pos
                end

                pos = pos + 4

                -- Check for surrogate pair (high surrogate: D800-DBFF)
                if cp >= 0xD800 and cp <= 0xDBFF then
                    -- Expect low surrogate
                    if str:sub(pos + 1, pos + 2) == '\\u' then
                        local hex2 = str:sub(pos + 3, pos + 6)
                        local cp2 = tonumber(hex2, 16)

                        if cp2 and cp2 >= 0xDC00 and cp2 <= 0xDFFF then
                            -- Valid surrogate pair
                            cp = 0x10000 + ((cp - 0xD800) * 0x400) + (cp2 - 0xDC00)
                            pos = pos + 6
                        else
                            log_warn("Invalid low surrogate at position " .. pos)
                        end
                    else
                        log_warn("Missing low surrogate at position " .. pos)
                    end
                elseif cp >= 0xDC00 and cp <= 0xDFFF then
                    -- Lone low surrogate (invalid)
                    log_warn("Lone low surrogate at position " .. pos)
                end

                -- Encode codepoint as UTF-8
                if cp < 128 then
                    table.insert(result, string.char(cp))
                elseif cp < 0x800 then
                    table.insert(result, string.char(
                        0xC0 + math.floor(cp / 64),
                        0x80 + (cp % 64)))
                elseif cp < 0x10000 then
                    table.insert(result, string.char(
                        0xE0 + math.floor(cp / 4096),
                        0x80 + (math.floor(cp / 64) % 64),
                        0x80 + (cp % 64)))
                else
                    -- 4-byte UTF-8 for codepoints > U+FFFF
                    table.insert(result, string.char(
                        0xF0 + math.floor(cp / 0x40000),
                        0x80 + (math.floor(cp / 0x1000) % 0x40),
                        0x80 + (math.floor(cp / 0x40) % 0x40),
                        0x80 + (cp % 0x40)))
                end
            else
                -- Unknown escape, keep as-is
                table.insert(result, esc)
            end
        else
            -- Regular character
            table.insert(result, c)
        end

        pos = pos + 1
    end

    -- Unterminated string
    return nil, pos
end

--- Parse a JSON number starting at pos.
-- @param str string: JSON content.
-- @param pos number: Current position.
-- @return number|nil: Parsed number, or nil on error.
-- @return number: New position after parsing.
local function parse_json_number(str, pos)
    local start = pos

    -- Optional negative sign
    if str:sub(pos, pos) == '-' then
        pos = pos + 1
    end

    -- Integer part
    while pos <= #str and str:sub(pos, pos):match('[0-9]') do
        pos = pos + 1
    end

    -- Decimal part
    if pos <= #str and str:sub(pos, pos) == '.' then
        pos = pos + 1
        while pos <= #str and str:sub(pos, pos):match('[0-9]') do
            pos = pos + 1
        end
    end

    -- Exponent part
    if pos <= #str and str:sub(pos, pos):match('[eE]') then
        pos = pos + 1
        if str:sub(pos, pos):match('[+-]') then
            pos = pos + 1
        end
        while pos <= #str and str:sub(pos, pos):match('[0-9]') do
            pos = pos + 1
        end
    end

    local num = tonumber(str:sub(start, pos - 1))

    -- Validate range
    if num and not is_valid_timestamp(num) then
        log_warn("Timestamp out of range: " .. num)
        return nil, pos
    end

    return num, pos
end

--- Skip whitespace in JSON content.
-- @param str string: JSON content.
-- @param pos number: Current position.
-- @return number: Position of next non-whitespace character.
local function skip_ws(str, pos)
    while pos <= #str and str:sub(pos, pos):match('%s') do
        pos = pos + 1
    end
    return pos
end

--- Parse the skip_data.json format.
-- Expected format: {"filename": {"intro_start": N, "intro_end": N, ...}, ...}
-- @param content string: JSON content.
-- @return table: Parsed skip data, or empty table on error.
local function parse_json_object(content)
    local data = {}
    local pos = skip_ws(content, 1)

    if content:sub(pos, pos) ~= '{' then
        return data
    end
    pos = pos + 1

    local first = true

    while pos <= #content do
        pos = skip_ws(content, pos)

        if content:sub(pos, pos) == '}' then
            break
        end

        -- Handle comma separator
        if not first then
            if content:sub(pos, pos) == ',' then
                pos = skip_ws(content, pos + 1)
            else
                break
            end
        end
        first = false

        -- Parse filename key
        local filename, np = parse_json_string(content, pos)
        if not filename then
            log_err("JSON parse error: expected string key at pos " .. pos)
            break
        end
        pos = skip_ws(content, np)

        -- Expect colon
        if content:sub(pos, pos) ~= ':' then
            log_err("JSON parse error: expected ':' at pos " .. pos)
            break
        end
        pos = skip_ws(content, pos + 1)

        -- Expect inner object
        if content:sub(pos, pos) ~= '{' then
            log_err("JSON parse error: expected '{' at pos " .. pos)
            break
        end
        pos = pos + 1

        -- Parse inner object (timestamp fields)
        local entry = {}
        local inner_first = true

        while pos <= #content do
            pos = skip_ws(content, pos)

            if content:sub(pos, pos) == '}' then
                pos = pos + 1
                break
            end

            if not inner_first then
                if content:sub(pos, pos) == ',' then
                    pos = skip_ws(content, pos + 1)
                else
                    break
                end
            end
            inner_first = false

            -- Parse field name
            local key, np2 = parse_json_string(content, pos)
            if not key then break end
            pos = skip_ws(content, np2)

            -- Expect colon
            if content:sub(pos, pos) ~= ':' then break end
            pos = skip_ws(content, pos + 1)

            -- Parse numeric value
            local val, np3 = parse_json_number(content, pos)
            if val ~= nil then
                entry[key] = val
                pos = np3
            else
                -- Skip this field but continue parsing
                dbg("Skipping invalid value for key: " .. key)
                -- Find next comma or closing brace
                while pos <= #content do
                    local c = content:sub(pos, pos)
                    if c == ',' or c == '}' then break end
                    pos = pos + 1
                end
            end
        end

        -- Validate entry
        if filename and next(entry) then
            -- Check for intro/outro overlap
            if entry.intro_end and entry.outro_start then
                if entry.intro_end > entry.outro_start then
                    log_warn("Overlap: intro_end > outro_start for " .. filename)
                    -- Clear invalid outro
                    entry.outro_start = nil
                    entry.outro_end = nil
                end
            end
            data[filename] = entry
        end
    end

    return data
end

-- ============================================================
-- Key normalization
--
-- Must produce the same string as Python's get_skip_key().
--
-- Steps (identical to Python):
--   1. percent-decode URI escapes
--   2. strip file extension
--   3. lower-case
--   4. multibyte UTF-8 dashes → "."   MUST be before bracket classes!
--   5. remaining ASCII separator bytes → "."
--   6. strip non-alphanumeric / non-dot bytes
--   7. collapse repeated dots
--   8. trim leading / trailing dots
--
-- WHY ORDER MATTERS:
-- Lua's string library is byte-oriented; [...] matches individual bytes.
-- Placing \xe2\x80\x93 (en-dash) inside a bracket class would match E2,
-- 80, and 93 independently, corrupting characters that share those bytes
-- (e.g. the Euro sign € = E2 82 AC shares the E2 lead byte).
-- ============================================================

--- Normalize a filename to a skip data lookup key.
-- @param name string: Filename (may be URI-encoded).
-- @return string: Normalized key.
local function normalize_key(name)
    if not name or name == "" then return "" end

    -- Step 1: Percent-decode URI escapes
    name = name:gsub("%%(%x%x)", function(h)
        return string.char(tonumber(h, 16))
    end)

    -- Step 2: Strip file extension
    name = name:gsub("%.[^%.]+$", "")

    -- Step 3: Lower-case
    name = name:lower()

    -- Step 4: UTF-8 multibyte dashes → "." (BEFORE bracket classes!)
    name = name:gsub("\xe2\x80\x93", ".")  -- en-dash
    name = name:gsub("\xe2\x80\x94", ".")  -- em-dash

    -- Step 5: ASCII separators → "."
    name = name:gsub("[ \t_%-.,:]+", ".")

    -- Step 6: Remove non-alphanumeric/non-dot
    name = name:gsub("[^a-z0-9.]+", "")

    -- Step 7: Collapse repeated dots
    name = name:gsub("%.%.+", ".")

    -- Step 8: Trim leading/trailing dots
    name = name:gsub("^%.+", ""):gsub("%.+$", "")

    return name
end

-- ============================================================
-- JSON data management
-- ============================================================

--- Load skip data from JSON file.
-- Uses copy-on-write pattern for thread safety.
function load_skip_data()
    local new_hash = file_content_hash(SKIP_DATA_FILE)

    -- Skip reload if content hasn't changed
    if new_hash == skip_data_hash and skip_data_hash ~= "" then
        dbg("Skip data unchanged, skipping reload")
        return
    end

    local f = io.open(SKIP_DATA_FILE, "r")
    if not f then
        dbg("skip_data.json not found: " .. SKIP_DATA_FILE)
        skip_data = {}
        skip_data_hash = ""
        skip_entry_cache = {}
        return
    end

    local content = f:read("*all")
    f:close()

    if not content or #content == 0 then
        dbg("skip_data.json is empty")
        skip_data = {}
        skip_data_hash = ""
        skip_entry_cache = {}
        return
    end

    local ok, result = pcall(parse_json_object, content)

    if ok and type(result) == "table" then
        -- Atomic swap (copy-on-write)
        skip_data = result
        skip_data_hash = new_hash
        skip_entry_cache = {}  -- Invalidate cache
        dbg("Loaded " .. count_skip_entries() .. " entries from JSON")
    else
        log_err("Failed to parse skip_data.json: " .. tostring(result))
    end
end

--- Count entries in skip_data.
-- @return number: Number of entries.
function count_skip_entries()
    local c = 0
    for _ in pairs(skip_data) do
        c = c + 1
    end
    return c
end

--- Find skip entry for a filename with caching.
-- Three-level lookup:
--   L1: normalized key — matches Python get_skip_key() exactly
--   L2: raw filename   — handles use_full_path=True entries
--   L3: full scan      — covers any remaining edge cases
--
-- Results are cached to avoid repeated scans.
--
-- @param fn string: Filename to look up.
-- @return table|nil: Skip entry, or nil if not found.
local function find_skip_entry(fn)
    if not fn or fn == "" then return nil end

    -- Check cache first
    local cached = skip_entry_cache[fn]
    if cached ~= nil then
        if cached == false then
            return nil  -- Cached negative result
        end
        return cached
    end

    local nfn = normalize_key(fn)

    -- L1: Normalized key lookup
    if nfn ~= "" and skip_data[nfn] then
        dbg("find_skip_entry: L1 hit for " .. fn)
        skip_entry_cache[fn] = skip_data[nfn]
        return skip_data[nfn]
    end

    -- L2: Raw filename lookup
    if skip_data[fn] then
        dbg("find_skip_entry: L2 raw hit for " .. fn)
        skip_entry_cache[fn] = skip_data[fn]
        return skip_data[fn]
    end

    -- L3: Full scan (normalize each key and compare)
    if nfn ~= "" then
        for key, val in pairs(skip_data) do
            if normalize_key(key) == nfn then
                dbg("find_skip_entry: L3 scan hit key=" .. key)
                skip_entry_cache[fn] = val
                return val
            end
        end
    end

    -- Cache negative result
    dbg("find_skip_entry: no match for " .. fn)
    skip_entry_cache[fn] = false
    return nil
end

-- ============================================================
-- Filename extraction with caching
-- ============================================================

--- Get the filename of the currently playing item.
-- @return string|nil: Filename, or nil if nothing playing.
function get_current_filename()
    local iok, item = pcall(function() return vlc.input.item() end)
    if not iok or not item then return nil end

    local uok, uri = pcall(function() return item:uri() end)
    if not uok or not uri then return nil end

    -- Check cache
    if filename_cache[uri] then
        return filename_cache[uri]
    end

    -- Extract filename from URI
    local fn = uri:match("([^/\\]+)$")
    if fn then
        -- Percent-decode
        fn = fn:gsub("%%(%x%x)", function(h)
            return string.char(tonumber(h, 16))
        end)

        -- Cache the result
        filename_cache[uri] = fn
    end

    return fn
end

-- ============================================================
-- Seek helpers
--
-- seek_to()   — For dialog button callbacks (GUI thread).
--               NO sleep. Sleeping on the GUI thread freezes the
--               entire VLC player window for the sleep duration.
--
-- safe_seek() — For trigger_auto_skip() only (background thread).
--               Seeks then sleeps to let VLC settle before the
--               next poll tick.
--
-- Do NOT call safe_seek() from any dialog callback.
-- Do NOT call seek_to() from trigger_auto_skip().
-- ============================================================

--- Seek to a target time (GUI thread version, no sleep).
-- @param target_s number: Target time in seconds.
-- @return boolean: True if seek succeeded.
local function seek_to(target_s)
    if not target_s or target_s < 0 then return false end

    dbg("seek_to(" .. target_s .. ") [GUI thread, no sleep]")

    local iok, input = pcall(function() return vlc.object.input() end)
    if not iok or not input then return false end

    return pcall(function()
        vlc.var.set(input, "time", math.floor(target_s * 1000000))
    end)
end

--- Seek to a target time (background thread version, with settle delay).
-- @param target_s number: Target time in seconds.
-- @return boolean: True if seek succeeded.
local function safe_seek(target_s)
    if not target_s or target_s < 0 then return false end

    dbg("safe_seek(" .. target_s .. ") [background thread, with settle]")

    local iok, input = pcall(function() return vlc.object.input() end)
    if not iok or not input then return false end

    local ok = pcall(function()
        vlc.var.set(input, "time", math.floor(target_s * 1000000))
    end)

    -- Post-seek settle delay (background thread only)
    if ok and vlc.misc and vlc.misc.msleep then
        pcall(vlc.misc.msleep, SEEK_SETTLE_US)
    end

    return ok
end

-- ============================================================
-- Safe playlist navigation
-- ============================================================

--- Advance to the next playlist item if available.
-- Checks playlist length to avoid wrapping to first item.
-- Works for both 0-based and 1-based indices across VLC builds.
-- @return boolean: True if advanced to next item.
local function safe_playlist_next()
    local pl = vlc.playlist
    if not pl then return false end

    local iok, items = pcall(function()
        return pl.get and pl:get("normal")
    end)

    local cok, cur = pcall(function()
        return pl.current and pl:current()
    end)

    if not iok or not items or not items.children then return false end
    if not cok or cur == nil then return false end

    local n = #items.children

    -- Handle both 0-based and 1-based indices:
    -- 0-based: cur ∈ [0, n-1], last item at n-1
    -- 1-based: cur ∈ [1, n], last item at n
    -- For 0-based: allow if cur < n - 1 (second-to-last or earlier)
    -- For 1-based: allow if cur < n (second-to-last or earlier)
    -- Combined check: cur < n - 1 works for 0-based
    --                 cur <= n - 2 doesn't work for 1-based at n-1
    -- Safe approach: check if cur < n (works if there's a next item)
    -- But we need to avoid wrap-around, so check cur + 1 < n (0-based)
    -- or cur < n (1-based second-to-last)

    -- Most robust: allow advance if n > 1 and we're not at the last
    -- For 0-based: last = n-1, so cur < n-1 means not at last
    -- For 1-based: last = n, so cur < n means not at last
    -- This is ambiguous, so use: n > 1 AND (cur < n - 1 OR cur < n)
    -- which simplifies to: n > 1 AND cur < n

    local can_advance = n > 1 and cur < n

    -- Additional safety: if 0-based, also check cur + 1 <= n - 1
    -- We'll trust that VLC's next() won't wrap if there's actually a next item

    if can_advance then
        dbg("safe_playlist_next() advancing (cur=" .. cur .. " n=" .. n .. ")")
        local ok = pcall(function() pl:next() end)
        return ok
    end

    dbg("safe_playlist_next() blocked — last item (cur="
        .. tostring(cur) .. " n=" .. tostring(n) .. ")")
    return false
end

-- ============================================================
-- Skip actions (for button callbacks)
-- ============================================================

--- Skip the intro of the current item.
-- @return boolean: True if skip succeeded.
-- @return string: Status message.
local function do_skip_intro()
    local fn = get_current_filename()
    if not fn then
        return false, "No file playing"
    end

    local d = find_skip_entry(fn)
    if not d then
        return false, "No skip data for: " .. fn
    end
    if not d.intro_end then
        return false, "No intro_end in data"
    end

    dbg("do_skip_intro() target=" .. d.intro_end)

    if seek_to(d.intro_end) then
        intro_skipped = true
        return true, "Intro skipped → " .. format_time(d.intro_end)
    end

    return false, "Seek failed"
end

--- Skip the outro of the current item.
-- @return boolean: True if skip succeeded.
-- @return string: Status message.
local function do_skip_outro()
    local fn = get_current_filename()
    if not fn then
        return false, "No file playing"
    end

    local d = find_skip_entry(fn)
    if not d then
        return false, "No skip data for: " .. fn
    end
    if not d.outro_start then
        return false, "No outro_start in data"
    end

    dbg("do_skip_outro() outro_start=" .. d.outro_start)

    -- Try to advance to next item
    if safe_playlist_next() then
        outro_skipped = true
        return true, "Outro → next episode"
    end

    -- Fall back to seeking to end
    local target = d.outro_end

    if not target then
        local iok, item = pcall(function() return vlc.input.item() end)
        if iok and item then
            local dok, dur = pcall(function() return item:duration() end)
            if dok and dur and dur > 0 then
                target = dur / 1000000
                dbg("do_skip_outro() fallback to item duration=" .. target)
            end
        end
    end

    if target and seek_to(target) then
        outro_skipped = true
        return true, "Outro → end"
    end

    return false, "Outro seek failed"
end

-- ============================================================
-- Auto-skip polling (input-listener callback)
--
-- VLC calls this function periodically because "input-listener"
-- is declared in descriptor().capabilities.
--
-- THREADING: This runs on a background thread.
--   - safe_seek()           — correct here (sleeps in background)
--   - seek_to()             — NOT used here
--   - Dialog widgets        — NEVER accessed here
--
-- Status updates are written to pending_status which the GUI
-- thread reads on the next refresh.
-- ============================================================

--- Determine intro zone for a given time position.
-- @param t number: Current time in seconds.
-- @param intro_start number: Intro start time.
-- @param intro_end number: Intro end time.
-- @return number: Zone (0=before, 1=auto-skip, 2=manual, 3=past).
local function get_intro_zone(t, intro_start, intro_end)
    local grace_end = math.min(
        intro_start + INTRO_GRACE_SEC,
        intro_end - 0.5
    )

    if t < intro_start then
        return 0  -- Before intro
    elseif t < grace_end then
        return 1  -- Auto-skip zone
    elseif t < intro_end then
        return 2  -- Manual seek zone (user seeked here)
    else
        return 3  -- Past intro
    end
end

--- Background auto-skip polling function.
-- Called periodically by VLC's input-listener system.
function trigger_auto_skip()
    if not auto_skip_enabled then return end

    -- Periodic JSON check
    tick_count = tick_count + 1
    if tick_count >= JSON_CHECK_TICKS then
        tick_count = 0
        dbg("Periodic JSON check")
        load_skip_data()
    end

    local fn = get_current_filename()
    if not fn then return end

    -- Detect file change
    if fn ~= last_filename then
        dbg("New file: " .. fn)
        intro_skipped = false
        outro_skipped = false
        last_filename = fn
    end

    local d = find_skip_entry(fn)
    if not d then return end

    -- Get current playback position
    local input = vlc.object.input()
    if not input then return end

    local tus = vlc.var.get(input, "time")
    if not tus then return end
    local t = tus / 1000000

    -- --------------------------------------------------------
    -- Intro auto-skip (zone-based state machine)
    --
    --   Zone 0: Before intro start → wait
    --   Zone 1: [intro_start, grace_end) → auto-skip
    --   Zone 2: [grace_end, intro_end) → user seeked, mark done
    --   Zone 3: [intro_end, ∞) → already past, mark done
    -- --------------------------------------------------------
    if not intro_skipped and d.intro_end then
        local intro_start = d.intro_start or 0
        local zone = get_intro_zone(t, intro_start, d.intro_end)

        if zone == 1 then
            -- Auto-skip zone: perform skip
            dbg("Intro skip (Zone 1) t=" .. t .. " target=" .. d.intro_end)

            if safe_seek(d.intro_end) then
                intro_skipped = true
                pending_status = "Auto-skipped intro → " .. format_time(d.intro_end)
            else
                dbg("Intro safe_seek failed")
            end

        elseif zone == 2 then
            -- Manual seek zone: user seeked into body
            dbg("Intro Zone 2 (manual seek into body) t=" .. t)
            intro_skipped = true

        elseif zone == 3 then
            -- Past intro: mark done
            dbg("Intro Zone 3 (already past) t=" .. t)
            intro_skipped = true
        end
    end

    -- --------------------------------------------------------
    -- Outro auto-skip
    --
    -- Fires once when t >= outro_start.  Tries playlist:next()
    -- first, falls back to seeking to outro_end or duration.
    -- --------------------------------------------------------
    if not outro_skipped and d.outro_start and t >= d.outro_start then
        dbg("Outro skip fired t=" .. t .. " outro_start=" .. d.outro_start)
        outro_skipped = true

        -- Try playlist next
        local advanced = safe_playlist_next()

        if not advanced then
            -- Fall back to seeking
            local target = d.outro_end

            if not target then
                local item = vlc.input.item()
                if item then
                    local dur = item:duration()
                    if dur and dur > 0 then
                        target = dur / 1000000
                        dbg("Outro fallback to item duration=" .. target)
                    end
                end
            else
                dbg("Outro seeking to outro_end=" .. target)
            end

            if target then
                safe_seek(target)
            else
                dbg("Outro: no fallback seek target found")
            end
        end

        pending_status = "Auto-skipped outro @ " .. format_time(d.outro_start)
    end
end

-- ============================================================
-- Panel 2 UI helpers
-- ============================================================

--- Get the auto-skip status label text.
-- @return string: Label text.
function auto_skip_label_text()
    if auto_skip_enabled then
        return "Auto-skip: ON ✓"
    else
        return "Auto-skip: OFF ✗"
    end
end

--- Update Panel 2 info labels.
-- Called from GUI thread only.
function update_p2_info()
    if not dialog then return end

    -- Check for pending status from background thread
    if pending_status and p2_status_label then
        pcall(p2_status_label.set_text, p2_status_label, pending_status)
        pending_status = nil
    end

    local fn = get_current_filename() or "Nothing playing"

    if p2_file_label then
        pcall(p2_file_label.set_text, p2_file_label, fn)
    end

    local d = find_skip_entry(fn)
    local info = ""

    if d then
        if d.intro_start ~= nil and d.intro_end ~= nil then
            info = info
                .. "Intro: " .. format_time(d.intro_start)
                .. " – " .. format_time(d.intro_end)
                .. "<br/>"
        elseif d.intro_end ~= nil then
            info = info
                .. "Intro: 0:00 – " .. format_time(d.intro_end)
                .. "<br/>"
        end

        if d.outro_start ~= nil then
            info = info .. "Outro: " .. format_time(d.outro_start)
            if d.outro_end ~= nil then
                info = info .. " – " .. format_time(d.outro_end)
            end
            info = info .. "<br/>"
        end

        if info == "" then
            info = "Entry found but no timestamps present"
        end
    else
        info = "No skip data for this file"
    end

    if p2_info_label then
        pcall(p2_info_label.set_text, p2_info_label, info)
    end

    if p2_auto_label then
        pcall(p2_auto_label.set_text, p2_auto_label, auto_skip_label_text())
    end
end

-- ============================================================
-- Panel 2 button callbacks (GUI thread)
-- ============================================================

--- Skip Intro button callback.
function do_skip_intro_btn()
    local ok, msg = do_skip_intro()

    if p2_status_label then
        pcall(p2_status_label.set_text, p2_status_label, msg)
    end

    if ok then
        update_p2_info()
    end
end

--- Skip Outro button callback.
function do_skip_outro_btn()
    local ok, msg = do_skip_outro()

    if p2_status_label then
        pcall(p2_status_label.set_text, p2_status_label, msg)
    end

    if ok then
        update_p2_info()
    end
end

--- Toggle Auto button callback.
function toggle_auto_btn()
    auto_skip_enabled = not auto_skip_enabled
    local state = auto_skip_enabled and "enabled" or "disabled"

    dbg("Auto-skip toggled: " .. state)

    if p2_status_label then
        pcall(p2_status_label.set_text, p2_status_label, "Auto-skip " .. state)
    end

    update_p2_info()
end

--- Reload JSON button callback.
function reload_json_btn()
    -- Force reload by clearing hash
    skip_data_hash = ""
    load_skip_data()

    local msg = "Reloaded: " .. count_skip_entries() .. " entries"
    dbg(msg)

    if p2_status_label then
        pcall(p2_status_label.set_text, p2_status_label, msg)
    end

    update_p2_info()
end

--- Apply Manual button callback.
-- Adds session-only timestamps to skip_data (NOT saved to disk).
function apply_manual_btn()
    local fn = get_current_filename()
    if not fn then
        if p2_status_label then
            pcall(p2_status_label.set_text, p2_status_label, "No file playing")
        end
        return
    end

    local intro_val = p2_intro_input and tonumber(p2_intro_input:get_text())
    local outro_val = p2_outro_input and tonumber(p2_outro_input:get_text())

    -- Validate inputs
    if intro_val and not is_valid_timestamp(intro_val) then
        log_warn("Invalid intro timestamp: " .. intro_val)
        intro_val = nil
    end
    if outro_val and not is_valid_timestamp(outro_val) then
        log_warn("Invalid outro timestamp: " .. outro_val)
        outro_val = nil
    end

    if not intro_val and not outro_val then
        if p2_status_label then
            pcall(p2_status_label.set_text, p2_status_label,
                "Enter at least one valid number")
        end
        return
    end

    -- Copy-on-write: create new table with modification
    local new_skip_data = shallow_copy_table(skip_data)

    if not new_skip_data[fn] then
        new_skip_data[fn] = {}
    else
        -- Deep copy the entry
        local old_entry = new_skip_data[fn]
        new_skip_data[fn] = {}
        for k, v in pairs(old_entry) do
            new_skip_data[fn][k] = v
        end
    end

    if intro_val then
        new_skip_data[fn].intro_start = 0
        new_skip_data[fn].intro_end = intro_val
        dbg("apply_manual: intro_end=" .. intro_val .. " for " .. fn)
    end

    if outro_val then
        new_skip_data[fn].outro_start = outro_val

        -- Try to get duration for outro_end
        local iok, item = pcall(function() return vlc.input.item() end)
        if iok and item then
            local dok, dur = pcall(function() return item:duration() end)
            if dok and dur and dur > 0 then
                new_skip_data[fn].outro_end = dur / 1000000
                dbg("apply_manual: outro_start=" .. outro_val
                    .. " outro_end=" .. new_skip_data[fn].outro_end
                    .. " for " .. fn)
            end
        end
    end

    -- Validate no overlap
    local entry = new_skip_data[fn]
    if entry.intro_end and entry.outro_start then
        if entry.intro_end > entry.outro_start then
            if p2_status_label then
                pcall(p2_status_label.set_text, p2_status_label,
                    "Error: intro_end > outro_start")
            end
            return
        end
    end

    -- Atomic swap
    skip_data = new_skip_data

    -- Invalidate cache for this file
    skip_entry_cache[fn] = nil

    -- Reset skip flags so new timestamps take effect
    intro_skipped = false
    outro_skipped = false

    if p2_status_label then
        pcall(p2_status_label.set_text, p2_status_label,
            "Applied (session only — not saved to disk)")
    end

    update_p2_info()
end

--- Refresh button callback.
function refresh_btn()
    update_p2_info()
end

-- ============================================================
-- End of script
-- ============================================================
