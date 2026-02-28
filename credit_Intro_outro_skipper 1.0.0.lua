--[[
Unified Credit & Intro/Outro Skipper for VLC
============================================================
Version 4.0.1

Merges three skip systems into one extension:

  1. CREDIT SKIPPER (original by Michael Bull)
     Profile-based fixed start/stop trimming.
     Profiles saved to: vlc_configdir/credit-skipper.conf
     Works by re-queuing the playlist with start-time / stop-time
     options so the player never plays the trimmed regions.

  2. INTRO/OUTRO SKIPPER (auto-detection companion)
     Reads skip_data.json produced by skip_intro.py and performs
     real-time seeks during playback to jump over detected intros
     and outros.

  3. NETFLIX-STYLE PROMPT SYSTEM (new in v4.0.0)
     Shows a countdown prompt ("Skip Intro (8s)") instead of
     silently seeking.  User can confirm, dismiss, or set an
     "Always Skip" preference per series.

SKIP MODES
  - AUTO:   Silent background skipping (original behavior)
  - PROMPT: Netflix-style button with countdown (new default)
  - OFF:    No automatic skipping; manual buttons only

SEGMENT TYPES SUPPORTED
  - intro:   Opening title sequence
  - recap:   "Previously on..." segment
  - outro:   End credits
  - credits: Rolling credits (alias for outro)

IMPORTANT — Manual timestamp entries (panel 2 Apply button) and
"Always Skip" preferences are SESSION-ONLY.  They are stored in
memory and are NOT written to disk.  They will be lost on Reload
or VLC restart.  For permanent per-episode timestamps run
skip_intro.py with --import-timestamps instead.

JSON FORMAT NOTE — skip_data.json must contain a flat object whose
values are flat objects with numeric leaf values only.  The embedded
parser does not support nested objects, arrays, booleans, or null.

Supported keys per entry:
  {
    "show.s01e01": {
      "intro_start":   23.0,
      "intro_end":     92.0,
      "recap_start":   0.0,
      "recap_end":     18.0,
      "outro_start":   1280.0,
      "outro_end":     1320.0,
      "credits_start": 1290.0
    }
  }

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
  - Prompts are communicated via atomic active_prompt table swap.

CHANGELOG v4.0.1
  - Fixed: Use safe_seek() everywhere for consistent seek behavior
  - Fixed: Timeout now respects series "Always Skip" preference
  - Fixed: Always provide user feedback on prompt timeout
  - Fixed: Set skip flags BEFORE seek to prevent re-fire on failure
  - Fixed: Improved error messages and status feedback
  - Fixed: Defensive flag setting in all skip paths

CHANGELOG v4.0.0
  - Added Netflix-style prompt system with countdown
  - Added skip mode selector (Auto / Prompt / Off)
  - Added "Always Skip" per-series preference memory
  - Added recap segment support
  - Added Panel 3: Now Playing prompt bar
  - Prompts auto-dismiss after timeout (configurable)
  - Background thread sets prompts; GUI thread displays them
  - Full backward compatibility with v3.0.0 behavior (Auto mode)

CHANGELOG v3.0.0
  - Fixed race condition: skip_data now uses copy-on-write
  - Fixed cross-thread GUI access
  - Fixed memory leak in dialog management
  - Fixed UTF-8 surrogate pair handling in JSON parser
  - Added negative lookup cache for find_skip_entry()
  - Added timestamp validation (0-86400 seconds)
  - Added intro/outro overlap detection
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

local SKIPPER_VERSION = "4.0.1"

local _vlc_cfgdir = vlc.config.configdir()

-- Path to skip_data.json written by skip_intro.py.
local SKIP_DATA_FILE = _vlc_cfgdir .. "/intro_skipper/skip_data.json"

-- Credit Skipper profile storage (original format, unchanged).
local CREDIT_CONF_FILE = _vlc_cfgdir .. "/credit-skipper.conf"

-- ============================================================
-- Timing constants
-- ============================================================

-- Settle delay after seeks (microseconds).
-- This ensures the seek completes before the next action.
-- Reduce to 100000 (100ms) for snappier response at the cost
-- of occasional incomplete seeks on slower systems.
local SEEK_SETTLE_US = 500000  -- 500ms (conservative default)

-- How many input-listener ticks before skip_data.json is checked.
-- At VLC's default ~250 ms tick interval this is roughly 15 seconds.
local JSON_CHECK_TICKS = 60

-- Grace window (seconds) after intro_start during which auto-skip fires.
local INTRO_GRACE_SEC = 5

-- Maximum valid timestamp in seconds (24 hours).
local MAX_TIMESTAMP_SEC = 86400

-- ============================================================
-- Prompt system constants (Netflix-style UX)
-- ============================================================

-- How long the prompt stays visible before auto-action (seconds)
local PROMPT_DISPLAY_SEC = 8

-- Segment type identifiers
local SEG_INTRO   = "intro"
local SEG_RECAP   = "recap"
local SEG_OUTRO   = "outro"
local SEG_CREDITS = "credits"

-- Skip mode constants
local SKIP_MODE_AUTO   = 1  -- Silent background skipping (original)
local SKIP_MODE_PROMPT = 2  -- Netflix-style button with countdown
local SKIP_MODE_OFF    = 3  -- No automatic skipping

-- ============================================================
-- Logging helpers
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
            .. "(Intro/Outro Skipper) featuring Netflix-style prompts.",
        capabilities = { "input-listener" }
    }
end

-- ============================================================
-- Shared state (Credit Skipper)
-- ============================================================

local profiles = {}

-- ============================================================
-- Shared state (Intro/Outro Skipper - copy-on-write)
-- ============================================================

local skip_data = {}
local skip_data_hash = ""

-- Lookup cache: maps filename -> entry or false (negative cache)
local skip_entry_cache = {}

-- Filename cache: maps URI -> decoded filename
local filename_cache = {}

-- Per-file skip state
local intro_skipped = false
local outro_skipped = false
local recap_skipped = false
local last_filename = ""
local tick_count = 0

-- Status message from background thread (read by GUI on refresh)
local pending_status = nil

-- ============================================================
-- Prompt engine state (Netflix-style UX)
--
-- Written by background thread, read by GUI thread on refresh.
-- Uses copy-on-write: active_prompt is replaced atomically.
-- ============================================================

-- Current skip mode (defaults to Netflix-style prompts)
local skip_mode = SKIP_MODE_PROMPT

-- Active prompt (set by background, cleared by GUI or timeout)
-- Fields: seg_type, target_s, label, deadline_ticks, fired
local active_prompt = nil

-- Tick counter for prompt deadline (avoids os.clock issues)
local prompt_tick_counter = 0

-- Per-series auto-skip memory (session only)
-- Key = normalized filename stem, Value = true/false
local series_auto_skip_prefs = {}

-- ============================================================
-- Dialog & widget handles
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
local p2_mode_dropdown = nil
local p2_intro_input = nil
local p2_outro_input = nil

-- Panel 3 (Netflix-style prompt bar) widgets
local p3_prompt_bar = nil
local p3_prompt_btn = nil
local p3_dismiss_btn = nil
local p3_countdown_lbl = nil
local p3_remember_btn = nil

-- ============================================================
-- Utility helpers
-- ============================================================

--- Check if a file exists and is readable.
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
local function file_content_hash(path)
    local f = io.open(path, "rb")
    if not f then return "" end
    local content = f:read("*all")
    f:close()
    if not content then return "" end
    local len = #content
    local head = content:sub(1, 64)
    local tail = content:sub(-64)
    return string.format("%d:%s:%s", len, head, tail)
end

--- Shallow copy a table (one level deep for skip_data).
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

--- Format duration in a compact way for buttons.
local function format_duration_short(seconds)
    if not seconds then return "" end
    seconds = math.floor(seconds)
    if seconds < 60 then
        return tostring(seconds) .. "s"
    elseif seconds < 3600 then
        local m = math.floor(seconds / 60)
        local s = seconds % 60
        if s == 0 then
            return tostring(m) .. "m"
        else
            return string.format("%dm%02ds", m, s)
        end
    else
        return format_time(seconds)
    end
end

--- Validate that a timestamp is within acceptable range.
local function is_valid_timestamp(ts)
    return ts ~= nil and ts >= 0 and ts <= MAX_TIMESTAMP_SEC
end

--- Get segment type display name.
local function segment_display_name(seg_type)
    if seg_type == SEG_INTRO then
        return "Intro"
    elseif seg_type == SEG_RECAP then
        return "Recap"
    elseif seg_type == SEG_OUTRO or seg_type == SEG_CREDITS then
        return "Credits"
    else
        return seg_type or "Segment"
    end
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
    active_prompt = nil
    prompt_tick_counter = 0
    series_auto_skip_prefs = {}

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
function input_changed()
    dbg("input_changed() — resetting skip flags")
    intro_skipped = false
    outro_skipped = false
    recap_skipped = false
    active_prompt = nil
    prompt_tick_counter = 0
    update_p2_info()
end

-- ============================================================
-- Dialog management
-- ============================================================

--- Clean up all dialog resources.
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
    p2_mode_dropdown = nil
    p2_intro_input = nil
    p2_outro_input = nil

    -- Panel 3 widgets
    p3_prompt_bar = nil
    p3_prompt_btn = nil
    p3_dismiss_btn = nil
    p3_countdown_lbl = nil
    p3_remember_btn = nil
end

--- Create Panel 1 (Credit Skipper) widgets.
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

    -- Skip mode selector
    dialog:add_label("<b>Skip Mode:</b>", 1, row, 1, 1)
    p2_mode_dropdown = dialog:add_dropdown(2, row, 2, 1)
    p2_mode_dropdown:add_value("Auto (silent skip)", SKIP_MODE_AUTO)
    p2_mode_dropdown:add_value("Prompt (Netflix-style)", SKIP_MODE_PROMPT)
    p2_mode_dropdown:add_value("Off (manual only)", SKIP_MODE_OFF)
    -- Set current selection based on skip_mode
    if skip_mode == SKIP_MODE_AUTO then
        p2_mode_dropdown:set_value(1)
    elseif skip_mode == SKIP_MODE_PROMPT then
        p2_mode_dropdown:set_value(2)
    else
        p2_mode_dropdown:set_value(3)
    end
    dialog:add_button("Apply", apply_mode_btn, 4, row, 1, 1)
    row = row + 1

    dialog:add_button("Skip Intro", do_skip_intro_btn, 1, row, 1, 1)
    dialog:add_button("Skip Outro", do_skip_outro_btn, 2, row, 1, 1)
    dialog:add_button("Skip Recap", do_skip_recap_btn, 3, row, 1, 1)
    dialog:add_button("Reload JSON", reload_json_btn, 4, row, 1, 1)
    row = row + 1

    return row
end

--- Create Panel 3 (Netflix-style prompt bar) widgets.
local function create_prompt_panel(start_row)
    local row = start_row

    dialog:add_label(
        "<hr/><center><b>&#9654; Now Playing</b></center>",
        1, row, 4, 1)
    row = row + 1

    -- Prompt status bar
    p3_prompt_bar = dialog:add_label(
        "<center><i>No active prompt</i></center>",
        1, row, 4, 1)
    row = row + 1

    -- Action buttons
    p3_prompt_btn = dialog:add_button("Skip", confirm_prompt_btn, 1, row, 1, 1)
    p3_dismiss_btn = dialog:add_button("✕ Dismiss", dismiss_prompt_btn, 2, row, 1, 1)
    p3_remember_btn = dialog:add_button("Always Skip", remember_skip_btn, 3, row, 1, 1)
    p3_countdown_lbl = dialog:add_label("", 4, row, 1, 1)
    row = row + 1

    return row
end

--- Create manual override section widgets.
local function create_manual_override(start_row)
    local row = start_row

    dialog:add_label(
        "<hr/><center><b>Manual Override (Session Only)</b>"
            .. "<br/><small>Not saved &#8212; lost on Reload or VLC"
            .. " restart.</small></center>",
        1, row, 4, 1)
    row = row + 1

    dialog:add_label("Intro end (s):", 1, row, 1, 1)
    p2_intro_input = dialog:add_text_input("", 2, row, 1, 1)
    dialog:add_label("Outro start (s):", 3, row, 1, 1)
    p2_outro_input = dialog:add_text_input("", 4, row, 1, 1)
    row = row + 1

    dialog:add_button("Apply Manual", apply_manual_btn, 1, row, 1, 1)
    dialog:add_button("Clear Prefs", clear_prefs_btn, 2, row, 1, 1)
    dialog:add_button("Refresh", refresh_btn, 3, row, 1, 1)
    dialog:add_button("Close", close, 4, row, 1, 1)
    row = row + 1

    return row
end

--- Open the main dialog window.
function open_dialog()
    cleanup_dialog()
    dialog = vlc.dialog(descriptor().title)

    local row = 1
    row = create_credit_panel(row)
    row = create_intro_panel(row)
    row = create_prompt_panel(row)
    row = create_manual_override(row)

    populate_profile_fields()
    update_p2_info()
end

-- ============================================================
-- ============================================================
-- SECTION 1 — CREDIT SKIPPER
-- ============================================================
-- ============================================================

function populate_profile_dropdown()
    if not p1_profile_dropdown then return end

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

function save_profile()
    if not p1_profile_name_input then return end

    local name = p1_profile_name_input:get_text()
    if name == "" then return end

    local start_time = tonumber(p1_start_time_input:get_text()) or 0
    local finish_time = tonumber(p1_finish_time_input:get_text()) or 0

    if start_time < 0 then start_time = 0 end
    if finish_time < 0 then finish_time = 0 end

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
-- ============================================================

local function parse_json_string(str, pos)
    if str:sub(pos, pos) ~= '"' then
        return nil, pos
    end
    pos = pos + 1

    local result = {}

    while pos <= #str do
        local c = str:sub(pos, pos)

        if c == '"' then
            return table.concat(result), pos + 1

        elseif c == '\\' then
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
                local hex = str:sub(pos + 1, pos + 4)
                local cp = tonumber(hex, 16)

                if not cp then
                    log_err("Invalid \\u escape at position " .. pos)
                    return nil, pos
                end

                pos = pos + 4

                -- Handle surrogate pairs
                if cp >= 0xD800 and cp <= 0xDBFF then
                    if str:sub(pos + 1, pos + 2) == '\\u' then
                        local hex2 = str:sub(pos + 3, pos + 6)
                        local cp2 = tonumber(hex2, 16)

                        if cp2 and cp2 >= 0xDC00 and cp2 <= 0xDFFF then
                            cp = 0x10000 + ((cp - 0xD800) * 0x400) + (cp2 - 0xDC00)
                            pos = pos + 6
                        end
                    end
                end

                -- Encode as UTF-8
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
                    table.insert(result, string.char(
                        0xF0 + math.floor(cp / 0x40000),
                        0x80 + (math.floor(cp / 0x1000) % 0x40),
                        0x80 + (math.floor(cp / 0x40) % 0x40),
                        0x80 + (cp % 0x40)))
                end
            else
                table.insert(result, esc)
            end
        else
            table.insert(result, c)
        end

        pos = pos + 1
    end

    return nil, pos
end

local function parse_json_number(str, pos)
    local start = pos

    if str:sub(pos, pos) == '-' then
        pos = pos + 1
    end

    while pos <= #str and str:sub(pos, pos):match('[0-9]') do
        pos = pos + 1
    end

    if pos <= #str and str:sub(pos, pos) == '.' then
        pos = pos + 1
        while pos <= #str and str:sub(pos, pos):match('[0-9]') do
            pos = pos + 1
        end
    end

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

    if num and not is_valid_timestamp(num) then
        log_warn("Timestamp out of range: " .. num)
        return nil, pos
    end

    return num, pos
end

local function skip_ws(str, pos)
    while pos <= #str and str:sub(pos, pos):match('%s') do
        pos = pos + 1
    end
    return pos
end

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

        if not first then
            if content:sub(pos, pos) == ',' then
                pos = skip_ws(content, pos + 1)
            else
                break
            end
        end
        first = false

        local filename, np = parse_json_string(content, pos)
        if not filename then
            log_err("JSON parse error: expected string key at pos " .. pos)
            break
        end
        pos = skip_ws(content, np)

        if content:sub(pos, pos) ~= ':' then
            log_err("JSON parse error: expected ':' at pos " .. pos)
            break
        end
        pos = skip_ws(content, pos + 1)

        if content:sub(pos, pos) ~= '{' then
            log_err("JSON parse error: expected '{' at pos " .. pos)
            break
        end
        pos = pos + 1

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

            local key, np2 = parse_json_string(content, pos)
            if not key then break end
            pos = skip_ws(content, np2)

            if content:sub(pos, pos) ~= ':' then break end
            pos = skip_ws(content, pos + 1)

            local val, np3 = parse_json_number(content, pos)
            if val ~= nil then
                entry[key] = val
                pos = np3
            else
                while pos <= #content do
                    local c = content:sub(pos, pos)
                    if c == ',' or c == '}' then break end
                    pos = pos + 1
                end
            end
        end

        if filename and next(entry) then
            -- Validate: intro_end should be before outro_start
            if entry.intro_end and entry.outro_start then
                if entry.intro_end > entry.outro_start then
                    log_warn("Overlap: intro_end > outro_start for " .. filename)
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
-- ============================================================

local function normalize_key(name)
    if not name or name == "" then return "" end

    name = name:gsub("%%(%x%x)", function(h)
        return string.char(tonumber(h, 16))
    end)
    name = name:gsub("%.[^%.]+$", "")
    name = name:lower()
    name = name:gsub("\xe2\x80\x93", ".")
    name = name:gsub("\xe2\x80\x94", ".")
    name = name:gsub("[ \t_%-.,:]+", ".")
    name = name:gsub("[^a-z0-9.]+", "")
    name = name:gsub("%.%.+", ".")
    name = name:gsub("^%.+", ""):gsub("%.+$", "")

    return name
end

--- Extract series key from filename (for preferences).
-- Returns a shorter key representing the show (without episode number).
local function get_series_key(fn)
    if not fn then return "" end
    local nk = normalize_key(fn)
    -- Remove common episode patterns
    nk = nk:gsub("s%d+e%d+", "")
    nk = nk:gsub("e%d+", "")
    nk = nk:gsub("%d+x%d+", "")
    nk = nk:gsub("%.+", ".")
    nk = nk:gsub("^%.+", ""):gsub("%.+$", "")
    return nk
end

-- ============================================================
-- JSON data management
-- ============================================================

function load_skip_data()
    local new_hash = file_content_hash(SKIP_DATA_FILE)

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
        skip_data = result
        skip_data_hash = new_hash
        skip_entry_cache = {}
        dbg("Loaded " .. count_skip_entries() .. " entries from JSON")
    else
        log_err("Failed to parse skip_data.json: " .. tostring(result))
    end
end

function count_skip_entries()
    local c = 0
    for _ in pairs(skip_data) do
        c = c + 1
    end
    return c
end

local function find_skip_entry(fn)
    if not fn or fn == "" then return nil end

    local cached = skip_entry_cache[fn]
    if cached ~= nil then
        if cached == false then
            return nil
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

    -- L3: Full scan
    if nfn ~= "" then
        for key, val in pairs(skip_data) do
            if normalize_key(key) == nfn then
                dbg("find_skip_entry: L3 scan hit key=" .. key)
                skip_entry_cache[fn] = val
                return val
            end
        end
    end

    skip_entry_cache[fn] = false
    return nil
end

-- ============================================================
-- Filename extraction
-- ============================================================

function get_current_filename()
    local iok, item = pcall(function() return vlc.input.item() end)
    if not iok or not item then return nil end

    local uok, uri = pcall(function() return item:uri() end)
    if not uok or not uri then return nil end

    if filename_cache[uri] then
        return filename_cache[uri]
    end

    local fn = uri:match("([^/\\]+)$")
    if fn then
        fn = fn:gsub("%%(%x%x)", function(h)
            return string.char(tonumber(h, 16))
        end)
        filename_cache[uri] = fn
    end

    return fn
end

-- ============================================================
-- Seek helpers
--
-- safe_seek() is used EVERYWHERE for consistent seek behavior.
-- On GUI thread this causes a brief (~500ms) freeze, but ensures
-- seeks complete reliably without stutter.  This trade-off is
-- acceptable since the user explicitly triggered the action.
--
-- If the freeze is unacceptable, reduce SEEK_SETTLE_US to 100000
-- (100ms) for a balance between responsiveness and reliability.
-- ============================================================

--- Seek to a target time with settle delay.
-- Safe to call from both GUI and background threads.
-- @param target_s number: Target time in seconds.
-- @return boolean: True if seek succeeded.
local function safe_seek(target_s)
    if not target_s or target_s < 0 then return false end

    dbg("safe_seek(" .. target_s .. ")")

    local iok, input = pcall(function() return vlc.object.input() end)
    if not iok or not input then return false end

    local ok = pcall(function()
        vlc.var.set(input, "time", math.floor(target_s * 1000000))
    end)

    -- Settle delay ensures seek completes before next action
    if ok and vlc.misc and vlc.misc.msleep then
        pcall(vlc.misc.msleep, SEEK_SETTLE_US)
    end

    return ok
end

-- ============================================================
-- Safe playlist navigation
-- ============================================================

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

    local can_advance = n > 1 and cur < n

    if can_advance then
        dbg("safe_playlist_next() advancing (cur=" .. cur .. " n=" .. n .. ")")
        local ok = pcall(function() pl:next() end)
        return ok
    end

    dbg("safe_playlist_next() blocked — last item")
    return false
end

-- ============================================================
-- Intro zone detection
-- ============================================================

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
        return 2  -- Manual seek zone
    else
        return 3  -- Past intro
    end
end

-- ============================================================
-- ============================================================
-- SECTION 3 — NETFLIX-STYLE PROMPT ENGINE
-- ============================================================
-- ============================================================

--- Check if series has auto-skip preference.
local function series_prefers_skip(fn)
    local key = get_series_key(fn)
    if key == "" then return false end
    return series_auto_skip_prefs[key] == true
end

--- Clear the active prompt safely.
-- Idempotent: safe to call multiple times.
local function clear_prompt()
    if active_prompt then
        dbg("clear_prompt: " .. tostring(active_prompt.seg_type))
    end
    active_prompt = nil
end

--- Set an active prompt from the background thread.
-- @param seg_type string: Segment type constant.
-- @param target_s number|nil: Seek target (nil = next episode).
-- @param label string: Button label.
local function set_prompt(seg_type, target_s, label)
    -- Don't re-fire if same segment type is already prompting
    if active_prompt and active_prompt.seg_type == seg_type then
        return
    end

    -- Calculate deadline in ticks (not wall time)
    local deadline_ticks = prompt_tick_counter +
        math.floor(PROMPT_DISPLAY_SEC * 4)  -- ~4 ticks/sec

    -- Atomic table replacement
    active_prompt = {
        seg_type       = seg_type,
        target_s       = target_s,
        label          = label,
        deadline_ticks = deadline_ticks,
        fired          = false,
    }

    dbg("set_prompt: " .. seg_type .. " target=" .. tostring(target_s)
        .. " deadline_ticks=" .. deadline_ticks)

    pending_status = "⏭ " .. label
end

-- ============================================================
-- Skip actions (manual button callbacks)
--
-- All use safe_seek() for consistency.  The brief freeze is
-- acceptable since the user explicitly clicked the button.
--
-- IMPORTANT: Set skip flags BEFORE seek attempt to prevent
-- re-fire even if seek fails.
-- ============================================================

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

    -- Mark BEFORE seek to prevent re-fire if seek fails
    intro_skipped = true
    clear_prompt()

    if safe_seek(d.intro_end) then
        return true, "Intro skipped → " .. format_time(d.intro_end)
    end

    return false, "Seek failed (intro marked as handled)"
end

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

    -- Mark BEFORE action to prevent re-fire
    outro_skipped = true
    clear_prompt()

    -- Try to advance to next episode first
    if safe_playlist_next() then
        return true, "Outro → next episode"
    end

    -- Fallback: seek to outro_end or duration
    local target = d.outro_end
    if not target then
        local iok, item = pcall(function() return vlc.input.item() end)
        if iok and item then
            local dok, dur = pcall(function() return item:duration() end)
            if dok and dur and dur > 0 then
                target = dur / 1000000
            end
        end
    end

    if target and safe_seek(target) then
        return true, "Outro → end @ " .. format_time(target)
    end

    return false, "Outro action failed (marked as handled)"
end

local function do_skip_recap()
    local fn = get_current_filename()
    if not fn then
        return false, "No file playing"
    end

    local d = find_skip_entry(fn)
    if not d then
        return false, "No skip data for: " .. fn
    end
    if not d.recap_end then
        return false, "No recap_end in data"
    end

    dbg("do_skip_recap() target=" .. d.recap_end)

    -- Mark BEFORE seek to prevent re-fire
    recap_skipped = true
    clear_prompt()

    if safe_seek(d.recap_end) then
        return true, "Recap skipped → " .. format_time(d.recap_end)
    end

    return false, "Seek failed (recap marked as handled)"
end

-- ============================================================
-- Auto-skip polling (input-listener callback)
-- ============================================================

function trigger_auto_skip()
    if skip_mode == SKIP_MODE_OFF then return end

    -- Increment tick counter for prompt deadlines
    prompt_tick_counter = prompt_tick_counter + 1

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
        recap_skipped = false
        active_prompt = nil
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

    -- Determine if we should auto-act (silent skip)
    local auto_mode = (skip_mode == SKIP_MODE_AUTO) or series_prefers_skip(fn)

    -- --------------------------------------------------------
    -- RECAP detection
    -- --------------------------------------------------------
    if not recap_skipped and d.recap_end then
        local recap_start = d.recap_start or 0
        if t >= recap_start and t < d.recap_end then
            if auto_mode then
                -- Set flag BEFORE seek
                recap_skipped = true
                dbg("Auto-skipping recap → " .. d.recap_end)
                if safe_seek(d.recap_end) then
                    pending_status = "Auto-skipped recap → " .. format_time(d.recap_end)
                else
                    pending_status = "Recap skip failed (marked as handled)"
                end
            elseif skip_mode == SKIP_MODE_PROMPT then
                local dur = d.recap_end - recap_start
                set_prompt(SEG_RECAP, d.recap_end,
                    "Skip Recap (" .. format_duration_short(dur) .. ")")
            end
        elseif t >= d.recap_end then
            recap_skipped = true
            if active_prompt and active_prompt.seg_type == SEG_RECAP then
                clear_prompt()
            end
        end
    end

    -- --------------------------------------------------------
    -- INTRO detection
    -- --------------------------------------------------------
    if not intro_skipped and d.intro_end then
        local intro_start = d.intro_start or 0
        local zone = get_intro_zone(t, intro_start, d.intro_end)

        if zone == 1 then
            if auto_mode then
                -- Set flag BEFORE seek
                intro_skipped = true
                dbg("Auto-skip intro → " .. d.intro_end)
                if safe_seek(d.intro_end) then
                    pending_status = "Auto-skipped intro → " .. format_time(d.intro_end)
                else
                    pending_status = "Intro skip failed (marked as handled)"
                end
            elseif skip_mode == SKIP_MODE_PROMPT then
                local dur = d.intro_end - intro_start
                set_prompt(SEG_INTRO, d.intro_end,
                    "Skip Intro (" .. format_duration_short(dur) .. ")")
            end
        elseif zone >= 2 then
            intro_skipped = true
            if active_prompt and active_prompt.seg_type == SEG_INTRO then
                clear_prompt()
            end
        end
    end

    -- --------------------------------------------------------
    -- OUTRO detection
    -- --------------------------------------------------------
    if not outro_skipped and d.outro_start and t >= d.outro_start then
        if auto_mode then
            -- Set flag BEFORE action
            outro_skipped = true
            local advanced = safe_playlist_next()
            if not advanced then
                local target = d.outro_end
                if target then
                    safe_seek(target)
                end
            end
            pending_status = "Auto-skipped outro @ " .. format_time(d.outro_start)
        elseif skip_mode == SKIP_MODE_PROMPT then
            set_prompt(SEG_OUTRO, nil, "Next Episode →")
        end
    end

    -- --------------------------------------------------------
    -- Prompt timeout handling
    --
    -- On timeout:
    --   - If series has "Always Skip" pref OR mode is AUTO:
    --     → Auto-act (seek/next) and notify user
    --   - Otherwise:
    --     → Just dismiss and mark segment as handled
    --
    -- Set skip flags BEFORE attempting seek to prevent re-fire.
    -- --------------------------------------------------------
    if active_prompt and not active_prompt.fired then
        if prompt_tick_counter >= active_prompt.deadline_ticks then
            local seg = active_prompt.seg_type
            local tgt = active_prompt.target_s

            dbg("Prompt timed out: " .. seg)
            active_prompt.fired = true

            -- Set skip flags BEFORE any action to prevent re-fire
            if seg == SEG_INTRO then intro_skipped = true end
            if seg == SEG_RECAP then recap_skipped = true end
            if seg == SEG_OUTRO then outro_skipped = true end

            clear_prompt()

            -- Check if we should auto-act on timeout
            -- series_prefers_skip takes priority
            local should_auto_act = series_prefers_skip(fn) or auto_mode

            if should_auto_act then
                -- Perform the skip action
                if tgt then
                    if safe_seek(tgt) then
                        pending_status = "Auto-skipped " ..
                            segment_display_name(seg) ..
                            " → " .. format_time(tgt) .. " (timeout)"
                    else
                        pending_status = segment_display_name(seg) ..
                            " skip failed (timeout)"
                    end
                else
                    -- nil target = advance to next episode
                    if safe_playlist_next() then
                        pending_status = "Auto → next episode (timeout)"
                    else
                        -- Fallback: seek to end of current item
                        local item = vlc.input.item()
                        if item then
                            local dur = item:duration()
                            if dur and dur > 0 then
                                if safe_seek(dur / 1000000) then
                                    pending_status = "Auto → end (timeout)"
                                else
                                    pending_status = "Timeout — no next episode"
                                end
                            else
                                pending_status = "Timeout — no next episode"
                            end
                        else
                            pending_status = "Timeout — no next episode"
                        end
                    end
                end
            else
                -- User didn't set "Always Skip" and mode is PROMPT
                -- Just dismiss the prompt without seeking
                pending_status = "Prompt timed out — " ..
                    segment_display_name(seg) .. " not skipped"
            end
        end
    end
end

-- ============================================================
-- Prompt bar UI refresh
-- ============================================================

local function refresh_prompt_bar()
    if not p3_prompt_bar then return end

    if not active_prompt or active_prompt.fired then
        pcall(p3_prompt_bar.set_text, p3_prompt_bar,
            "<center><i>No active prompt</i></center>")
        if p3_countdown_lbl then
            pcall(p3_countdown_lbl.set_text, p3_countdown_lbl, "")
        end
        if p3_prompt_btn then
            pcall(p3_prompt_btn.set_text, p3_prompt_btn, "Skip")
        end
        return
    end

    -- Compute remaining time
    local remaining_ticks = math.max(0,
        active_prompt.deadline_ticks - prompt_tick_counter)
    local remaining_sec = math.ceil(remaining_ticks / 4)  -- ~4 ticks/sec

    -- Update prompt bar
    local bar_html = string.format(
        "<center><b>%s</b> &nbsp; <small>(%ds)</small></center>",
        active_prompt.label, remaining_sec)
    pcall(p3_prompt_bar.set_text, p3_prompt_bar, bar_html)

    -- Update skip button label
    if p3_prompt_btn then
        local btn_label = "Skip " .. segment_display_name(active_prompt.seg_type)
        pcall(p3_prompt_btn.set_text, p3_prompt_btn, btn_label)
    end

    -- Update countdown
    if p3_countdown_lbl then
        pcall(p3_countdown_lbl.set_text, p3_countdown_lbl,
            tostring(remaining_sec) .. "s")
    end
end

-- ============================================================
-- Panel 2 & 3 UI helpers
-- ============================================================

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
        if d.recap_start ~= nil or d.recap_end ~= nil then
            local rs = d.recap_start or 0
            local re = d.recap_end or 0
            info = info .. "Recap: " .. format_time(rs)
                .. " – " .. format_time(re) .. "<br/>"
        end

        if d.intro_start ~= nil and d.intro_end ~= nil then
            info = info .. "Intro: " .. format_time(d.intro_start)
                .. " – " .. format_time(d.intro_end) .. "<br/>"
        elseif d.intro_end ~= nil then
            info = info .. "Intro: 0:00 – " .. format_time(d.intro_end)
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
            info = "Entry found but no timestamps"
        end

        -- Show series preference status
        local series_key = get_series_key(fn)
        if series_key ~= "" and series_auto_skip_prefs[series_key] then
            info = info .. "<i>Always Skip: enabled</i>"
        end
    else
        info = "No skip data for this file"
    end

    if p2_info_label then
        pcall(p2_info_label.set_text, p2_info_label, info)
    end

    -- Update prompt bar
    refresh_prompt_bar()
end

-- ============================================================
-- Button callbacks (GUI thread)
-- ============================================================

function do_skip_intro_btn()
    local ok, msg = do_skip_intro()
    if p2_status_label then
        pcall(p2_status_label.set_text, p2_status_label, msg)
    end
    update_p2_info()
end

function do_skip_outro_btn()
    local ok, msg = do_skip_outro()
    if p2_status_label then
        pcall(p2_status_label.set_text, p2_status_label, msg)
    end
    update_p2_info()
end

function do_skip_recap_btn()
    local ok, msg = do_skip_recap()
    if p2_status_label then
        pcall(p2_status_label.set_text, p2_status_label, msg)
    end
    update_p2_info()
end

function reload_json_btn()
    skip_data_hash = ""
    load_skip_data()

    local msg = "Reloaded: " .. count_skip_entries() .. " entries"
    dbg(msg)

    if p2_status_label then
        pcall(p2_status_label.set_text, p2_status_label, msg)
    end

    update_p2_info()
end

function apply_mode_btn()
    if not p2_mode_dropdown then return end

    local mode_val = p2_mode_dropdown:get_value()
    if mode_val == 1 then
        skip_mode = SKIP_MODE_AUTO
    elseif mode_val == 2 then
        skip_mode = SKIP_MODE_PROMPT
    else
        skip_mode = SKIP_MODE_OFF
    end

    local mode_names = {
        [SKIP_MODE_AUTO] = "Auto (silent)",
        [SKIP_MODE_PROMPT] = "Prompt (Netflix-style)",
        [SKIP_MODE_OFF] = "Off (manual only)",
    }

    if p2_status_label then
        pcall(p2_status_label.set_text, p2_status_label,
            "Mode: " .. (mode_names[skip_mode] or "Unknown"))
    end

    dbg("Skip mode changed to: " .. tostring(skip_mode))
    update_p2_info()
end

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

    if intro_val and not is_valid_timestamp(intro_val) then
        intro_val = nil
    end
    if outro_val and not is_valid_timestamp(outro_val) then
        outro_val = nil
    end

    if not intro_val and not outro_val then
        if p2_status_label then
            pcall(p2_status_label.set_text, p2_status_label,
                "Enter at least one valid number")
        end
        return
    end

    -- Copy-on-write
    local new_skip_data = shallow_copy_table(skip_data)

    if not new_skip_data[fn] then
        new_skip_data[fn] = {}
    else
        local old_entry = new_skip_data[fn]
        new_skip_data[fn] = {}
        for k, v in pairs(old_entry) do
            new_skip_data[fn][k] = v
        end
    end

    if intro_val then
        new_skip_data[fn].intro_start = 0
        new_skip_data[fn].intro_end = intro_val
    end

    if outro_val then
        new_skip_data[fn].outro_start = outro_val
        local iok, item = pcall(function() return vlc.input.item() end)
        if iok and item then
            local dok, dur = pcall(function() return item:duration() end)
            if dok and dur and dur > 0 then
                new_skip_data[fn].outro_end = dur / 1000000
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

    skip_data = new_skip_data
    skip_entry_cache[fn] = nil

    intro_skipped = false
    outro_skipped = false
    recap_skipped = false
    clear_prompt()

    if p2_status_label then
        pcall(p2_status_label.set_text, p2_status_label,
            "Applied (session only)")
    end

    update_p2_info()
end

function clear_prefs_btn()
    series_auto_skip_prefs = {}

    if p2_status_label then
        pcall(p2_status_label.set_text, p2_status_label,
            "All \"Always Skip\" preferences cleared")
    end

    update_p2_info()
end

function refresh_btn()
    update_p2_info()
end

-- ============================================================
-- Panel 3 prompt button callbacks
-- ============================================================

function confirm_prompt_btn()
    if not active_prompt or active_prompt.fired then
        if p2_status_label then
            pcall(p2_status_label.set_text, p2_status_label,
                "No active prompt to confirm")
        end
        return
    end

    local seg = active_prompt.seg_type
    local tgt = active_prompt.target_s

    -- Set skip flags BEFORE any action to prevent re-fire
    if seg == SEG_INTRO then intro_skipped = true end
    if seg == SEG_RECAP then recap_skipped = true end
    if seg == SEG_OUTRO then outro_skipped = true end

    -- Mark as handled
    active_prompt.fired = true
    clear_prompt()

    if tgt then
        -- Seek to target timestamp
        if safe_seek(tgt) then
            if p2_status_label then
                pcall(p2_status_label.set_text, p2_status_label,
                    "Skipped " .. segment_display_name(seg)
                    .. " → " .. format_time(tgt))
            end
        else
            if p2_status_label then
                pcall(p2_status_label.set_text, p2_status_label,
                    "Seek failed for " .. segment_display_name(seg)
                    .. " (marked as handled)")
            end
        end
    else
        -- nil target = next episode
        if safe_playlist_next() then
            if p2_status_label then
                pcall(p2_status_label.set_text, p2_status_label,
                    "Next episode →")
            end
        else
            -- Fallback: seek to end of current item
            local iok, item = pcall(function() return vlc.input.item() end)
            if iok and item then
                local dok, dur = pcall(function() return item:duration() end)
                if dok and dur and dur > 0 then
                    if safe_seek(dur / 1000000) then
                        if p2_status_label then
                            pcall(p2_status_label.set_text, p2_status_label,
                                "Skipped to end")
                        end
                    else
                        if p2_status_label then
                            pcall(p2_status_label.set_text, p2_status_label,
                                "Seek to end failed")
                        end
                    end
                else
                    if p2_status_label then
                        pcall(p2_status_label.set_text, p2_status_label,
                            "No next episode available")
                    end
                end
            else
                if p2_status_label then
                    pcall(p2_status_label.set_text, p2_status_label,
                        "No next episode available")
                end
            end
        end
    end

    update_p2_info()
end

function dismiss_prompt_btn()
    if not active_prompt or active_prompt.fired then
        if p2_status_label then
            pcall(p2_status_label.set_text, p2_status_label,
                "No active prompt to dismiss")
        end
        return
    end

    local seg = active_prompt.seg_type

    -- Set skip flags BEFORE clearing to prevent re-fire
    if seg == SEG_INTRO then intro_skipped = true end
    if seg == SEG_RECAP then recap_skipped = true end
    if seg == SEG_OUTRO then outro_skipped = true end

    active_prompt.fired = true
    clear_prompt()

    if p2_status_label then
        pcall(p2_status_label.set_text, p2_status_label,
            "Dismissed — won't skip " .. segment_display_name(seg))
    end

    update_p2_info()
end

function remember_skip_btn()
    local fn = get_current_filename()
    if not fn then
        if p2_status_label then
            pcall(p2_status_label.set_text, p2_status_label,
                "No file playing")
        end
        return
    end

    local series_key = get_series_key(fn)
    if series_key == "" then
        if p2_status_label then
            pcall(p2_status_label.set_text, p2_status_label,
                "Could not determine series from filename")
        end
        return
    end

    series_auto_skip_prefs[series_key] = true

    dbg("Series auto-skip preference saved: " .. series_key)

    -- Immediately confirm the pending prompt if one exists
    if active_prompt and not active_prompt.fired then
        if p2_status_label then
            pcall(p2_status_label.set_text, p2_status_label,
                "Always Skip enabled for: " .. series_key)
        end
        confirm_prompt_btn()
    else
        if p2_status_label then
            pcall(p2_status_label.set_text, p2_status_label,
                "Always Skip enabled for: " .. series_key)
        end
        update_p2_info()
    end
end

-- ============================================================
-- End of script
-- ============================================================
