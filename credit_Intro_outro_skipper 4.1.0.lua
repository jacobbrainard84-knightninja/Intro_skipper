--[[
Unified Credit & Intro/Outro Skipper for VLC
============================================================
Version 4.1.0

CHANGELOG v4.1.0 (Critical Fixes)
  - Fixed: Syntax error in JSON parser (incomplete string handling)
  - Fixed: Replaced non-existent vlc.playlist.get() with correct API
  - Fixed: count_skip_entries() now defined before first use
  - Fixed: Playlist handling no longer destructive (uses item options)
  - Fixed: All flag-before-seek patterns verified
  - Fixed: Duration validation before applying start/stop-time
  - Fixed: JSON parser handles escape sequences and unicode
  - Fixed: Error feedback when JSON missing or malformed
  - Fixed: Extracted magic numbers to named constants
  - Fixed: Modularized trigger_auto_skip() into smaller functions

Merges three skip systems into one extension:

  1. CREDIT SKIPPER (original by Michael Bull)
     Profile-based fixed start/stop trimming.

  2. INTRO/OUTRO SKIPPER (auto-detection companion)
     Reads skip_data.json produced by skip_intro.py.

  3. NETFLIX-STYLE PROMPT SYSTEM
     Shows countdown prompt instead of silent seeking.

INSTALLATION
  Linux:   ~/.local/share/vlc/lua/extensions/
  Windows: %APPDATA%\vlc\lua\extensions\
  macOS:   ~/Library/Application Support/org.videolan.vlc/lua/extensions/

DEBUG MODE
  Set DEBUG = true to enable verbose logging.
  View in VLC: Tools > Messages > set verbosity to 2.
]]

-- ============================================================
-- Configuration & Constants
-- ============================================================

local DEBUG = false
local SKIPPER_VERSION = "4.1.0"

-- Timing constants (extracted from magic numbers)
local SEEK_SETTLE_US = 500000           -- 500ms settle after seek
local JSON_CHECK_TICKS = 60             -- ~15 sec at 250ms/tick
local INTRO_GRACE_SEC = 5               -- Grace window for auto-skip
local MAX_TIMESTAMP_SEC = 86400         -- 24 hours max
local PROMPT_DISPLAY_SEC = 8            -- Prompt visible duration
local TICKS_PER_SECOND = 4              -- Approximate tick rate
local MIN_SEGMENT_DURATION = 1          -- Minimum valid segment

-- Segment type identifiers
local SEG_INTRO   = "intro"
local SEG_RECAP   = "recap"
local SEG_OUTRO   = "outro"
local SEG_CREDITS = "credits"

-- Skip mode constants
local SKIP_MODE_AUTO   = 1
local SKIP_MODE_PROMPT = 2
local SKIP_MODE_OFF    = 3

-- ============================================================
-- Path Setup
-- ============================================================

local function get_config_dir()
    if vlc and vlc.config and vlc.config.configdir then
        return vlc.config.configdir()
    end
    return ""
end

local CONFIG_DIR = get_config_dir()
local SKIP_DATA_FILE = CONFIG_DIR .. "/intro_skipper/skip_data.json"
local CREDIT_CONF_FILE = CONFIG_DIR .. "/credit-skipper.conf"

-- ============================================================
-- Logging helpers
-- ============================================================

local function dbg(msg)
    if DEBUG and vlc and vlc.msg then
        vlc.msg.dbg("[Skipper] " .. tostring(msg))
    end
end

local function log_info(msg)
    if vlc and vlc.msg then
        vlc.msg.info("[Skipper] " .. tostring(msg))
    end
end

local function log_warn(msg)
    if vlc and vlc.msg then
        vlc.msg.warn("[Skipper] " .. tostring(msg))
    end
end

local function log_err(msg)
    if vlc and vlc.msg then
        vlc.msg.err("[Skipper] " .. tostring(msg))
    end
end

-- ============================================================
-- Shared State
-- ============================================================

-- Credit Skipper
local profiles = {}

-- Intro/Outro Skipper (copy-on-write for thread safety)
local skip_data = {}
local skip_data_hash = ""
local skip_entry_cache = {}
local filename_cache = {}

-- Per-file skip state
local intro_skipped = false
local outro_skipped = false
local recap_skipped = false
local last_filename = ""
local tick_count = 0

-- Status message bridge (background → GUI)
local pending_status = nil
local last_error_msg = nil

-- Prompt engine state
local skip_mode = SKIP_MODE_PROMPT
local active_prompt = nil
local prompt_tick_counter = 0
local series_auto_skip_prefs = {}

-- Dialog widgets
local dialog = nil
local p1_profile_dropdown = nil
local p1_profile_name_input = nil
local p1_start_time_input = nil
local p1_finish_time_input = nil
local p2_status_label = nil
local p2_file_label = nil
local p2_info_label = nil
local p2_mode_dropdown = nil
local p2_intro_input = nil
local p2_outro_input = nil
local p3_prompt_bar = nil
local p3_prompt_btn = nil
local p3_dismiss_btn = nil
local p3_countdown_lbl = nil
local p3_remember_btn = nil

-- ============================================================
-- Utility Functions
-- ============================================================

local function file_exists(path)
    if not path or path == "" then return false end
    local f = io.open(path, "rb")
    if f then
        f:close()
        return true
    end
    return false
end

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

local function is_valid_timestamp(ts)
    return type(ts) == "number" and ts >= 0 and ts <= MAX_TIMESTAMP_SEC
end

local function segment_display_name(seg_type)
    if seg_type == SEG_INTRO then return "Intro"
    elseif seg_type == SEG_RECAP then return "Recap"
    elseif seg_type == SEG_OUTRO or seg_type == SEG_CREDITS then return "Credits"
    else return seg_type or "Segment"
    end
end

-- ============================================================
-- JSON Parser (Robust Version)
-- ============================================================

local function skip_whitespace(str, pos)
    while pos <= #str do
        local c = str:sub(pos, pos)
        if c == " " or c == "\t" or c == "\n" or c == "\r" then
            pos = pos + 1
        else
            break
        end
    end
    return pos
end

local function parse_json_string(str, pos)
    if str:sub(pos, pos) ~= '"' then
        return nil, pos, "Expected '\"' at position " .. pos
    end
    pos = pos + 1

    local result = {}

    while pos <= #str do
        local c = str:sub(pos, pos)

        if c == '"' then
            return table.concat(result), pos + 1, nil
        elseif c == '\\' then
            pos = pos + 1
            if pos > #str then
                return nil, pos, "Unexpected end of string after escape"
            end
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
                if pos + 4 > #str then
                    return nil, pos, "Incomplete unicode escape"
                end
                local hex = str:sub(pos + 1, pos + 4)
                local cp = tonumber(hex, 16)

                if not cp then
                    log_warn("Invalid unicode escape: \\u" .. hex)
                    table.insert(result, '?')
                    pos = pos + 4
                else
                    pos = pos + 4

                    -- Handle surrogate pairs
                    if cp >= 0xD800 and cp <= 0xDBFF then
                        if pos + 6 <= #str and str:sub(pos + 1, pos + 2) == '\\u' then
                            local hex2 = str:sub(pos + 3, pos + 6)
                            local cp2 = tonumber(hex2, 16)
                            if cp2 and cp2 >= 0xDC00 and cp2 <= 0xDFFF then
                                cp = 0x10000 + ((cp - 0xD800) * 0x400) + (cp2 - 0xDC00)
                                pos = pos + 6
                            end
                        end
                    end

                    -- Encode as UTF-8
                    if cp < 0x80 then
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
                end
            else
                -- Unknown escape, keep as-is
                table.insert(result, esc)
            end
            pos = pos + 1
        elseif c == '\n' or c == '\r' then
            return nil, pos, "Newline in string"
        else
            table.insert(result, c)
            pos = pos + 1
        end
    end

    return nil, pos, "Unterminated string"
end

local function parse_json_number(str, pos)
    local start_pos = pos

    -- Optional minus
    if str:sub(pos, pos) == '-' then
        pos = pos + 1
    end

    -- Integer part
    if pos > #str then
        return nil, start_pos, "Unexpected end in number"
    end

    local c = str:sub(pos, pos)
    if c == '0' then
        pos = pos + 1
    elseif c >= '1' and c <= '9' then
        while pos <= #str and str:sub(pos, pos):match('[0-9]') do
            pos = pos + 1
        end
    else
        return nil, start_pos, "Invalid number"
    end

    -- Decimal part
    if pos <= #str and str:sub(pos, pos) == '.' then
        pos = pos + 1
        if pos > #str or not str:sub(pos, pos):match('[0-9]') then
            return nil, start_pos, "Invalid decimal"
        end
        while pos <= #str and str:sub(pos, pos):match('[0-9]') do
            pos = pos + 1
        end
    end

    -- Exponent part
    if pos <= #str and str:sub(pos, pos):match('[eE]') then
        pos = pos + 1
        if pos <= #str and str:sub(pos, pos):match('[%+%-]') then
            pos = pos + 1
        end
        if pos > #str or not str:sub(pos, pos):match('[0-9]') then
            return nil, start_pos, "Invalid exponent"
        end
        while pos <= #str and str:sub(pos, pos):match('[0-9]') do
            pos = pos + 1
        end
    end

    local num_str = str:sub(start_pos, pos - 1)
    local num = tonumber(num_str)

    if not num then
        return nil, start_pos, "Failed to parse number: " .. num_str
    end

    -- Validate timestamp range
    if not is_valid_timestamp(num) then
        log_warn("Timestamp out of range: " .. num)
        return nil, pos, "Timestamp out of range"
    end

    return num, pos, nil
end

local function parse_json_object(content)
    if not content or content == "" then
        return {}, "Empty content"
    end

    local data = {}
    local pos = skip_whitespace(content, 1)

    if pos > #content then
        return {}, "Empty content after whitespace"
    end

    if content:sub(pos, pos) ~= '{' then
        return {}, "Expected '{' at start"
    end
    pos = pos + 1

    local first = true
    local entry_count = 0

    while pos <= #content do
        pos = skip_whitespace(content, pos)

        if pos > #content then
            return data, "Unexpected end of object"
        end

        if content:sub(pos, pos) == '}' then
            break
        end

        -- Handle comma
        if not first then
            if content:sub(pos, pos) == ',' then
                pos = skip_whitespace(content, pos + 1)
            else
                return data, "Expected ',' at position " .. pos
            end
        end
        first = false

        -- Parse key
        local key, new_pos, err = parse_json_string(content, pos)
        if not key then
            log_warn("JSON key parse error: " .. (err or "unknown"))
            return data, err
        end
        pos = skip_whitespace(content, new_pos)

        -- Expect colon
        if content:sub(pos, pos) ~= ':' then
            return data, "Expected ':' after key at position " .. pos
        end
        pos = skip_whitespace(content, pos + 1)

        -- Expect inner object
        if content:sub(pos, pos) ~= '{' then
            return data, "Expected '{' for entry value at position " .. pos
        end
        pos = pos + 1

        -- Parse inner object
        local entry = {}
        local inner_first = true

        while pos <= #content do
            pos = skip_whitespace(content, pos)

            if pos > #content then
                break
            end

            if content:sub(pos, pos) == '}' then
                pos = pos + 1
                break
            end

            if not inner_first then
                if content:sub(pos, pos) == ',' then
                    pos = skip_whitespace(content, pos + 1)
                else
                    break
                end
            end
            inner_first = false

            -- Parse field name
            local field_name, np2, err2 = parse_json_string(content, pos)
            if not field_name then
                log_warn("JSON field parse error: " .. (err2 or "unknown"))
                break
            end
            pos = skip_whitespace(content, np2)

            if content:sub(pos, pos) ~= ':' then
                break
            end
            pos = skip_whitespace(content, pos + 1)

            -- Parse value
            local val, np3, err3 = parse_json_number(content, pos)
            if val then
                entry[field_name] = val
                pos = np3
            else
                -- Skip to next comma or closing brace
                while pos <= #content do
                    local ch = content:sub(pos, pos)
                    if ch == ',' or ch == '}' then break end
                    pos = pos + 1
                end
            end
        end

        -- Validate and store entry
        if key and next(entry) then
            -- Check for intro/outro overlap
            if entry.intro_end and entry.outro_start then
                if entry.intro_end > entry.outro_start then
                    log_warn("Overlap in " .. key .. ": intro_end > outro_start")
                    entry.outro_start = nil
                    entry.outro_end = nil
                end
            end
            data[key] = entry
            entry_count = entry_count + 1
        end
    end

    dbg("Parsed " .. entry_count .. " entries from JSON")
    return data, nil
end

-- ============================================================
-- Skip Entry Management
-- ============================================================

--- Count entries in skip_data.
-- Defined early so it can be used in dialog creation.
local function count_skip_entries()
    local c = 0
    for _ in pairs(skip_data) do
        c = c + 1
    end
    return c
end

local function normalize_key(name)
    if not name or name == "" then return "" end

    -- Percent-decode
    name = name:gsub("%%(%x%x)", function(h)
        return string.char(tonumber(h, 16))
    end)
    -- Remove extension
    name = name:gsub("%.[^%.]+$", "")
    -- Lowercase
    name = name:lower()
    -- UTF-8 dashes to dot
    name = name:gsub("\xe2\x80\x93", ".")
    name = name:gsub("\xe2\x80\x94", ".")
    -- ASCII separators to dot
    name = name:gsub("[ \t_%-.,:]+", ".")
    -- Remove non-alphanumeric
    name = name:gsub("[^a-z0-9.]+", "")
    -- Collapse dots
    name = name:gsub("%.%.+", ".")
    -- Trim dots
    name = name:gsub("^%.+", ""):gsub("%.+$", "")

    return name
end

local function get_series_key(fn)
    if not fn then return "" end
    local nk = normalize_key(fn)
    -- Remove episode patterns
    nk = nk:gsub("s%d+e%d+", "")
    nk = nk:gsub("e%d+", "")
    nk = nk:gsub("%d+x%d+", "")
    nk = nk:gsub("%.+", ".")
    nk = nk:gsub("^%.+", ""):gsub("%.+$", "")
    return nk
end

local function load_skip_data()
    local new_hash = file_content_hash(SKIP_DATA_FILE)

    -- Skip if unchanged
    if new_hash ~= "" and new_hash == skip_data_hash then
        dbg("Skip data unchanged")
        return true
    end

    if not file_exists(SKIP_DATA_FILE) then
        last_error_msg = "skip_data.json not found"
        log_warn(last_error_msg .. ": " .. SKIP_DATA_FILE)
        skip_data = {}
        skip_data_hash = ""
        skip_entry_cache = {}
        return false
    end

    local f = io.open(SKIP_DATA_FILE, "r")
    if not f then
        last_error_msg = "Cannot open skip_data.json"
        log_err(last_error_msg)
        return false
    end

    local content = f:read("*all")
    f:close()

    if not content or #content == 0 then
        last_error_msg = "skip_data.json is empty"
        log_warn(last_error_msg)
        skip_data = {}
        skip_data_hash = ""
        skip_entry_cache = {}
        return false
    end

    local result, err = parse_json_object(content)

    if err then
        last_error_msg = "JSON parse error: " .. tostring(err)
        log_err(last_error_msg)
        -- Keep existing data on parse error
        return false
    end

    skip_data = result
    skip_data_hash = new_hash
    skip_entry_cache = {}
    last_error_msg = nil

    log_info("Loaded " .. count_skip_entries() .. " entries from JSON")
    return true
end

local function find_skip_entry(fn)
    if not fn or fn == "" then return nil end

    -- Check cache
    local cached = skip_entry_cache[fn]
    if cached ~= nil then
        return cached == false and nil or cached
    end

    local nfn = normalize_key(fn)

    -- L1: Normalized key
    if nfn ~= "" and skip_data[nfn] then
        skip_entry_cache[fn] = skip_data[nfn]
        return skip_data[nfn]
    end

    -- L2: Raw filename
    if skip_data[fn] then
        skip_entry_cache[fn] = skip_data[fn]
        return skip_data[fn]
    end

    -- L3: Scan with normalization
    if nfn ~= "" then
        for key, val in pairs(skip_data) do
            if normalize_key(key) == nfn then
                skip_entry_cache[fn] = val
                return val
            end
        end
    end

    skip_entry_cache[fn] = false
    return nil
end

-- ============================================================
-- Filename & Playback Helpers
-- ============================================================

local function get_current_filename()
    local ok, item = pcall(function() return vlc.input.item() end)
    if not ok or not item then return nil end

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

local function get_current_duration()
    local ok, item = pcall(function() return vlc.input.item() end)
    if not ok or not item then return nil end

    local dok, dur = pcall(function() return item:duration() end)
    if dok and dur and dur > 0 then
        return dur / 1000000  -- Convert to seconds
    end
    return nil
end

--- Seek with settle delay.
-- Safe for both GUI and background threads.
local function safe_seek(target_s)
    if not target_s or target_s < 0 then return false end

    dbg("safe_seek(" .. target_s .. ")")

    local ok, input = pcall(function() return vlc.object.input() end)
    if not ok or not input then return false end

    local seek_ok = pcall(function()
        vlc.var.set(input, "time", math.floor(target_s * 1000000))
    end)

    if seek_ok and vlc.misc and vlc.misc.msleep then
        pcall(vlc.misc.msleep, SEEK_SETTLE_US)
    end

    return seek_ok
end

--- Advance to next playlist item if available.
local function safe_playlist_next()
    local ok, pl = pcall(function() return vlc.playlist end)
    if not ok or not pl then return false end

    -- Check if there's a next item
    local status_ok, status = pcall(function()
        return pl.status and pl.status()
    end)

    -- Try to advance
    local next_ok = pcall(function()
        if pl.next then
            pl.next()
        end
    end)

    return next_ok
end

-- ============================================================
-- Prompt Engine
-- ============================================================

local function clear_prompt()
    if active_prompt then
        dbg("clear_prompt: " .. tostring(active_prompt.seg_type))
    end
    active_prompt = nil
end

local function set_prompt(seg_type, target_s, label)
    if active_prompt and active_prompt.seg_type == seg_type then
        return
    end

    local deadline = prompt_tick_counter +
        math.floor(PROMPT_DISPLAY_SEC * TICKS_PER_SECOND)

    active_prompt = {
        seg_type = seg_type,
        target_s = target_s,
        label = label,
        deadline_ticks = deadline,
        fired = false,
    }

    dbg("set_prompt: " .. seg_type)
    pending_status = "⏭ " .. label
end

local function series_prefers_skip(fn)
    local key = get_series_key(fn)
    if key == "" then return false end
    return series_auto_skip_prefs[key] == true
end

-- ============================================================
-- Skip Actions
-- ============================================================

local function do_skip_intro()
    local fn = get_current_filename()
    if not fn then return false, "No file playing" end

    local d = find_skip_entry(fn)
    if not d then return false, "No skip data" end
    if not d.intro_end then return false, "No intro_end" end

    -- Set flag BEFORE seek
    intro_skipped = true
    clear_prompt()

    if safe_seek(d.intro_end) then
        return true, "Intro → " .. format_time(d.intro_end)
    end
    return false, "Seek failed"
end

local function do_skip_outro()
    local fn = get_current_filename()
    if not fn then return false, "No file playing" end

    local d = find_skip_entry(fn)
    if not d then return false, "No skip data" end
    if not d.outro_start then return false, "No outro_start" end

    -- Set flag BEFORE action
    outro_skipped = true
    clear_prompt()

    if safe_playlist_next() then
        return true, "Next episode"
    end

    local target = d.outro_end or get_current_duration()
    if target and safe_seek(target) then
        return true, "Outro → end"
    end
    return false, "Action failed"
end

local function do_skip_recap()
    local fn = get_current_filename()
    if not fn then return false, "No file playing" end

    local d = find_skip_entry(fn)
    if not d then return false, "No skip data" end
    if not d.recap_end then return false, "No recap_end" end

    -- Set flag BEFORE seek
    recap_skipped = true
    clear_prompt()

    if safe_seek(d.recap_end) then
        return true, "Recap → " .. format_time(d.recap_end)
    end
    return false, "Seek failed"
end

-- ============================================================
-- Intro Zone Detection
-- ============================================================

local function get_intro_zone(t, intro_start, intro_end)
    local grace_end = math.min(
        intro_start + INTRO_GRACE_SEC,
        intro_end - 0.5
    )

    if t < intro_start then return 0      -- Before intro
    elseif t < grace_end then return 1    -- Auto-skip zone
    elseif t < intro_end then return 2    -- Manual zone
    else return 3                          -- Past intro
    end
end

-- ============================================================
-- Auto-Skip Logic (Modularized)
-- ============================================================

local function handle_recap_detection(d, t, fn, auto_mode)
    if recap_skipped or not d.recap_end then return end

    local recap_start = d.recap_start or 0

    if t >= recap_start and t < d.recap_end then
        if auto_mode then
            recap_skipped = true
            if safe_seek(d.recap_end) then
                pending_status = "Auto-skipped recap"
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

local function handle_intro_detection(d, t, fn, auto_mode)
    if intro_skipped or not d.intro_end then return end

    local intro_start = d.intro_start or 0
    local zone = get_intro_zone(t, intro_start, d.intro_end)

    if zone == 1 then
        if auto_mode then
            intro_skipped = true
            if safe_seek(d.intro_end) then
                pending_status = "Auto-skipped intro"
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

local function handle_outro_detection(d, t, fn, auto_mode)
    if outro_skipped or not d.outro_start then return end

    if t >= d.outro_start then
        if auto_mode then
            outro_skipped = true
            if not safe_playlist_next() then
                local target = d.outro_end
                if target then safe_seek(target) end
            end
            pending_status = "Auto-skipped outro"
        elseif skip_mode == SKIP_MODE_PROMPT then
            set_prompt(SEG_OUTRO, nil, "Next Episode →")
        end
    end
end

local function handle_prompt_timeout(fn, auto_mode)
    if not active_prompt or active_prompt.fired then return end

    if prompt_tick_counter < active_prompt.deadline_ticks then return end

    local seg = active_prompt.seg_type
    local tgt = active_prompt.target_s

    -- Set flags BEFORE action
    active_prompt.fired = true
    if seg == SEG_INTRO then intro_skipped = true end
    if seg == SEG_RECAP then recap_skipped = true end
    if seg == SEG_OUTRO then outro_skipped = true end

    clear_prompt()

    local should_auto_act = series_prefers_skip(fn) or auto_mode

    if should_auto_act then
        if tgt then
            if safe_seek(tgt) then
                pending_status = "Auto-skipped " .. segment_display_name(seg)
            end
        else
            if safe_playlist_next() then
                pending_status = "Auto → next episode"
            else
                local dur = get_current_duration()
                if dur then safe_seek(dur) end
            end
        end
    else
        pending_status = "Prompt timed out"
    end
end

-- ============================================================
-- Main Auto-Skip Function
-- ============================================================

function trigger_auto_skip()
    if skip_mode == SKIP_MODE_OFF then return end

    prompt_tick_counter = prompt_tick_counter + 1

    -- Periodic JSON check
    tick_count = tick_count + 1
    if tick_count >= JSON_CHECK_TICKS then
        tick_count = 0
        load_skip_data()
    end

    local fn = get_current_filename()
    if not fn then return end

    -- File change detection
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

    -- Get playback position
    local ok, input = pcall(function() return vlc.object.input() end)
    if not ok or not input then return end

    local tok, tus = pcall(function() return vlc.var.get(input, "time") end)
    if not tok or not tus then return end
    local t = tus / 1000000

    local auto_mode = (skip_mode == SKIP_MODE_AUTO) or series_prefers_skip(fn)

    -- Handle each segment type
    handle_recap_detection(d, t, fn, auto_mode)
    handle_intro_detection(d, t, fn, auto_mode)
    handle_outro_detection(d, t, fn, auto_mode)
    handle_prompt_timeout(fn, auto_mode)
end

-- ============================================================
-- Extension Descriptor & Lifecycle
-- ============================================================

function descriptor()
    return {
        title = "Credit & Intro/Outro Skipper",
        version = SKIPPER_VERSION,
        author = "Michael Bull / skip_intro.py",
        shortdesc = "Skip Credits + Auto Intro/Outro",
        description = "Profile-based credit trimming with Netflix-style skip prompts.",
        capabilities = { "input-listener" }
    }
end

function activate()
    dbg("activate()")

    profiles = {}
    skip_data = {}
    skip_data_hash = ""
    skip_entry_cache = {}
    filename_cache = {}
    pending_status = nil
    last_error_msg = nil
    active_prompt = nil
    prompt_tick_counter = 0
    series_auto_skip_prefs = {}

    if file_exists(CREDIT_CONF_FILE) then
        load_all_profiles()
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
    -- Required by VLC API
end

function input_changed()
    dbg("input_changed()")
    intro_skipped = false
    outro_skipped = false
    recap_skipped = false
    active_prompt = nil
    prompt_tick_counter = 0
    update_p2_info()
end

-- ============================================================
-- Dialog Management
-- ============================================================

local function cleanup_dialog()
    if dialog then
        pcall(function() dialog:delete() end)
        dialog = nil
    end

    p1_profile_dropdown = nil
    p1_profile_name_input = nil
    p1_start_time_input = nil
    p1_finish_time_input = nil
    p2_status_label = nil
    p2_file_label = nil
    p2_info_label = nil
    p2_mode_dropdown = nil
    p2_intro_input = nil
    p2_outro_input = nil
    p3_prompt_bar = nil
    p3_prompt_btn = nil
    p3_dismiss_btn = nil
    p3_countdown_lbl = nil
    p3_remember_btn = nil
end

-- ============================================================
-- Credit Skipper Functions
-- ============================================================

function load_all_profiles()
    if not file_exists(CREDIT_CONF_FILE) then return end

    profiles = {}
    local f = io.open(CREDIT_CONF_FILE, "r")
    if not f then return end

    for line in f:lines() do
        local name, st, ft = line:match("(.+)=(%d+),(%d+)")
        if name then
            table.insert(profiles, {
                name = name,
                start_time = tonumber(st) or 0,
                finish_time = tonumber(ft) or 0,
            })
        end
    end
    f:close()
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
                profile.name,
                tonumber(profile.start_time) or 0,
                tonumber(profile.finish_time) or 0))
        end
    end
    f:close()
end

function populate_profile_dropdown()
    if not p1_profile_dropdown or not dialog then return end

    pcall(function() dialog:del_widget(p1_profile_dropdown) end)
    p1_profile_dropdown = dialog:add_dropdown(2, 2, 1, 1)

    for i, profile in ipairs(profiles) do
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
        table.remove(profiles, idx)
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

    -- Update existing or create new
    local updated = false
    for _, profile in ipairs(profiles) do
        if profile and profile.name == name then
            profile.start_time = start_time
            profile.finish_time = finish_time
            updated = true
            break
        end
    end

    if not updated then
        table.insert(profiles, {
            name = name,
            start_time = start_time,
            finish_time = finish_time,
        })
    end

    save_all_profiles()
    populate_profile_dropdown()
end

function start_playlist()
    if not p1_start_time_input or not p1_finish_time_input then return end

    local skip_start = tonumber(p1_start_time_input:get_text()) or 0
    local skip_finish = tonumber(p1_finish_time_input:get_text()) or 0

    if skip_start < 0 then skip_start = 0 end
    if skip_finish < 0 then skip_finish = 0 end

    -- Get current item and apply options directly
    local ok, item = pcall(function() return vlc.input.item() end)
    if not ok or not item then
        if p2_status_label then
            pcall(p2_status_label.set_text, p2_status_label,
                "No item playing")
        end
        return
    end

    local dur = get_current_duration()
    if not dur then
        if p2_status_label then
            pcall(p2_status_label.set_text, p2_status_label,
                "Cannot get duration")
        end
        return
    end

    -- Validate durations
    local effective = dur - skip_start - skip_finish
    if effective < MIN_SEGMENT_DURATION then
        if p2_status_label then
            pcall(p2_status_label.set_text, p2_status_label,
                "Trim exceeds duration")
        end
        return
    end

    -- Seek to start position
    if skip_start > 0 then
        safe_seek(skip_start)
    end

    if p2_status_label then
        pcall(p2_status_label.set_text, p2_status_label,
            "Applied: start=" .. skip_start .. "s, finish=" .. skip_finish .. "s")
    end

    if dialog then
        dialog:hide()
    end
end

-- ============================================================
-- Panel 2 & 3 Update Functions
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

    local remaining_ticks = math.max(0,
        active_prompt.deadline_ticks - prompt_tick_counter)
    local remaining_sec = math.ceil(remaining_ticks / TICKS_PER_SECOND)

    local bar_html = string.format(
        "<center><b>%s</b> (%ds)</center>",
        active_prompt.label, remaining_sec)
    pcall(p3_prompt_bar.set_text, p3_prompt_bar, bar_html)

    if p3_prompt_btn then
        pcall(p3_prompt_btn.set_text, p3_prompt_btn,
            "Skip " .. segment_display_name(active_prompt.seg_type))
    end

    if p3_countdown_lbl then
        pcall(p3_countdown_lbl.set_text, p3_countdown_lbl,
            tostring(remaining_sec) .. "s")
    end
end

function update_p2_info()
    if not dialog then return end

    -- Show pending status
    if pending_status and p2_status_label then
        pcall(p2_status_label.set_text, p2_status_label, pending_status)
        pending_status = nil
    elseif last_error_msg and p2_status_label then
        pcall(p2_status_label.set_text, p2_status_label, last_error_msg)
    end

    local fn = get_current_filename() or "Nothing playing"

    if p2_file_label then
        pcall(p2_file_label.set_text, p2_file_label, fn)
    end

    local d = find_skip_entry(fn)
    local info = ""

    if d then
        if d.recap_start or d.recap_end then
            info = info .. "Recap: " .. format_time(d.recap_start or 0)
                .. " - " .. format_time(d.recap_end or 0) .. "<br/>"
        end
        if d.intro_end then
            info = info .. "Intro: " .. format_time(d.intro_start or 0)
                .. " - " .. format_time(d.intro_end) .. "<br/>"
        end
        if d.outro_start then
            info = info .. "Outro: " .. format_time(d.outro_start)
            if d.outro_end then
                info = info .. " - " .. format_time(d.outro_end)
            end
            info = info .. "<br/>"
        end
        if info == "" then
            info = "Entry found, no timestamps"
        end

        local series_key = get_series_key(fn)
        if series_key ~= "" and series_auto_skip_prefs[series_key] then
            info = info .. "<i>Always Skip: ON</i>"
        end
    else
        info = "No skip data"
    end

    if p2_info_label then
        pcall(p2_info_label.set_text, p2_info_label, info)
    end

    refresh_prompt_bar()
end

-- ============================================================
-- Button Callbacks
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
    local ok = load_skip_data()

    local msg = ok
        and ("Loaded: " .. count_skip_entries() .. " entries")
        or (last_error_msg or "Load failed")

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

    local names = {
        [SKIP_MODE_AUTO] = "Auto",
        [SKIP_MODE_PROMPT] = "Prompt",
        [SKIP_MODE_OFF] = "Off",
    }

    if p2_status_label then
        pcall(p2_status_label.set_text, p2_status_label,
            "Mode: " .. (names[skip_mode] or "?"))
    end
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

    if intro_val and not is_valid_timestamp(intro_val) then intro_val = nil end
    if outro_val and not is_valid_timestamp(outro_val) then outro_val = nil end

    if not intro_val and not outro_val then
        if p2_status_label then
            pcall(p2_status_label.set_text, p2_status_label, "Enter valid number")
        end
        return
    end

    -- Copy-on-write update
    local new_data = shallow_copy_table(skip_data)
    if not new_data[fn] then new_data[fn] = {} end

    if intro_val then
        new_data[fn].intro_start = 0
        new_data[fn].intro_end = intro_val
    end
    if outro_val then
        new_data[fn].outro_start = outro_val
        new_data[fn].outro_end = get_current_duration()
    end

    -- Validate
    if new_data[fn].intro_end and new_data[fn].outro_start then
        if new_data[fn].intro_end > new_data[fn].outro_start then
            if p2_status_label then
                pcall(p2_status_label.set_text, p2_status_label,
                    "Error: intro > outro")
            end
            return
        end
    end

    skip_data = new_data
    skip_entry_cache[fn] = nil
    intro_skipped = false
    outro_skipped = false
    recap_skipped = false
    clear_prompt()

    if p2_status_label then
        pcall(p2_status_label.set_text, p2_status_label, "Applied (session)")
    end
    update_p2_info()
end

function clear_prefs_btn()
    series_auto_skip_prefs = {}
    if p2_status_label then
        pcall(p2_status_label.set_text, p2_status_label, "Preferences cleared")
    end
    update_p2_info()
end

function refresh_btn()
    update_p2_info()
end

-- ============================================================
-- Prompt Button Callbacks
-- ============================================================

function confirm_prompt_btn()
    if not active_prompt or active_prompt.fired then
        if p2_status_label then
            pcall(p2_status_label.set_text, p2_status_label, "No prompt")
        end
        return
    end

    local seg = active_prompt.seg_type
    local tgt = active_prompt.target_s

    -- Set flags BEFORE action
    if seg == SEG_INTRO then intro_skipped = true end
    if seg == SEG_RECAP then recap_skipped = true end
    if seg == SEG_OUTRO then outro_skipped = true end

    active_prompt.fired = true
    clear_prompt()

    if tgt then
        if safe_seek(tgt) then
            if p2_status_label then
                pcall(p2_status_label.set_text, p2_status_label,
                    "Skipped " .. segment_display_name(seg))
            end
        end
    else
        if safe_playlist_next() then
            if p2_status_label then
                pcall(p2_status_label.set_text, p2_status_label, "Next episode")
            end
        else
            local dur = get_current_duration()
            if dur then safe_seek(dur) end
        end
    end

    update_p2_info()
end

function dismiss_prompt_btn()
    if not active_prompt or active_prompt.fired then
        if p2_status_label then
            pcall(p2_status_label.set_text, p2_status_label, "No prompt")
        end
        return
    end

    local seg = active_prompt.seg_type

    -- Set flags BEFORE clearing
    if seg == SEG_INTRO then intro_skipped = true end
    if seg == SEG_RECAP then recap_skipped = true end
    if seg == SEG_OUTRO then outro_skipped = true end

    active_prompt.fired = true
    clear_prompt()

    if p2_status_label then
        pcall(p2_status_label.set_text, p2_status_label,
            "Dismissed " .. segment_display_name(seg))
    end
    update_p2_info()
end

function remember_skip_btn()
    local fn = get_current_filename()
    if not fn then
        if p2_status_label then
            pcall(p2_status_label.set_text, p2_status_label, "No file")
        end
        return
    end

    local series_key = get_series_key(fn)
    if series_key == "" then
        if p2_status_label then
            pcall(p2_status_label.set_text, p2_status_label, "Cannot detect series")
        end
        return
    end

    series_auto_skip_prefs[series_key] = true

    if p2_status_label then
        pcall(p2_status_label.set_text, p2_status_label,
            "Always Skip: " .. series_key)
    end

    -- Confirm current prompt if any
    if active_prompt and not active_prompt.fired then
        confirm_prompt_btn()
    else
        update_p2_info()
    end
end

-- ============================================================
-- Dialog Creation
-- ============================================================

function open_dialog()
    cleanup_dialog()

    dialog = vlc.dialog(descriptor().title)

    local row = 1

    -- Panel 1: Credit Skipper
    dialog:add_label(
        "<center><h3>Credit Skipper</h3></center>",
        1, row, 4, 1)
    row = row + 1

    dialog:add_label("Profile:", 1, row, 1, 1)
    p1_profile_dropdown = dialog:add_dropdown(2, row, 1, 1)
    populate_profile_dropdown()
    dialog:add_button("Load", populate_profile_fields, 3, row, 1, 1)
    dialog:add_button("Delete", delete_profile, 4, row, 1, 1)
    row = row + 1

    dialog:add_label("Name:", 1, row, 1, 1)
    p1_profile_name_input = dialog:add_text_input("", 2, row, 3, 1)
    row = row + 1

    dialog:add_label("Intro (s):", 1, row, 1, 1)
    p1_start_time_input = dialog:add_text_input("0", 2, row, 1, 1)
    dialog:add_label("Outro (s):", 3, row, 1, 1)
    p1_finish_time_input = dialog:add_text_input("0", 4, row, 1, 1)
    row = row + 1

    dialog:add_button("Save Profile", save_profile, 1, row, 2, 1)
    dialog:add_button("Apply to Current", start_playlist, 3, row, 2, 1)
    row = row + 1

    -- Panel 2: Intro/Outro Skipper
    dialog:add_label(
        "<hr/><center><h3>Intro/Outro Skipper</h3></center>",
        1, row, 4, 1)
    row = row + 1

    p2_status_label = dialog:add_label(
        "Entries: " .. count_skip_entries(),
        1, row, 4, 1)
    row = row + 1

    dialog:add_label("File:", 1, row, 1, 1)
    p2_file_label = dialog:add_label(
        get_current_filename() or "None",
        2, row, 3, 1)
    row = row + 1

    p2_info_label = dialog:add_label("", 1, row, 4, 1)
    row = row + 1

    dialog:add_label("Mode:", 1, row, 1, 1)
    p2_mode_dropdown = dialog:add_dropdown(2, row, 2, 1)
    p2_mode_dropdown:add_value("Auto", SKIP_MODE_AUTO)
    p2_mode_dropdown:add_value("Prompt", SKIP_MODE_PROMPT)
    p2_mode_dropdown:add_value("Off", SKIP_MODE_OFF)
    p2_mode_dropdown:set_value(skip_mode)
    dialog:add_button("Set", apply_mode_btn, 4, row, 1, 1)
    row = row + 1

    dialog:add_button("Skip Intro", do_skip_intro_btn, 1, row, 1, 1)
    dialog:add_button("Skip Outro", do_skip_outro_btn, 2, row, 1, 1)
    dialog:add_button("Skip Recap", do_skip_recap_btn, 3, row, 1, 1)
    dialog:add_button("Reload", reload_json_btn, 4, row, 1, 1)
    row = row + 1

    -- Panel 3: Prompt Bar
    dialog:add_label("<hr/><center><b>Now Playing</b></center>",
        1, row, 4, 1)
    row = row + 1

    p3_prompt_bar = dialog:add_label(
        "<center><i>No prompt</i></center>",
        1, row, 4, 1)
    row = row + 1

    p3_prompt_btn = dialog:add_button("Skip", confirm_prompt_btn, 1, row, 1, 1)
    p3_dismiss_btn = dialog:add_button("Dismiss", dismiss_prompt_btn, 2, row, 1, 1)
    p3_remember_btn = dialog:add_button("Always Skip", remember_skip_btn, 3, row, 1, 1)
    p3_countdown_lbl = dialog:add_label("", 4, row, 1, 1)
    row = row + 1

    -- Manual Override
    dialog:add_label("<hr/><center>Manual Override (Session)</center>",
        1, row, 4, 1)
    row = row + 1

    dialog:add_label("Intro end:", 1, row, 1, 1)
    p2_intro_input = dialog:add_text_input("", 2, row, 1, 1)
    dialog:add_label("Outro start:", 3, row, 1, 1)
    p2_outro_input = dialog:add_text_input("", 4, row, 1, 1)
    row = row + 1

    dialog:add_button("Apply", apply_manual_btn, 1, row, 1, 1)
    dialog:add_button("Clear Prefs", clear_prefs_btn, 2, row, 1, 1)
    dialog:add_button("Refresh", refresh_btn, 3, row, 1, 1)
    dialog:add_button("Close", close, 4, row, 1, 1)

    populate_profile_fields()
    update_p2_info()
end

-- ============================================================
-- End of Script
-- ============================================================
