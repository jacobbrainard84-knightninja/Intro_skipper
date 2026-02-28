--[[
Unified Credit & Intro/Outro Skipper for VLC
============================================================
Version 4.2.0

FIXES in 4.2.0:
  - Fixed: normalize_key separator regex (hyphen last in class, no comma)
  - Fixed: normalize_key now truncates at 120 chars with SHA-256 suffix
    to match Python get_skip_key() exactly
  - Fixed: parse_json_number no longer validates domain inside the parser;
    validation moved to point of use, preventing state corruption on skip
  - Fixed: inner object parser scans to matching '}' on error instead of
    breaking with pos inside the bad object
  - Fixed: safe_seek uses vlc.misc.mwait (not msleep) and guards against
    calling mwait from the GUI thread
  - Fixed: safe_playlist_next uses URI-based last-item detection instead
    of .id comparison; correctly prevents wrap-around on last episode
  - Fixed: start_playlist now applies stop-time via vlc.var.set
  - Fixed: get_current_duration divides by 1000 (ms), not 1000000
  - Fixed: SKIP_DATA_FILE path matches Python DATA_DIR exactly
  - Fixed: file_content_hash reads only head/tail without loading entire file
  - Fixed: set_value on dropdown removed (not in VLC Lua API)
  - Fixed: trigger_auto_skip wired via meta_changed polling workaround
  - Fixed: SHA-256 pure-Lua implementation added for key truncation
]]

-- ============================================================
-- Configuration & Constants
-- ============================================================

local DEBUG = false
local SKIPPER_VERSION = "4.2.0"

local SEEK_SETTLE_US    = 500000    -- 500ms settle after seek (mwait units)
local JSON_CHECK_TICKS  = 60        -- reload JSON every N ticks (~15s at 250ms)
local INTRO_GRACE_SEC   = 5         -- auto-skip window at intro start
local MAX_TIMESTAMP_SEC = 86400     -- 24-hour sanity cap for timestamps
local PROMPT_DISPLAY_SEC = 8        -- how long prompt stays visible
local TICKS_PER_SECOND  = 4         -- approximate tick rate for countdown
local MIN_SEGMENT_DURATION = 1      -- minimum valid trimmed duration (s)
local MAX_KEY_LENGTH    = 120       -- must match Python CONSTANTS.MAX_KEY_LENGTH
local KEY_HASH_LEN      = 7         -- must match Python CONSTANTS.KEY_HASH_SUFFIX_LEN

local SEG_INTRO   = "intro"
local SEG_RECAP   = "recap"
local SEG_OUTRO   = "outro"
local SEG_CREDITS = "credits"

local SKIP_MODE_AUTO   = 1
local SKIP_MODE_PROMPT = 2
local SKIP_MODE_OFF    = 3

-- ============================================================
-- Path Setup
-- FIX: Paths now match Python DATA_DIR exactly.
--
-- Python (Linux):   ~/.config/intro_skipper/skip_data.json
-- Python (Windows): %APPDATA%\intro_skipper\skip_data.json
-- Python (macOS):   ~/Library/Application Support/intro_skipper/skip_data.json
--
-- vlc.config.userdatadir() returns the VLC user data directory which
-- is one level above the VLC config dir on all platforms, making it
-- easy to construct the sibling "intro_skipper" directory.
-- ============================================================

local function get_data_dir()
    -- vlc.config.userdatadir() is not always available; fall back to
    -- constructing the path from HOME/APPDATA environment variables.
    local sys = package.config:sub(1, 1) == "\\" and "windows" or "unix"

    if sys == "windows" then
        local appdata = os.getenv("APPDATA") or ""
        return appdata .. "\\intro_skipper"
    end

    -- macOS
    local home = os.getenv("HOME") or ""
    if package.loaded["os"] then
        -- Check for macOS by looking for /Library
        local f = io.open(home .. "/Library", "r")
        if f then
            f:close()
            return home .. "/Library/Application Support/intro_skipper"
        end
    end

    -- Linux / Steam Deck
    local xdg = os.getenv("XDG_CONFIG_HOME") or (home .. "/.config")
    return xdg .. "/intro_skipper"
end

local DATA_DIR       = get_data_dir()
local SKIP_DATA_FILE = DATA_DIR .. "/skip_data.json"

-- Credit conf uses the standard VLC config dir (separate from Python output)
local function get_vlc_config_dir()
    if vlc and vlc.config and vlc.config.configdir then
        local ok, d = pcall(vlc.config.configdir)
        if ok and d then return d end
    end
    local home = os.getenv("HOME") or ""
    if package.config:sub(1, 1) == "\\" then
        return (os.getenv("APPDATA") or "") .. "\\vlc"
    end
    return home .. "/.config/vlc"
end

local CREDIT_CONF_FILE = get_vlc_config_dir() .. "/credit-skipper.conf"

-- ============================================================
-- Logging helpers
-- ============================================================

local function dbg(msg)
    if DEBUG and vlc and vlc.msg then
        vlc.msg.dbg("[Skipper] " .. tostring(msg))
    end
end

local function log_info(msg)
    if vlc and vlc.msg then vlc.msg.info("[Skipper] " .. tostring(msg)) end
end

local function log_warn(msg)
    if vlc and vlc.msg then vlc.msg.warn("[Skipper] " .. tostring(msg)) end
end

local function log_err(msg)
    if vlc and vlc.msg then vlc.msg.err("[Skipper] " .. tostring(msg)) end
end

-- ============================================================
-- Shared State
-- ============================================================

local profiles = {}

local skip_data       = {}
local skip_data_hash  = ""
local skip_entry_cache = {}
local filename_cache  = {}

local intro_skipped  = false
local outro_skipped  = false
local recap_skipped  = false
local last_filename  = ""
local tick_count     = 0

local pending_status = nil
local last_error_msg = nil

local skip_mode      = SKIP_MODE_PROMPT
local active_prompt  = nil
local prompt_tick_counter = 0
local series_auto_skip_prefs = {}

-- GUI thread flag: mwait must not be called from GUI thread
local is_gui_thread  = true   -- true in extension dialog context

-- Dialog widgets
local dialog               = nil
local p1_profile_dropdown  = nil
local p1_profile_name_input = nil
local p1_start_time_input  = nil
local p1_finish_time_input = nil
local p2_status_label      = nil
local p2_file_label        = nil
local p2_info_label        = nil
local p2_mode_label        = nil  -- label shows current mode
local p2_intro_input       = nil
local p2_outro_input       = nil
local p3_prompt_bar        = nil
local p3_prompt_btn        = nil
local p3_dismiss_btn       = nil
local p3_countdown_lbl     = nil
local p3_remember_btn      = nil

-- ============================================================
-- Utility Functions
-- ============================================================

local function file_exists(path)
    if not path or path == "" then return false end
    local f = io.open(path, "rb")
    if f then f:close(); return true end
    return false
end

-- FIX: Read only head and tail bytes; do not load entire file.
local function file_content_hash(path)
    local f = io.open(path, "rb")
    if not f then return "" end
    local head = f:read(64) or ""
    -- Seek to near-end for tail
    local ok_seek = pcall(function() f:seek("end", -64) end)
    local tail = ""
    if ok_seek then tail = f:read(64) or "" end
    local size = f:seek("end", 0) or 0
    f:close()
    return string.format("%d:%s:%s", size, head, tail)
end

local function shallow_copy_table(t)
    if type(t) ~= "table" then return t end
    local copy = {}
    for k, v in pairs(t) do
        if type(v) == "table" then
            copy[k] = {}
            for k2, v2 in pairs(v) do copy[k][k2] = v2 end
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
    return string.format("%d:%02d", math.floor(seconds / 60), seconds % 60)
end

local function format_duration_short(seconds)
    if not seconds then return "" end
    seconds = math.floor(seconds)
    if seconds < 60 then return tostring(seconds) .. "s" end
    if seconds < 3600 then
        local m = math.floor(seconds / 60)
        local s = seconds % 60
        return s == 0 and (tostring(m) .. "m")
                       or string.format("%dm%02ds", m, s)
    end
    return format_time(seconds)
end

local function is_valid_timestamp(ts)
    return type(ts) == "number" and ts >= 0 and ts <= MAX_TIMESTAMP_SEC
end

local function segment_display_name(seg_type)
    if seg_type == SEG_INTRO   then return "Intro"
    elseif seg_type == SEG_RECAP   then return "Recap"
    elseif seg_type == SEG_OUTRO
        or seg_type == SEG_CREDITS then return "Credits"
    else return seg_type or "Segment" end
end

-- ============================================================
-- Pure-Lua SHA-256
-- RFC 6234 compliant. Used for normalize_key truncation suffix.
-- Output must be byte-identical to Python's hashlib.sha256.
-- ============================================================

local _sha256_k = {
    0x428a2f98,0x71374491,0xb5c0fbcf,0xe9b5dba5,
    0x3956c25b,0x59f111f1,0x923f82a4,0xab1c5ed5,
    0xd807aa98,0x12835b01,0x243185be,0x550c7dc3,
    0x72be5d74,0x80deb1fe,0x9bdc06a7,0xc19bf174,
    0xe49b69c1,0xefbe4786,0x0fc19dc6,0x240ca1cc,
    0x2de92c6f,0x4a7484aa,0x5cb0a9dc,0x76f988da,
    0x983e5152,0xa831c66d,0xb00327c8,0xbf597fc7,
    0xc6e00bf3,0xd5a79147,0x06ca6351,0x14292967,
    0x27b70a85,0x2e1b2138,0x4d2c6dfc,0x53380d13,
    0x650a7354,0x766a0abb,0x81c2c92e,0x92722c85,
    0xa2bfe8a1,0xa81a664b,0xc24b8b70,0xc76c51a3,
    0xd192e819,0xd6990624,0xf40e3585,0x106aa070,
    0x19a4c116,0x1e376c08,0x2748774c,0x34b0bcb5,
    0x391c0cb3,0x4ed8aa4a,0x5b9cca4f,0x682e6ff3,
    0x748f82ee,0x78a5636f,0x84c87814,0x8cc70208,
    0x90befffa,0xa4506ceb,0xbef9a3f7,0xc67178f2,
}

local function _u32(n) return n % 0x100000000 end
local function _rotr(x, n)
    return _u32((x >> n) | (x << (32 - n)))
end

local function sha256_hex(msg)
    -- Padding
    local len = #msg
    local bits = len * 8
    msg = msg .. "\x80"
    while (#msg % 64) ~= 56 do msg = msg .. "\x00" end
    -- Big-endian 64-bit length
    for i = 7, 0, -1 do
        msg = msg .. string.char(_u32(math.floor(bits / (2 ^ (i * 8)))) % 256)
    end

    local h = {
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
        0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
    }

    for blk = 0, (#msg // 64) - 1 do
        local w = {}
        for i = 0, 15 do
            local o = blk * 64 + i * 4 + 1
            w[i] = _u32(
                string.byte(msg, o)     * 0x1000000 +
                string.byte(msg, o + 1) * 0x10000   +
                string.byte(msg, o + 2) * 0x100     +
                string.byte(msg, o + 3))
        end
        for i = 16, 63 do
            local s0 = _u32(
                _rotr(w[i-15], 7) ~ _rotr(w[i-15], 18) ~ (w[i-15] >> 3))
            local s1 = _u32(
                _rotr(w[i-2], 17) ~ _rotr(w[i-2], 19) ~ (w[i-2] >> 10))
            w[i] = _u32(w[i-16] + s0 + w[i-7] + s1)
        end
        local a,b,c,d,e,f,g,hh =
            h[1],h[2],h[3],h[4],h[5],h[6],h[7],h[8]
        for i = 0, 63 do
            local S1  = _u32(_rotr(e, 6) ~ _rotr(e, 11) ~ _rotr(e, 25))
            local ch  = _u32((e & f) ~ ((~e) & g))
            local tmp1= _u32(hh + S1 + ch + _sha256_k[i + 1] + w[i])
            local S0  = _u32(_rotr(a, 2) ~ _rotr(a, 13) ~ _rotr(a, 22))
            local maj = _u32((a & b) ~ (a & c) ~ (b & c))
            local tmp2= _u32(S0 + maj)
            hh=g; g=f; f=e; e=_u32(d+tmp1)
            d=c; c=b; b=a; a=_u32(tmp1+tmp2)
        end
        h[1]=_u32(h[1]+a); h[2]=_u32(h[2]+b)
        h[3]=_u32(h[3]+c); h[4]=_u32(h[4]+d)
        h[5]=_u32(h[5]+e); h[6]=_u32(h[6]+f)
        h[7]=_u32(h[7]+g); h[8]=_u32(h[8]+hh)
    end

    local hex = ""
    for _, v in ipairs(h) do hex = hex .. string.format("%08x", v) end
    return hex
end

-- ============================================================
-- JSON Parser
-- ============================================================

local function skip_whitespace(str, pos)
    while pos <= #str do
        local c = str:sub(pos, pos)
        if c ~= " " and c ~= "\t" and c ~= "\n" and c ~= "\r" then break end
        pos = pos + 1
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
                return nil, pos, "Unexpected end after escape"
            end
            local esc = str:sub(pos, pos)
            if     esc == 'n'  then table.insert(result, '\n')
            elseif esc == 'r'  then table.insert(result, '\r')
            elseif esc == 't'  then table.insert(result, '\t')
            elseif esc == 'b'  then table.insert(result, '\b')
            elseif esc == 'f'  then table.insert(result, '\f')
            elseif esc == '\\' then table.insert(result, '\\')
            elseif esc == '"'  then table.insert(result, '"')
            elseif esc == '/'  then table.insert(result, '/')
            elseif esc == 'u'  then
                if pos + 4 > #str then
                    return nil, pos, "Incomplete unicode escape"
                end
                local hex = str:sub(pos + 1, pos + 4)
                local cp  = tonumber(hex, 16)
                if not cp then
                    log_warn("Invalid \\u escape: " .. hex)
                    table.insert(result, '?')
                    pos = pos + 4
                else
                    pos = pos + 4
                    -- Surrogate pair handling
                    if cp >= 0xD800 and cp <= 0xDBFF then
                        if pos + 6 <= #str
                                and str:sub(pos + 1, pos + 2) == '\\u' then
                            local hex2 = str:sub(pos + 3, pos + 6)
                            local cp2  = tonumber(hex2, 16)
                            if cp2 and cp2 >= 0xDC00 and cp2 <= 0xDFFF then
                                cp  = 0x10000
                                    + (cp  - 0xD800) * 0x400
                                    + (cp2 - 0xDC00)
                                pos = pos + 6
                            end
                        end
                    end
                    -- UTF-8 encode
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
                            0x80 + (math.floor(cp / 0x40)   % 0x40),
                            0x80 + (cp % 0x40)))
                    end
                end
            else
                table.insert(result, esc)
            end
            pos = pos + 1
        elseif c == '\n' or c == '\r' then
            return nil, pos, "Bare newline in string"
        else
            table.insert(result, c)
            pos = pos + 1
        end
    end
    return nil, pos, "Unterminated string"
end

-- FIX: No domain validation here; returns any valid JSON number.
local function parse_json_number(str, pos)
    local start_pos = pos
    if str:sub(pos, pos) == '-' then pos = pos + 1 end
    if pos > #str then return nil, start_pos, "Unexpected end in number" end
    local c = str:sub(pos, pos)
    if c == '0' then
        pos = pos + 1
    elseif c >= '1' and c <= '9' then
        while pos <= #str and str:sub(pos, pos):match('[0-9]') do
            pos = pos + 1
        end
    else
        return nil, start_pos, "Invalid number start"
    end
    if pos <= #str and str:sub(pos, pos) == '.' then
        pos = pos + 1
        if pos > #str or not str:sub(pos, pos):match('[0-9]') then
            return nil, start_pos, "Invalid decimal"
        end
        while pos <= #str and str:sub(pos, pos):match('[0-9]') do
            pos = pos + 1
        end
    end
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
    local num = tonumber(str:sub(start_pos, pos - 1))
    if not num then return nil, start_pos, "tonumber failed" end
    return num, pos, nil
end

-- FIX: On inner-object parse error, scan forward to matching '}'
-- to keep pos correctly positioned for the outer loop.
local function scan_to_object_end(content, pos)
    local depth = 1
    while pos <= #content and depth > 0 do
        local c = content:sub(pos, pos)
        if c == '{' then
            depth = depth + 1
            pos = pos + 1
        elseif c == '}' then
            depth = depth - 1
            pos = pos + 1
        elseif c == '"' then
            -- Skip over string to avoid counting braces inside strings
            local _, np, _ = parse_json_string(content, pos)
            pos = np
        else
            pos = pos + 1
        end
    end
    return pos
end

local function parse_json_object(content)
    if not content or content == "" then
        return {}, "Empty content"
    end
    local data      = {}
    local pos       = skip_whitespace(content, 1)
    local entry_count = 0

    if pos > #content or content:sub(pos, pos) ~= '{' then
        return {}, "Expected '{' at start"
    end
    pos = pos + 1

    local first = true
    while pos <= #content do
        pos = skip_whitespace(content, pos)
        if pos > #content then break end
        if content:sub(pos, pos) == '}' then break end

        if not first then
            if content:sub(pos, pos) == ',' then
                pos = skip_whitespace(content, pos + 1)
            else
                log_warn("Expected ',' between entries at pos " .. pos)
                break
            end
        end
        first = false

        -- Outer key
        local key, new_pos, err = parse_json_string(content, pos)
        if not key then
            log_warn("Outer key error: " .. (err or "?"))
            break
        end
        pos = skip_whitespace(content, new_pos)

        if content:sub(pos, pos) ~= ':' then
            log_warn("Expected ':' after outer key at pos " .. pos)
            break
        end
        pos = skip_whitespace(content, pos + 1)

        -- Inner object
        if content:sub(pos, pos) ~= '{' then
            log_warn("Expected '{' for value of key '" .. key .. "'")
            break
        end

        -- Remember where inner object starts for error recovery
        local inner_start = pos
        pos = pos + 1  -- consume '{'

        local entry       = {}
        local inner_first = true
        local inner_ok    = true

        while pos <= #content do
            pos = skip_whitespace(content, pos)
            if pos > #content then inner_ok = false; break end
            if content:sub(pos, pos) == '}' then pos = pos + 1; break end

            if not inner_first then
                if content:sub(pos, pos) == ',' then
                    pos = skip_whitespace(content, pos + 1)
                else
                    log_warn("Expected ',' in inner object at pos " .. pos)
                    inner_ok = false
                    break
                end
            end
            inner_first = false

            local field, np2, e2 = parse_json_string(content, pos)
            if not field then
                log_warn("Inner key error: " .. (e2 or "?"))
                inner_ok = false; break
            end
            pos = skip_whitespace(content, np2)

            if content:sub(pos, pos) ~= ':' then
                log_warn("Expected ':' after inner key '" .. field .. "'")
                inner_ok = false; break
            end
            pos = skip_whitespace(content, pos + 1)

            local val, np3, e3 = parse_json_number(content, pos)
            if val ~= nil then
                -- FIX: Validate at point of use, not inside parser.
                if is_valid_timestamp(val) then
                    entry[field] = val
                else
                    log_warn("Timestamp out of range for '"
                        .. field .. "': " .. val .. " — skipped")
                end
                pos = np3
            else
                log_warn("Value parse error for field '"
                    .. (field or "?") .. "': " .. (e3 or "?"))
                inner_ok = false; break
            end
        end

        -- FIX: If inner parse failed, scan forward past the closing '}'
        -- so the outer loop can continue correctly.
        if not inner_ok then
            log_warn("Recovering from bad inner object for key '" .. key .. "'")
            pos = scan_to_object_end(content, inner_start + 1)
        end

        if key and next(entry) then
            -- Validate intro/outro ordering
            if entry.intro_end and entry.outro_start then
                if entry.intro_end > entry.outro_start then
                    log_warn("intro_end > outro_start in '"
                        .. key .. "' — discarding outro")
                    entry.outro_start = nil
                    entry.outro_end   = nil
                end
            end
            data[key] = entry
            entry_count = entry_count + 1
        end
    end

    dbg("Parsed " .. entry_count .. " entries")
    return data, nil
end

-- ============================================================
-- normalize_key
-- Must produce byte-identical output to Python's get_skip_key().
--
-- Canonical steps (numbered to match Python's _normalise_stem/_truncate_key):
--   1. Percent-decode URI escapes
--   2. Strip last .ext segment
--   3. Lower-case
--   4. Replace en-dash (E2 80 93) and em-dash (E2 80 94) with "."
--      BEFORE any bracket class to prevent 0xE2 byte from matching
--      inside [...] patterns
--   5. Collapse ASCII separators [ \t_.:- ] into "."
--      "-" is LAST in the class so it is a literal hyphen, not a range.
--      Python _RE_SEPARATOR = r"[ \t_.:-]+" — no comma.
--   6. Strip bytes that are not [a-z0-9.]
--   7. Collapse ".." into "."
--   8. Strip leading/trailing dots
--   9. If len > MAX_KEY_LENGTH: truncate and append "-" + first
--      KEY_HASH_LEN hex chars of SHA-256(full_normalised_stem)
-- ============================================================

local function normalize_key(name)
    if not name or name == "" then return "" end

    -- 1. Percent-decode
    name = name:gsub("%%(%x%x)", function(h)
        return string.char(tonumber(h, 16))
    end)
    -- 2. Strip extension
    name = name:gsub("%.[^%.]+$", "")
    -- 3. Lower-case
    name = name:lower()
    -- 4. Multi-byte dashes (must precede bracket-class patterns)
    name = name:gsub("\xe2\x80\x93", ".")   -- U+2013 en-dash
    name = name:gsub("\xe2\x80\x94", ".")   -- U+2014 em-dash
    -- 5. ASCII separators; "-" is last so it is literal, not a range.
    --    Matches Python: r"[ \t_.:-]+"  (no comma)
    name = name:gsub("[ \t_%.:%-]+", ".")
    -- 6. Strip non-[a-z0-9.]
    name = name:gsub("[^a-z0-9%.]+", "")
    -- 7. Collapse repeated dots
    name = name:gsub("%.%.+", ".")
    -- 8. Strip leading/trailing dots
    name = name:gsub("^%.+", ""):gsub("%.+$", "")
    -- 9. Truncate with SHA-256 suffix (must match Python _truncate_key)
    if #name > MAX_KEY_LENGTH then
        local full_hash = sha256_hex(name)
        local suffix    = full_hash:sub(1, KEY_HASH_LEN)
        name = name:sub(1, MAX_KEY_LENGTH - KEY_HASH_LEN - 1)
             .. "-" .. suffix
    end

    return name
end

-- ============================================================
-- Skip Entry Management
-- ============================================================

local function count_skip_entries()
    local c = 0
    for _ in pairs(skip_data) do c = c + 1 end
    return c
end

local function get_series_key(fn)
    if not fn then return "" end
    local nk = normalize_key(fn)
    nk = nk:gsub("s%d+e%d+", "")
    nk = nk:gsub("e%d+", "")
    nk = nk:gsub("%d+x%d+", "")
    nk = nk:gsub("%.+", ".")
    nk = nk:gsub("^%.+", ""):gsub("%.+$", "")
    return nk
end

local function load_skip_data()
    local new_hash = file_content_hash(SKIP_DATA_FILE)
    if new_hash ~= "" and new_hash == skip_data_hash then
        dbg("skip_data unchanged")
        return true
    end
    if not file_exists(SKIP_DATA_FILE) then
        last_error_msg = "skip_data.json not found at: " .. SKIP_DATA_FILE
        log_warn(last_error_msg)
        skip_data = {}; skip_data_hash = ""; skip_entry_cache = {}
        return false
    end
    local f = io.open(SKIP_DATA_FILE, "r")
    if not f then
        last_error_msg = "Cannot open skip_data.json"
        log_err(last_error_msg); return false
    end
    local content = f:read("*all"); f:close()
    if not content or #content == 0 then
        last_error_msg = "skip_data.json is empty"
        log_warn(last_error_msg)
        skip_data = {}; skip_data_hash = ""; skip_entry_cache = {}
        return false
    end
    local result, err = parse_json_object(content)
    if err then
        last_error_msg = "JSON error: " .. tostring(err)
        log_err(last_error_msg)
        return false  -- keep previous skip_data on parse failure
    end
    skip_data      = result
    skip_data_hash = new_hash
    skip_entry_cache = {}
    last_error_msg = nil
    log_info("Loaded " .. count_skip_entries() .. " entries")
    return true
end

local function find_skip_entry(fn)
    if not fn or fn == "" then return nil end
    local cached = skip_entry_cache[fn]
    if cached ~= nil then
        return cached == false and nil or cached
    end
    local nfn = normalize_key(fn)
    if nfn ~= "" and skip_data[nfn] then
        skip_entry_cache[fn] = skip_data[nfn]; return skip_data[nfn]
    end
    if skip_data[fn] then
        skip_entry_cache[fn] = skip_data[fn]; return skip_data[fn]
    end
    if nfn ~= "" then
        for key, val in pairs(skip_data) do
            if normalize_key(key) == nfn then
                skip_entry_cache[fn] = val; return val
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
    if filename_cache[uri] then return filename_cache[uri] end
    local fn = uri:match("([^/\\]+)$")
    if fn then
        fn = fn:gsub("%%(%x%x)", function(h)
            return string.char(tonumber(h, 16))
        end)
        filename_cache[uri] = fn
    end
    return fn
end

-- FIX: item:duration() returns milliseconds, not microseconds.
local function get_current_duration()
    local ok, item = pcall(function() return vlc.input.item() end)
    if not ok or not item then return nil end
    local dok, dur = pcall(function() return item:duration() end)
    if dok and dur and dur > 0 then
        return dur / 1000   -- milliseconds → seconds
    end
    return nil
end

-- FIX: Use mwait (not msleep). Guard against calling mwait on GUI thread.
-- use_mwait should be false when called from button callbacks (GUI thread)
-- and true when called from a background intf loop.
local function safe_seek(target_s, use_mwait)
    if not target_s or target_s < 0 then return false end
    dbg("safe_seek(" .. target_s .. ")")
    local ok, input = pcall(function() return vlc.object.input() end)
    if not ok or not input then return false end
    local seek_ok = pcall(function()
        vlc.var.set(input, "time", math.floor(target_s * 1000000))
    end)
    -- mwait takes an absolute timestamp; must not be called on GUI thread.
    if seek_ok and use_mwait
            and vlc.misc and vlc.misc.mwait and vlc.misc.mdate then
        vlc.misc.mwait(vlc.misc.mdate() + SEEK_SETTLE_US)
    end
    return seek_ok
end

-- FIX: URI-based position detection prevents wrap-around on last episode.
local function safe_playlist_next()
    local pok, root = pcall(function()
        return vlc.playlist.get("normal")
    end)
    if not pok or not root or not root.children then return false end
    local n = #root.children
    if n <= 1 then return false end

    local uok, cur_uri = pcall(function()
        local item = vlc.input.item()
        return item and item:uri()
    end)
    if not uok or not cur_uri then return false end

    local cur_idx = nil
    for i, child in ipairs(root.children) do
        local cok, child_uri = pcall(function() return child:uri() end)
        if cok and child_uri == cur_uri then cur_idx = i; break end
    end

    if not cur_idx or cur_idx >= n then
        return false  -- unknown or last item — do not advance
    end

    return pcall(vlc.playlist.next)
end

-- ============================================================
-- Prompt Engine
-- ============================================================

local function clear_prompt()
    active_prompt = nil
end

local function set_prompt(seg_type, target_s, label)
    if active_prompt and active_prompt.seg_type == seg_type then return end
    local deadline = prompt_tick_counter
                   + math.floor(PROMPT_DISPLAY_SEC * TICKS_PER_SECOND)
    active_prompt = {
        seg_type       = seg_type,
        target_s       = target_s,
        label          = label,
        deadline_ticks = deadline,
        fired          = false,
    }
    dbg("set_prompt: " .. seg_type)
    pending_status = "⏭ " .. label
end

local function series_prefers_skip(fn)
    local key = get_series_key(fn)
    return key ~= "" and series_auto_skip_prefs[key] == true
end

-- ============================================================
-- Skip Actions (called from buttons — GUI thread, no mwait)
-- ============================================================

local function do_skip_intro()
    local fn = get_current_filename()
    if not fn then return false, "No file playing" end
    local d = find_skip_entry(fn)
    if not d then return false, "No skip data" end
    if not d.intro_end then return false, "No intro_end" end
    intro_skipped = true
    clear_prompt()
    if safe_seek(d.intro_end, false) then   -- false = GUI thread
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
    outro_skipped = true
    clear_prompt()
    if safe_playlist_next() then return true, "Next episode" end
    local target = d.outro_end or get_current_duration()
    if target and safe_seek(target, false) then return true, "Outro → end" end
    return false, "Action failed"
end

local function do_skip_recap()
    local fn = get_current_filename()
    if not fn then return false, "No file playing" end
    local d = find_skip_entry(fn)
    if not d then return false, "No skip data" end
    if not d.recap_end then return false, "No recap_end" end
    recap_skipped = true
    clear_prompt()
    if safe_seek(d.recap_end, false) then
        return true, "Recap → " .. format_time(d.recap_end)
    end
    return false, "Seek failed"
end

-- ============================================================
-- Auto-Skip Detection Handlers
-- These are called from trigger_auto_skip which runs on a
-- background tick — safe to pass use_mwait=true IF this is
-- running in an intf context. In the extension context these
-- ticks come from input_changed/meta_changed callbacks which
-- run on the GUI thread, so we pass false here too.
-- ============================================================

local function handle_recap_detection(d, t, fn, auto_mode)
    if recap_skipped or not d.recap_end then return end
    local recap_start = d.recap_start or 0
    if t >= recap_start and t < d.recap_end then
        if auto_mode then
            recap_skipped = true
            if safe_seek(d.recap_end, false) then
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

local function get_intro_zone(t, intro_start, intro_end)
    local grace_end = math.min(intro_start + INTRO_GRACE_SEC, intro_end - 0.5)
    if t < intro_start   then return 0
    elseif t < grace_end then return 1
    elseif t < intro_end then return 2
    else                      return 3 end
end

local function handle_intro_detection(d, t, fn, auto_mode)
    if intro_skipped or not d.intro_end then return end
    local intro_start = d.intro_start or 0
    local zone = get_intro_zone(t, intro_start, d.intro_end)
    if zone == 1 then
        if auto_mode then
            intro_skipped = true
            if safe_seek(d.intro_end, false) then
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
                if target then safe_seek(target, false) end
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
    active_prompt.fired = true
    if seg == SEG_INTRO then intro_skipped = true end
    if seg == SEG_RECAP then recap_skipped = true end
    if seg == SEG_OUTRO then outro_skipped = true end
    clear_prompt()
    local should_act = series_prefers_skip(fn) or auto_mode
    if should_act then
        if tgt then
            if safe_seek(tgt, false) then
                pending_status = "Auto-skipped " .. segment_display_name(seg)
            end
        else
            if safe_playlist_next() then
                pending_status = "Auto → next episode"
            else
                local dur = get_current_duration()
                if dur then safe_seek(dur, false) end
            end
        end
    else
        pending_status = "Prompt expired"
    end
end

-- ============================================================
-- Main Tick (called from input_changed in extension context)
-- ============================================================

function trigger_auto_skip()
    if skip_mode == SKIP_MODE_OFF then return end
    prompt_tick_counter = prompt_tick_counter + 1

    tick_count = tick_count + 1
    if tick_count >= JSON_CHECK_TICKS then
        tick_count = 0
        load_skip_data()
    end

    local fn = get_current_filename()
    if not fn then return end

    if fn ~= last_filename then
        dbg("New file: " .. fn)
        intro_skipped = false; outro_skipped = false; recap_skipped = false
        active_prompt = nil; last_filename = fn
    end

    local d = find_skip_entry(fn)
    if not d then return end

    local ok, input = pcall(function() return vlc.object.input() end)
    if not ok or not input then return end

    local tok, tus = pcall(function() return vlc.var.get(input, "time") end)
    if not tok or not tus then return end
    local t = tus / 1000000

    local auto_mode = (skip_mode == SKIP_MODE_AUTO) or series_prefers_skip(fn)

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
        title       = "Credit & Intro/Outro Skipper",
        version     = SKIPPER_VERSION,
        author      = "Michael Bull / skip_intro.py",
        shortdesc   = "Skip Credits + Auto Intro/Outro",
        description = "Profile-based credit trimming with Netflix-style skip prompts.",
        capabilities = { "input-listener" },
    }
end

function activate()
    dbg("activate()")
    profiles             = {}
    skip_data            = {}
    skip_data_hash       = ""
    skip_entry_cache     = {}
    filename_cache       = {}
    pending_status       = nil
    last_error_msg       = nil
    active_prompt        = nil
    prompt_tick_counter  = 0
    series_auto_skip_prefs = {}
    tick_count           = 0

    if file_exists(CREDIT_CONF_FILE) then load_all_profiles() end
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
    -- VLC calls meta_changed() periodically while playback is active.
    -- We use it as our polling tick since extensions have no timer API.
    trigger_auto_skip()
    update_p2_info()
end

function input_changed()
    dbg("input_changed()")
    intro_skipped       = false
    outro_skipped       = false
    recap_skipped       = false
    active_prompt       = nil
    prompt_tick_counter = 0
    -- Clear filename cache entry for the new item (stale entry would
    -- be harmless but wastes memory over long sessions).
    filename_cache = {}
    update_p2_info()
end

-- ============================================================
-- Dialog Management
-- ============================================================

function cleanup_dialog()
    if dialog then pcall(function() dialog:delete() end); dialog = nil end
    p1_profile_dropdown  = nil; p1_profile_name_input = nil
    p1_start_time_input  = nil; p1_finish_time_input  = nil
    p2_status_label      = nil; p2_file_label         = nil
    p2_info_label        = nil; p2_mode_label         = nil
    p2_intro_input       = nil; p2_outro_input        = nil
    p3_prompt_bar        = nil; p3_prompt_btn         = nil
    p3_dismiss_btn       = nil; p3_countdown_lbl      = nil
    p3_remember_btn      = nil
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
                name        = name,
                start_time  = tonumber(st) or 0,
                finish_time = tonumber(ft) or 0,
            })
        end
    end
    f:close()
end

function save_all_profiles()
    local f = io.open(CREDIT_CONF_FILE, "w")
    if not f then log_err("Cannot write: " .. CREDIT_CONF_FILE); return end
    for _, profile in pairs(profiles) do
        if profile and profile.name then
            f:write(string.format("%s=%d,%d\n",
                profile.name,
                tonumber(profile.start_time)  or 0,
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
    if not p1_profile_dropdown or not p1_profile_name_input then return end
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
    local start_time  = math.max(0, tonumber(p1_start_time_input:get_text())  or 0)
    local finish_time = math.max(0, tonumber(p1_finish_time_input:get_text()) or 0)
    local updated = false
    for _, profile in ipairs(profiles) do
        if profile and profile.name == name then
            profile.start_time  = start_time
            profile.finish_time = finish_time
            updated = true; break
        end
    end
    if not updated then
        table.insert(profiles, {
            name = name, start_time = start_time, finish_time = finish_time
        })
    end
    save_all_profiles()
    populate_profile_dropdown()
end

-- FIX: Now correctly applies stop-time in addition to seeking to start.
function start_playlist()
    if not p1_start_time_input or not p1_finish_time_input then return end
    local skip_start  = math.max(0, tonumber(p1_start_time_input:get_text())  or 0)
    local skip_finish = math.max(0, tonumber(p1_finish_time_input:get_text()) or 0)

    local ok, item = pcall(function() return vlc.input.item() end)
    if not ok or not item then
        if p2_status_label then
            pcall(p2_status_label.set_text, p2_status_label, "No item playing")
        end
        return
    end

    local dur = get_current_duration()
    if not dur then
        if p2_status_label then
            pcall(p2_status_label.set_text, p2_status_label, "Cannot get duration")
        end
        return
    end

    local effective = dur - skip_start - skip_finish
    if effective < MIN_SEGMENT_DURATION then
        if p2_status_label then
            pcall(p2_status_label.set_text, p2_status_label, "Trim exceeds duration")
        end
        return
    end

    -- Apply stop-time before seeking so VLC knows where to stop.
    if skip_finish > 0 then
        local stop_pos = dur - skip_finish
        local iok, input = pcall(function() return vlc.object.input() end)
        if iok and input then
            pcall(function()
                vlc.var.set(input, "stop-time",
                    math.floor(stop_pos * 1000000))
            end)
        end
    end

    if skip_start > 0 then
        safe_seek(skip_start, false)
    end

    if p2_status_label then
        pcall(p2_status_label.set_text, p2_status_label,
            string.format("Applied: +%ds / -%ds", skip_start, skip_finish))
    end

    if dialog then pcall(function() dialog:hide() end) end
end

-- ============================================================
-- Panel Update Functions
-- ============================================================

local function skip_mode_name()
    if skip_mode == SKIP_MODE_AUTO   then return "Auto"
    elseif skip_mode == SKIP_MODE_PROMPT then return "Prompt"
    else                                      return "Off" end
end

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
    pcall(p3_prompt_bar.set_text, p3_prompt_bar,
        string.format("<center><b>%s</b> (%ds)</center>",
            active_prompt.label, remaining_sec))
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
    if p2_mode_label then
        pcall(p2_mode_label.set_text, p2_mode_label,
            "Mode: " .. skip_mode_name())
    end
    local d    = find_skip_entry(fn)
    local info = ""
    if d then
        if d.recap_start or d.recap_end then
            info = info .. "Recap: "
                .. format_time(d.recap_start or 0)
                .. " - " .. format_time(d.recap_end or 0) .. "<br/>"
        end
        if d.intro_end then
            info = info .. "Intro: "
                .. format_time(d.intro_start or 0)
                .. " - " .. format_time(d.intro_end) .. "<br/>"
        end
        if d.outro_start then
            info = info .. "Outro: " .. format_time(d.outro_start)
            if d.outro_end then
                info = info .. " - " .. format_time(d.outro_end)
            end
            info = info .. "<br/>"
        end
        if info == "" then info = "Entry found, no timestamps" end
        local sk = get_series_key(fn)
        if sk ~= "" and series_auto_skip_prefs[sk] then
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
    local ok  = load_skip_data()
    local msg = ok
        and ("Loaded: " .. count_skip_entries() .. " entries")
        or  (last_error_msg or "Load failed")
    if p2_status_label then
        pcall(p2_status_label.set_text, p2_status_label, msg)
    end
    update_p2_info()
end

function set_mode_auto_btn()
    skip_mode = SKIP_MODE_AUTO
    if p2_status_label then
        pcall(p2_status_label.set_text, p2_status_label, "Mode: Auto")
    end
    update_p2_info()
end

function set_mode_prompt_btn()
    skip_mode = SKIP_MODE_PROMPT
    if p2_status_label then
        pcall(p2_status_label.set_text, p2_status_label, "Mode: Prompt")
    end
    update_p2_info()
end

function set_mode_off_btn()
    skip_mode = SKIP_MODE_OFF
    if p2_status_label then
        pcall(p2_status_label.set_text, p2_status_label, "Mode: Off")
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
    local new_data = shallow_copy_table(skip_data)
    if not new_data[fn] then new_data[fn] = {} end
    if intro_val then
        new_data[fn].intro_start = 0
        new_data[fn].intro_end   = intro_val
    end
    if outro_val then
        new_data[fn].outro_start = outro_val
        new_data[fn].outro_end   = get_current_duration()
    end
    if new_data[fn].intro_end and new_data[fn].outro_start then
        if new_data[fn].intro_end > new_data[fn].outro_start then
            if p2_status_label then
                pcall(p2_status_label.set_text, p2_status_label,
                    "Error: intro_end > outro_start")
            end
            return
        end
    end
    skip_data = new_data
    skip_entry_cache[fn] = nil
    intro_skipped = false; outro_skipped = false; recap_skipped = false
    clear_prompt()
    if p2_status_label then
        pcall(p2_status_label.set_text, p2_status_label,
            "Applied (session only — will be lost on Reload)")
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
            pcall(p2_status_label.set_text, p2_status_label, "No active prompt")
        end
        return
    end
    local seg = active_prompt.seg_type
    local tgt = active_prompt.target_s
    if seg == SEG_INTRO then intro_skipped = true end
    if seg == SEG_RECAP then recap_skipped = true end
    if seg == SEG_OUTRO then outro_skipped = true end
    active_prompt.fired = true
    clear_prompt()
    if tgt then
        if safe_seek(tgt, false) then
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
            if dur then safe_seek(dur, false) end
        end
    end
    update_p2_info()
end

function dismiss_prompt_btn()
    if not active_prompt or active_prompt.fired then
        if p2_status_label then
            pcall(p2_status_label.set_text, p2_status_label, "No active prompt")
        end
        return
    end
    local seg = active_prompt.seg_type
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
            pcall(p2_status_label.set_text, p2_status_label, "No file playing")
        end
        return
    end
    local sk = get_series_key(fn)
    if sk == "" then
        if p2_status_label then
            pcall(p2_status_label.set_text, p2_status_label, "Cannot detect series")
        end
        return
    end
    series_auto_skip_prefs[sk] = true
    if p2_status_label then
        pcall(p2_status_label.set_text, p2_status_label, "Always Skip: " .. sk)
    end
    if active_prompt and not active_prompt.fired then
        confirm_prompt_btn()
    else
        update_p2_info()
    end
end

-- ============================================================
-- Dialog Creation
-- FIX: Replaced p2_mode_dropdown (set_value not in VLC Lua API)
-- with three explicit mode buttons and a mode label.
-- ============================================================

function open_dialog()
    cleanup_dialog()
    dialog = vlc.dialog(descriptor().title)
    local row = 1

    -- ---- Panel 1: Credit Skipper ----
    dialog:add_label(
        "<center><h3>Credit Skipper</h3></center>", 1, row, 4, 1)
    row = row + 1

    dialog:add_label("Profile:", 1, row, 1, 1)
    p1_profile_dropdown = dialog:add_dropdown(2, row, 1, 1)
    populate_profile_dropdown()
    dialog:add_button("Load",   populate_profile_fields, 3, row, 1, 1)
    dialog:add_button("Delete", delete_profile,          4, row, 1, 1)
    row = row + 1

    dialog:add_label("Name:", 1, row, 1, 1)
    p1_profile_name_input = dialog:add_text_input("", 2, row, 3, 1)
    row = row + 1

    dialog:add_label("Start (s):", 1, row, 1, 1)
    p1_start_time_input = dialog:add_text_input("0", 2, row, 1, 1)
    dialog:add_label("Finish (s):", 3, row, 1, 1)
    p1_finish_time_input = dialog:add_text_input("0", 4, row, 1, 1)
    row = row + 1

    dialog:add_button("Save Profile",       save_profile,    1, row, 2, 1)
    dialog:add_button("Apply to Current",   start_playlist,  3, row, 2, 1)
    row = row + 1

    -- ---- Panel 2: Intro/Outro Skipper ----
    dialog:add_label(
        "<hr/><center><h3>Intro/Outro Skipper</h3></center>", 1, row, 4, 1)
    row = row + 1

    p2_status_label = dialog:add_label(
        "Entries: " .. count_skip_entries(), 1, row, 4, 1)
    row = row + 1

    dialog:add_label("File:", 1, row, 1, 1)
    p2_file_label = dialog:add_label(
        get_current_filename() or "None", 2, row, 3, 1)
    row = row + 1

    p2_info_label = dialog:add_label("", 1, row, 4, 1)
    row = row + 1

    -- FIX: Three buttons instead of dropdown+set_value.
    p2_mode_label = dialog:add_label(
        "Mode: " .. skip_mode_name(), 1, row, 1, 1)
    dialog:add_button("Auto",   set_mode_auto_btn,   2, row, 1, 1)
    dialog:add_button("Prompt", set_mode_prompt_btn, 3, row, 1, 1)
    dialog:add_button("Off",    set_mode_off_btn,    4, row, 1, 1)
    row = row + 1

    dialog:add_button("Skip Intro",  do_skip_intro_btn,  1, row, 1, 1)
    dialog:add_button("Skip Outro",  do_skip_outro_btn,  2, row, 1, 1)
    dialog:add_button("Skip Recap",  do_skip_recap_btn,  3, row, 1, 1)
    dialog:add_button("Reload JSON", reload_json_btn,    4, row, 1, 1)
    row = row + 1

    -- ---- Panel 3: Prompt Bar ----
    dialog:add_label(
        "<hr/><center><b>Now Playing</b></center>", 1, row, 4, 1)
    row = row + 1

    p3_prompt_bar = dialog:add_label(
        "<center><i>No prompt</i></center>", 1, row, 4, 1)
    row = row + 1

    p3_prompt_btn   = dialog:add_button("Skip",        confirm_prompt_btn, 1, row, 1, 1)
    p3_dismiss_btn  = dialog:add_button("Dismiss",     dismiss_prompt_btn, 2, row, 1, 1)
    p3_remember_btn = dialog:add_button("Always Skip", remember_skip_btn,  3, row, 1, 1)
    p3_countdown_lbl = dialog:add_label("",                                4, row, 1, 1)
    row = row + 1

    -- ---- Manual Override ----
    dialog:add_label(
        "<hr/><center>Manual Override (Session Only)</center>", 1, row, 4, 1)
    row = row + 1

    dialog:add_label("Intro end (s):",   1, row, 1, 1)
    p2_intro_input = dialog:add_text_input("", 2, row, 1, 1)
    dialog:add_label("Outro start (s):", 3, row, 1, 1)
    p2_outro_input = dialog:add_text_input("", 4, row, 1, 1)
    row = row + 1

    dialog:add_button("Apply",       apply_manual_btn, 1, row, 1, 1)
    dialog:add_button("Clear Prefs", clear_prefs_btn,  2, row, 1, 1)
    dialog:add_button("Refresh",     refresh_btn,      3, row, 1, 1)
    dialog:add_button("Close",       close,            4, row, 1, 1)

    populate_profile_fields()
    update_p2_info()
end
