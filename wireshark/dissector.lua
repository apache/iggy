local iggy = Proto("iggy", "Iggy Protocol")

iggy.prefs.server_port = Pref.uint("Server Port", 8090, "Target TCP server port")

local common_fields = {
    message_type = ProtoField.string("iggy.message_type", "Message Type"),
    req_length = ProtoField.uint32("iggy.request.length", "Length", base.DEC, nil, nil, "Length of command code + payload"),
    req_command = ProtoField.uint32("iggy.request.command", "Command Code", base.DEC),
    req_command_name = ProtoField.string("iggy.request.command_name", "Command Name"),
    req_payload_tree = ProtoField.none("iggy.request.payload_tree", "Payload"),
    resp_status = ProtoField.uint32("iggy.response.status", "Status Code", base.DEC),
    resp_status_name = ProtoField.string("iggy.response.status_name", "Status Name"),
    resp_length = ProtoField.uint32("iggy.response.length", "Length", base.DEC, nil, nil, "Length of payload"),
    resp_payload_tree = ProtoField.none("iggy.response.payload_tree", "Payload"),
    request_frame = ProtoField.framenum("iggy.request_frame", "Request Frame", base.NONE, frametype.REQUEST),
    response_frame = ProtoField.framenum("iggy.response_frame", "Response Frame", base.NONE, frametype.RESPONSE),
}

local COMMANDS = {
    [1] = {
        name = "ping",
        fields = { request = {}, response = {},},
        request_payload_dissector = function(self, buffer, tree, offset) end,
        response_payload_dissector = function(self, buffer, tree, offset) end,
    },
    [38] = {
        name = "user.login",
        fields = {
            request = {
                username_len = ProtoField.uint8("iggy.login_user.req.username_len", "Username Length", base.DEC),
                username = ProtoField.string("iggy.login_user.req.username", "Username"),
                password_len = ProtoField.uint8("iggy.login_user.req.password_len", "Password Length", base.DEC),
                password = ProtoField.string("iggy.login_user.req.password", "Password"),
                version_len = ProtoField.uint32("iggy.login_user.req.version_len", "Version Length", base.DEC),
                version = ProtoField.string("iggy.login_user.req.version", "Version"),
                context_len = ProtoField.uint32("iggy.login_user.req.context_len", "Context Length", base.DEC),
                context = ProtoField.string("iggy.login_user.req.context", "Context"),
            },
            response = {
                user_id = ProtoField.uint32("iggy.login_user.resp.user_id", "User ID", base.DEC),
            },
        },
        request_payload_dissector = function(self, buffer, tree, offset)
            local f = self.fields.request

            local username_len = buffer(offset, 1):uint()
            tree:add(f.username_len, buffer(offset, 1))
            offset = offset + 1
            tree:add(f.username, buffer(offset, username_len))
            offset = offset + username_len

            local password_len = buffer(offset, 1):uint()
            tree:add(f.password_len, buffer(offset, 1))
            offset = offset + 1
            tree:add(f.password, buffer(offset, password_len))
            offset = offset + password_len

            local version_len = buffer(offset, 4):le_uint()
            tree:add_le(f.version_len, buffer(offset, 4))
            offset = offset + 4
            if version_len > 0 then
                tree:add(f.version, buffer(offset, version_len))
                offset = offset + version_len
            end

            local context_len = buffer(offset, 4):le_uint()
            tree:add_le(f.context_len, buffer(offset, 4))
            offset = offset + 4
            if context_len > 0 then
                tree:add(f.context, buffer(offset, context_len))
            end
        end,
        response_payload_dissector = function(self, buffer, tree, offset)
            local f = self.fields.response
            tree:add_le(f.user_id, buffer(offset, 4))
        end,
    },
    [302] = {
        name = "topic.create",
        fields = {
            request = {
                stream_id_kind = ProtoField.uint8("iggy.create_topic.req.stream_id_kind", "Stream ID Kind", base.DEC),
                stream_id_length = ProtoField.uint8("iggy.create_topic.req.stream_id_length", "Stream ID Length", base.DEC),
                stream_id_value_numeric = ProtoField.uint32("iggy.create_topic.req.stream_id_value_numeric", "Stream ID Value (Numeric)", base.DEC),
                stream_id_value_string = ProtoField.string("iggy.create_topic.req.stream_id_value_string", "Stream ID Value (String)"),
                topic_id = ProtoField.uint32("iggy.create_topic.req.topic_id", "Topic ID", base.DEC),
                partitions_count = ProtoField.uint32("iggy.create_topic.req.partitions_count", "Partitions Count", base.DEC),
                compression_algorithm = ProtoField.uint8("iggy.create_topic.req.compression_algorithm", "Compression Algorithm", base.DEC),
                message_expiry = ProtoField.uint64("iggy.create_topic.req.message_expiry", "Message Expiry (μs)", base.DEC),
                max_topic_size = ProtoField.uint64("iggy.create_topic.req.max_topic_size", "Max Topic Size (bytes)", base.DEC),
                replication_factor = ProtoField.uint8("iggy.create_topic.req.replication_factor", "Replication Factor", base.DEC),
                name_len = ProtoField.uint8("iggy.create_topic.req.name_len", "Name Length", base.DEC),
                name = ProtoField.string("iggy.create_topic.req.name", "Name"),
            },
            response = {
                topic_id = ProtoField.uint32("iggy.create_topic.resp.topic_id", "Topic ID", base.DEC),
                created_at = ProtoField.uint64("iggy.create_topic.resp.created_at", "Created At (μs)", base.DEC),
                partitions_count = ProtoField.uint32("iggy.create_topic.resp.partitions_count", "Partitions Count", base.DEC),
                message_expiry = ProtoField.uint64("iggy.create_topic.resp.message_expiry", "Message Expiry (μs)", base.DEC),
                compression_algorithm = ProtoField.uint8("iggy.create_topic.resp.compression_algorithm", "Compression Algorithm", base.DEC),
                max_topic_size = ProtoField.uint64("iggy.create_topic.resp.max_topic_size", "Max Topic Size (bytes)", base.DEC),
                replication_factor = ProtoField.uint8("iggy.create_topic.resp.replication_factor", "Replication Factor", base.DEC),
                size = ProtoField.uint64("iggy.create_topic.resp.size", "Size (bytes)", base.DEC),
                messages_count = ProtoField.uint64("iggy.create_topic.resp.messages_count", "Messages Count", base.DEC),
                name_len = ProtoField.uint8("iggy.create_topic.resp.name_len", "Name Length", base.DEC),
                name = ProtoField.string("iggy.create_topic.resp.name", "Name"),
            },
        },
        request_payload_dissector = function(self, buffer, tree, offset)
            local f = self.fields.request

            local stream_id_kind = buffer(offset, 1):uint()
            local kind_item = tree:add(f.stream_id_kind, buffer(offset, 1))
            if stream_id_kind == 1 then
                kind_item:set_generated()
                kind_item:append_text(" (Numeric)")
            elseif stream_id_kind == 2 then
                kind_item:set_generated()
                kind_item:append_text(" (String)")
            end
            offset = offset + 1

            local stream_id_length = buffer(offset, 1):uint()
            tree:add(f.stream_id_length, buffer(offset, 1))
            offset = offset + 1

            if stream_id_kind == 1 then
                tree:add_le(f.stream_id_value_numeric, buffer(offset, stream_id_length))
            else
                tree:add(f.stream_id_value_string, buffer(offset, stream_id_length))
            end
            offset = offset + stream_id_length

            tree:add_le(f.topic_id, buffer(offset, 4))
            offset = offset + 4
            tree:add_le(f.partitions_count, buffer(offset, 4))
            offset = offset + 4
            tree:add(f.compression_algorithm, buffer(offset, 1))
            offset = offset + 1
            tree:add_le(f.message_expiry, buffer(offset, 8))
            offset = offset + 8
            tree:add_le(f.max_topic_size, buffer(offset, 8))
            offset = offset + 8
            tree:add(f.replication_factor, buffer(offset, 1))
            offset = offset + 1

            local name_len = buffer(offset, 1):uint()
            tree:add(f.name_len, buffer(offset, 1))
            offset = offset + 1
            tree:add(f.name, buffer(offset, name_len))
        end,
        response_payload_dissector = function(self, buffer, tree, offset)
            local f = self.fields.response

            tree:add_le(f.topic_id, buffer(offset, 4))
            offset = offset + 4
            tree:add_le(f.created_at, buffer(offset, 8))
            offset = offset + 8
            tree:add_le(f.partitions_count, buffer(offset, 4))
            offset = offset + 4
            tree:add_le(f.message_expiry, buffer(offset, 8))
            offset = offset + 8
            tree:add(f.compression_algorithm, buffer(offset, 1))
            offset = offset + 1
            tree:add_le(f.max_topic_size, buffer(offset, 8))
            offset = offset + 8
            tree:add(f.replication_factor, buffer(offset, 1))
            offset = offset + 1
            tree:add_le(f.size, buffer(offset, 8))
            offset = offset + 8
            tree:add_le(f.messages_count, buffer(offset, 8))
            offset = offset + 8

            local name_len = buffer(offset, 1):uint()
            tree:add(f.name_len, buffer(offset, 1))
            offset = offset + 1
            tree:add(f.name, buffer(offset, name_len))
        end,
    },
}

for code, cmd in pairs(COMMANDS) do
    assert(type(code) == "number", "Command code must be number")
    assert(type(cmd.name) == "string" and cmd.name:match("%S"),
        string.format("Command %d: name must be non-empty string", code))
    assert(type(cmd.request_payload_dissector) == "function",
        string.format(
            "Command %d (%s): request_payload_dissector must be function (found %s); use empty fn if no payload",
            code, cmd.name, type(cmd.request_payload_dissector)))
    assert(type(cmd.response_payload_dissector) == "function",
        string.format(
            "Command %d (%s): response_payload_dissector must be function (found %s); use empty fn if no payload",
            code, cmd.name, type(cmd.response_payload_dissector)))
    assert(type(cmd.fields) == "table",
        string.format("Command %d (%s): fields must be table (found %s); use {} if no fields",
            code, cmd.name, type(cmd.fields)))
    assert(type(cmd.fields.request) == "table",
        string.format("Command %d (%s): fields.request must be table (found %s); use {} if none",
            code, cmd.name, type(cmd.fields.request)))
    assert(type(cmd.fields.response) == "table",
        string.format("Command %d (%s): fields.response must be table (found %s); use {} if none",
            code, cmd.name, type(cmd.fields.response)))
end

local all_fields = {}

local function collect_fields(tbl, result)
    for _, value in pairs(tbl) do
        if type(value) == "table" then
            collect_fields(value, result)
        else
            table.insert(result, value)
        end
    end
end

for _, field in pairs(common_fields) do
    table.insert(all_fields, field)
end

for _code, cmd in pairs(COMMANDS) do
    if cmd.fields then
        collect_fields(cmd.fields, all_fields)
    end
end

iggy.fields = all_fields

local STATUS_CODES = {
    [0] = "ok",
    [1] = "error",
    [2] = "invalid_configuration",
    [3] = "invalid_command",
    [4] = "invalid_format",
    [5] = "feature_unavailable",
    [6] = "invalid_identifier",
    [8] = "disconnected",
    [40] = "unauthenticated",
    [41] = "unauthorized",
    [42] = "invalid_credentials",
}

-- Request-Response Tracker for pipelined protocols using Conversation API (Wireshark 4.6+)
local ReqRespTracker = {}
ReqRespTracker.__index = ReqRespTracker

function ReqRespTracker.new(proto)
    local self = setmetatable({}, ReqRespTracker)
    self.proto = proto
    return self
end

function ReqRespTracker:record_request(pinfo, command_code)
    if not pinfo.conversation then return false end

    local conv = pinfo.conversation
    local conv_data = conv[self.proto]

    if not conv_data then
        conv_data = {
            queue = {first = 0, last = -1},
            matched = {}
        }
    end

    if not pinfo.visited then
        local last = conv_data.queue.last + 1
        conv_data.queue.last = last
        conv_data.queue[last] = {
            command_code = command_code,
            frame_num = pinfo.number
        }
    end

    conv[self.proto] = conv_data
    return true
end

function ReqRespTracker:find_request(pinfo)
    if not pinfo.conversation then return nil end

    local conv = pinfo.conversation
    local conv_data = conv[self.proto]
    if not conv_data then return nil end

    local resp_frame_num = pinfo.number

    if conv_data.matched[resp_frame_num] then
        return conv_data.matched[resp_frame_num]
    end

    if not pinfo.visited then
        local queue = conv_data.queue
        local first = queue.first

        if first > queue.last then return nil end

        local request_data = queue[first]
        queue[first] = nil
        queue.first = first + 1

        conv_data.matched[resp_frame_num] = request_data
        conv[self.proto] = conv_data

        return request_data
    else
        return conv_data.matched[resp_frame_num]
    end
end

local request_tracker = ReqRespTracker.new(iggy)

function iggy.dissector(buffer, pinfo, tree)
    pinfo.cols.protocol:set("IGGY")

    local buflen = buffer:len()
    local server_port = iggy.prefs.server_port
    local is_request = (pinfo.dst_port == server_port)
    local is_response = (pinfo.src_port == server_port)
    local cf = common_fields

    -- TCP Desegmentation
    if buflen < 8 then
        pinfo.desegment_len = DESEGMENT_ONE_MORE_SEGMENT
        return
    end

    local total_len
    if is_request then
        local length_field = buffer(0, 4):le_uint()
        total_len = 4 + length_field
    elseif is_response then
        local length_field = buffer(4, 4):le_uint()
        total_len = 8 + length_field
    end

    if buflen < total_len then
        pinfo.desegment_len = total_len - buflen
        return
    end

    local HEADER_SIZE = 8
    local payload_offset = HEADER_SIZE
    local payload_len = total_len - HEADER_SIZE

    if is_request then
        local length = buffer(0, 4):le_uint()
        local command_code = buffer(4, 4):le_uint()

        local subtree = tree:add(iggy, buffer(0, total_len), "Iggy Protocol - Request")
        subtree:add(cf.message_type, "Request"):set_generated()

        subtree:add_le(cf.req_length, buffer(0, 4))
        subtree:add_le(cf.req_command, buffer(4, 4))

        local command_info = COMMANDS[command_code]
        if not command_info then
            local command_name = string.format("Unimplemented (%d)", command_code)
            subtree:add(cf.req_command_name, command_name):set_generated()

            if payload_len > 0 then
                local payload_tree = subtree:add(cf.req_payload_tree, buffer(payload_offset, payload_len))
                payload_tree:set_text("Payload")
            end

            request_tracker:record_request(pinfo, command_code)
            pinfo.cols.info:set(string.format("Request: %s (length=%d)", command_name, length))
            return total_len
        end

        local command_name = command_info.name
        subtree:add(cf.req_command_name, command_name):set_generated()

        if payload_len > 0 then
            local payload_tree = subtree:add(cf.req_payload_tree, buffer(payload_offset, payload_len))
            payload_tree:set_text("Payload")
            command_info.request_payload_dissector(command_info, buffer, payload_tree, payload_offset)
        end

        request_tracker:record_request(pinfo, command_code)
        pinfo.cols.info:set(string.format("Request: %s (code=%d, length=%d)", command_name, command_code, length))
    elseif is_response then
        local status_code = buffer(0, 4):le_uint()
        local length = buffer(4, 4):le_uint()

        local subtree = tree:add(iggy, buffer(0, total_len), "Iggy Protocol - Response")
        subtree:add(cf.message_type, "Response"):set_generated()

        subtree:add_le(cf.resp_status, buffer(0, 4))
        subtree:add_le(cf.resp_length, buffer(4, 4))

        local status_name = STATUS_CODES[status_code] or (status_code == 0 and "ok" or string.format("Error(%d)", status_code))
        subtree:add(cf.resp_status_name, status_name):set_generated()

        local request_data = request_tracker:find_request(pinfo)
        local command_code = request_data and request_data.command_code
        local request_frame_num = request_data and request_data.frame_num
        local command_info = command_code and COMMANDS[command_code]

        if request_frame_num then
            subtree:add(cf.request_frame, request_frame_num)
        end

        if not command_info then
            local command_name
            if command_code then
                command_name = string.format("Unimplemented (%d)", command_code)
            else
                command_name = "No matching request"
            end

            subtree:add(cf.req_command_name, command_name):set_generated()

            if payload_len > 0 then
                local payload_tree = subtree:add(cf.resp_payload_tree, buffer(payload_offset, payload_len))
                payload_tree:set_text("Payload")
            end

            if status_code == 0 then
                pinfo.cols.info:set(string.format("Response: %s ok (length=%d)", command_name, length))
            else
                pinfo.cols.info:set(string.format("Response: %s %s (status=%d, length=%d)",
                    command_name, status_name, status_code, length))
            end
            return total_len
        end

        local command_name = command_info.name
        subtree:add(cf.req_command_name, command_name):set_generated()

        if payload_len > 0 and status_code == 0 then
            local payload_tree = subtree:add(cf.resp_payload_tree, buffer(payload_offset, payload_len))
            payload_tree:set_text("Payload")
            command_info.response_payload_dissector(command_info, buffer, payload_tree, payload_offset)
        end

        if status_code == 0 then
            pinfo.cols.info:set(string.format("Response: %s ok (length=%d)", command_name, length))
        else
            pinfo.cols.info:set(string.format("Response: %s %s (status=%d, length=%d)",
                command_name, status_name, status_code, length))
        end
    end

    return total_len
end

local current_port = 0

function iggy.prefs_changed()
    local tcp_port = DissectorTable.get("tcp.port")

    if current_port ~= iggy.prefs.server_port then
        if current_port > 0 then
            tcp_port:remove(current_port, iggy)
        end

        current_port = iggy.prefs.server_port
        if current_port > 0 then
            tcp_port:add(current_port, iggy)
        end
    end
end

DissectorTable.get("tcp.port"):add(iggy.prefs.server_port, iggy)
current_port = iggy.prefs.server_port
