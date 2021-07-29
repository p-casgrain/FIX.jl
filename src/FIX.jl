module FIX

using DataStructures: OrderedDict, CircularBuffer
using DandelionWebSockets
using Dates
import Base: length, collect

abstract type AbstractMessageHandler <: DandelionWebSockets.WebSocketHandler end
export AbstractMessageHandler, FIXClient, send_message, start, close
export onFIXMessage

global const TAGS_INT_STRING = Dict{Int64, String}()
global const TAGS_STRING_INT = Dict{String, Int64}()


function onFIXMessage(this::AbstractMessageHandler, x::Any)
    T = typeof(this)
    X = typeof(x)
    throw(ErrorException("Method `onFIXMessage` is not implemented by $T for argument type $X"))
end

"Load FIX tags as vector of tuples from csv file at path `csv_path`"
function load_fix_tags(csv_path)
    fid = open(joinpath(@__DIR__, csv_path), "r");
    line_number = 0
    tagval_tpl_vec = Vector{Pair{Int64,String}}()
    while  !eof(fid)
        line_number += 1
        line = readline(fid);
        data = split(line, ",");
        if length(data) != 2
            close(fid)
            throw(ErrorException("Invalid data in 'etc/tags.csv' file on line $line_number: $line"))
        end
        tag = parse(Int64, String(data[1]))
        val = String(data[2])
        Base.push!(tagval_tpl_vec,(tag => val))
    end
    close(fid)
    return tagval_tpl_vec
end


function __init__()
    # load FIX tags
    tagval_tpl_vec = load_fix_tags("../etc/tags.csv")
    global TAGS_INT_STRING = Dict( x => y for (x,y) in tagval_tpl_vec )
    global TAGS_STRING_INT = Dict( y => x for (x,y) in tagval_tpl_vec )
    return
end

mutable struct FIXClientTasks
    read::Union{Task,Nothing}
end

FIXClientTasks() = FIXClientTasks(nothing)

include("parse.jl")
include("management.jl")

struct FIXClient{T <: IO, H <: AbstractMessageHandler}
    stream::T
    handler::H
    delimiter::Char
    m_head::Dict{Int64, String}
    m_tasks::FIXClientTasks
    m_messages::FIXMessageManagement
    m_lock::ReentrantLock
    function FIXClient(stream::T,
                        handler::H,
                        header::Dict{Int64, String},
                        ratelimit::RateLimit;
                        delimiter::Char = Char(1)) where {T,H}
        return new{T, H}(stream,
                        handler,
                        delimiter,
                        header,
                        FIXClientTasks(),
                        FIXMessageManagement(ratelimit),
                        ReentrantLock())
    end
end

checksum(chk_str::String)::Int64 = sum([Int(x) for x in chk_str]) % 256
fixjoin(msg_dct::AbstractDict{Int64, String}, delimiter::Char)::String = join([string(k) * "=" * v for (k, v) in msg_dct], delimiter) * delimiter

function fixmessage(client::FIXClient, msg::Dict{Int64, String})::OrderedDict{Int64, String}
    ordered = OrderedDict{Int64, String}()
    sizehint!(ordered,length(client.m_head) + length(msg) + 4)
    #header
    ordered[8] = client.m_head[8]
    ordered[9] = ""
    ordered[35] = msg[35] #message type
    ordered[49] = client.m_head[49] #SenderCompID
    ordered[56] = client.m_head[56] #TargetCompID
    ordered[34] = getNextOutgoingMsgSeqNum(client)
    ordered[52] = ""
    #body
    body_length = 0
    for (k, v) in msg
        if k != 8 && k != 9 && k != 10
            ordered[k] = v
        end
    end

    for (k, v) in ordered
        if k != 8 && k != 9
            body_length += length(string(k)) + 1 + length(v) + 1 #tag=value|
        end
    end

    ordered[9] = string(body_length)

    #tail
    msg = fixjoin(ordered, client.delimiter)
    c = checksum(msg)
    c_str = string(c)
    while length(c_str) < 3
        c_str = '0' * c_str
    end
    ordered[10] = c_str

    #done
    return ordered
end

function send_message_fake(this::FIXClient, msg::Dict{Int64, String})
    lock(this.m_lock)

    msg = fixmessage(this, msg)
    msg_str = fixjoin(msg, this.delimiter)
    # write(this.stream, msg_str)
    onSent(this.m_messages, msg)

    unlock(this.m_lock)

    return (msg, msg_str)
end

function send_message(this::FIXClient, msg::Dict{Int64, String})
    lock(this.m_lock)

    msg = fixmessage(this, msg)
    msg_str = fixjoin(msg, this.delimiter)
    write(this.stream, msg_str)
    onSent(this.m_messages, msg)

    unlock(this.m_lock)

    return (msg, msg_str)
end

function start(this::FIXClient)
    this.m_tasks.read = @async begin
        while true
            incoming = readavailable(this.stream)
            if isempty(incoming)
                @printf("[%ls] EMPTY FIX MESSAGE\n", now())
                break
            end

            for (_, msg) in fixparse(incoming)
                onGet(this, msg)
                onFIXMessage(this.handler, msg)
            end
        end
        @printf("[%ls] FIX: read task done\n", now())
    end
    
    return this.m_tasks
end

close(this::FIXClient) = close(this.stream)

onGet(this::FIXClient, msg::DICTMSG) = onGet(this.m_messages, msg)

getOpenOrders(this::FIXClient) = getOpenOrders(this.m_messages)

getPosition(this::FIXClient, instrument::String) = getPosition(this.m_messages, instrument)
getPositions(this::FIXClient) = getPositions(this.m_messages)

numMsgsLeftToSend(this::FIXClient) = numleft(this.m_messages.outgoing.ratelimit, now())

getNextOutgoingMsgSeqNum(this::FIXClient) = getNextOutgoingMsgSeqNum(this.m_messages)

end