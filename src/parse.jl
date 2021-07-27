# Iteraor-Based FIX Parser Object
struct SingleMsgFixIterator{T<:AbstractArray{UInt8}}
  data::T
end

# Constructor Method
SingleMsgFixIterator(x::T) where {T<:AbstractArray{UInt8}} = SingleMsgFixIterator{T}(x)

IteratorSize(::SingleMsgFixIterator) = SizeUnknown()
eltype(::SingleMsgFixIterator) = Pair{Int64,String}

# Iteration interface
function Base.iterate(msg_obj::SingleMsgFixIterator,ix1::Int64=1)
  ix0 = ix1 # Front and back of selection range
  tag_no = Int64(0)
  while ix1 <= length(msg_obj.data)
    if msg_obj.data[ix1] == 0x3d # cur_char is '='
      @inbounds tag_no = parse(Int64,String(@view msg_obj.data[ix0:ix1-1])) # store all of left of range
      ix0, ix1 = ix1+1, ix1+1 # shift both range markers one ahead
    elseif msg_obj.data[ix1] == 0x01 # cur_char is tag separator
      @inbounds tag_val = String(@view msg_obj.data[ix0:ix1-1]) # store all of left of range
      return ( tag_no => tag_val , ix1+1 ) # end of tag encountered, return tag pair and next index
    else # other char encountered
      ix1 += 1 # shift right range marker ahead
    end
  end
  return nothing
end

# Parse by passing iterator to an OrderedDict
fixparse(x::T) where {T<:AbstractArray{UInt8}} = OrderedDict( SingleMsgFixIterator(x) )
fixparse(x::String) = myfixparse(Vector{UInt8}(x))

# Convert FIX message keys from Int64 to string
fixconvert(tags::Dict{Int64, String}, msg::AbstractDict{Int64, String}) = OrderedDict( (tags[k],v) for (k,v) in msg )
