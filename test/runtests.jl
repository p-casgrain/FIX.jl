using FIX
using DataStructures: OrderedDict
using Test

@test FIX.checksum("123ioн") == 171

smpl = Dict(1 => "g", 2=> "h") |> OrderedDict
@test FIX.fixjoin(smpl, Char(1)) == "2=h" * Char(1) * "1=g" * Char(1)
smpl_back = FIX.fixparse(UInt8.([x for x in FIX.fixjoin(smpl, Char(1))]))[0]
@test smpl_back == smpl
@test FIX.fixconvert(FIX.TAGS_INT_STRING, smpl_back) == Dict("AdvId" => "h", "Account" => "g")

@btime FIX.fixconvert(FIX.TAGS_INT_STRING, smpl_back) == Dict("AdvId" => "h", "Account" => "g")



