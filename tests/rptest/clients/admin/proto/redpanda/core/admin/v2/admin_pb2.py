"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(_runtime_version.Domain.PUBLIC, 5, 29, 0, '', 'proto/redpanda/core/admin/v2/admin.proto')
_sym_db = _symbol_database.Default()
from ......proto.redpanda.pbgen import options_pb2 as proto_dot_redpanda_dot_pbgen_dot_options__pb2
from ......proto.redpanda.pbgen import rpc_pb2 as proto_dot_redpanda_dot_pbgen_dot_rpc__pb2
DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n(proto/redpanda/core/admin/v2/admin.proto\x12\x16redpanda.core.admin.v2\x1a"proto/redpanda/pbgen/options.proto\x1a\x1eproto/redpanda/pbgen/rpc.proto"\x16\n\x14ListBuildInfoRequest"O\n\x15ListBuildInfoResponse\x126\n\x0bbuild_infos\x18\x01 \x03(\x0b2!.redpanda.core.admin.v2.BuildInfo"@\n\tBuildInfo\x12\x0f\n\x07node_id\x18\x01 \x01(\x05\x12\x0f\n\x07version\x18\x02 \x01(\t\x12\x11\n\tbuild_sha\x18\x03 \x01(\t",\n\x08RPCRoute\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\x12\n\nhttp_route\x18\x02 \x01(\t"\'\n\x14ListRPCRoutesRequest\x12\x0f\n\x07node_id\x18\x01 \x01(\x05"I\n\x15ListRPCRoutesResponse\x120\n\x06routes\x18\x01 \x03(\x0b2 .redpanda.core.admin.v2.RPCRoute2\xfa\x01\n\x0cAdminService\x12t\n\rListBuildInfo\x12,.redpanda.core.admin.v2.ListBuildInfoRequest\x1a-.redpanda.core.admin.v2.ListBuildInfoResponse"\x06\xea\x92\x19\x02\x10\x03\x12t\n\rListRPCRoutes\x12,.redpanda.core.admin.v2.ListRPCRoutesRequest\x1a-.redpanda.core.admin.v2.ListRPCRoutesResponse"\x06\xea\x92\x19\x02\x10\x03B\x10\xea\x92\x19\x0cproto::adminb\x06proto3')
_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'proto.redpanda.core.admin.v2.admin_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
    _globals['DESCRIPTOR']._loaded_options = None
    _globals['DESCRIPTOR']._serialized_options = b'\xea\x92\x19\x0cproto::admin'
    _globals['_ADMINSERVICE'].methods_by_name['ListBuildInfo']._loaded_options = None
    _globals['_ADMINSERVICE'].methods_by_name['ListBuildInfo']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_ADMINSERVICE'].methods_by_name['ListRPCRoutes']._loaded_options = None
    _globals['_ADMINSERVICE'].methods_by_name['ListRPCRoutes']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_LISTBUILDINFOREQUEST']._serialized_start = 136
    _globals['_LISTBUILDINFOREQUEST']._serialized_end = 158
    _globals['_LISTBUILDINFORESPONSE']._serialized_start = 160
    _globals['_LISTBUILDINFORESPONSE']._serialized_end = 239
    _globals['_BUILDINFO']._serialized_start = 241
    _globals['_BUILDINFO']._serialized_end = 305
    _globals['_RPCROUTE']._serialized_start = 307
    _globals['_RPCROUTE']._serialized_end = 351
    _globals['_LISTRPCROUTESREQUEST']._serialized_start = 353
    _globals['_LISTRPCROUTESREQUEST']._serialized_end = 392
    _globals['_LISTRPCROUTESRESPONSE']._serialized_start = 394
    _globals['_LISTRPCROUTESRESPONSE']._serialized_end = 467
    _globals['_ADMINSERVICE']._serialized_start = 470
    _globals['_ADMINSERVICE']._serialized_end = 720