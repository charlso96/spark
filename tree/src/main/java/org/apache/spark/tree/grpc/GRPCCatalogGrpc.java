package org.apache.spark.tree.grpc;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
        value = "by gRPC proto compiler (version 1.47.0)",
        comments = "Source: grpccatalog.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class GRPCCatalogGrpc {

    private GRPCCatalogGrpc() {}

    public static final String SERVICE_NAME = "GRPCCatalog";

    // Static method descriptors that strictly reflect the proto.
    private static volatile io.grpc.MethodDescriptor<Grpccatalog.StartTxnRequest,
            Grpccatalog.StartTxnResponse> getStartTxnMethod;

    @io.grpc.stub.annotations.RpcMethod(
            fullMethodName = SERVICE_NAME + '/' + "StartTxn",
            requestType = Grpccatalog.StartTxnRequest.class,
            responseType = Grpccatalog.StartTxnResponse.class,
            methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
    public static io.grpc.MethodDescriptor<Grpccatalog.StartTxnRequest,
            Grpccatalog.StartTxnResponse> getStartTxnMethod() {
        io.grpc.MethodDescriptor<Grpccatalog.StartTxnRequest, Grpccatalog.StartTxnResponse> getStartTxnMethod;
        if ((getStartTxnMethod = GRPCCatalogGrpc.getStartTxnMethod) == null) {
            synchronized (GRPCCatalogGrpc.class) {
                if ((getStartTxnMethod = GRPCCatalogGrpc.getStartTxnMethod) == null) {
                    GRPCCatalogGrpc.getStartTxnMethod = getStartTxnMethod =
                            io.grpc.MethodDescriptor.<Grpccatalog.StartTxnRequest, Grpccatalog.StartTxnResponse>newBuilder()
                                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                                    .setFullMethodName(generateFullMethodName(SERVICE_NAME, "StartTxn"))
                                    .setSampledToLocalTracing(true)
                                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                            Grpccatalog.StartTxnRequest.getDefaultInstance()))
                                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                            Grpccatalog.StartTxnResponse.getDefaultInstance()))
                                    .setSchemaDescriptor(new GRPCCatalogMethodDescriptorSupplier("StartTxn"))
                                    .build();
                }
            }
        }
        return getStartTxnMethod;
    }

    private static volatile io.grpc.MethodDescriptor<Grpccatalog.SnapshotRequest,
            Grpccatalog.SnapshotResponse> getSnapshotMethod;

    @io.grpc.stub.annotations.RpcMethod(
            fullMethodName = SERVICE_NAME + '/' + "Snapshot",
            requestType = Grpccatalog.SnapshotRequest.class,
            responseType = Grpccatalog.SnapshotResponse.class,
            methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
    public static io.grpc.MethodDescriptor<Grpccatalog.SnapshotRequest,
            Grpccatalog.SnapshotResponse> getSnapshotMethod() {
        io.grpc.MethodDescriptor<Grpccatalog.SnapshotRequest, Grpccatalog.SnapshotResponse> getSnapshotMethod;
        if ((getSnapshotMethod = GRPCCatalogGrpc.getSnapshotMethod) == null) {
            synchronized (GRPCCatalogGrpc.class) {
                if ((getSnapshotMethod = GRPCCatalogGrpc.getSnapshotMethod) == null) {
                    GRPCCatalogGrpc.getSnapshotMethod = getSnapshotMethod =
                            io.grpc.MethodDescriptor.<Grpccatalog.SnapshotRequest, Grpccatalog.SnapshotResponse>newBuilder()
                                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                                    .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Snapshot"))
                                    .setSampledToLocalTracing(true)
                                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                            Grpccatalog.SnapshotRequest.getDefaultInstance()))
                                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                            Grpccatalog.SnapshotResponse.getDefaultInstance()))
                                    .setSchemaDescriptor(new GRPCCatalogMethodDescriptorSupplier("Snapshot"))
                                    .build();
                }
            }
        }
        return getSnapshotMethod;
    }

    private static volatile io.grpc.MethodDescriptor<Grpccatalog.CloneRequest,
            Grpccatalog.CloneResponse> getCloneMethod;

    @io.grpc.stub.annotations.RpcMethod(
            fullMethodName = SERVICE_NAME + '/' + "Clone",
            requestType = Grpccatalog.CloneRequest.class,
            responseType = Grpccatalog.CloneResponse.class,
            methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
    public static io.grpc.MethodDescriptor<Grpccatalog.CloneRequest,
            Grpccatalog.CloneResponse> getCloneMethod() {
        io.grpc.MethodDescriptor<Grpccatalog.CloneRequest, Grpccatalog.CloneResponse> getCloneMethod;
        if ((getCloneMethod = GRPCCatalogGrpc.getCloneMethod) == null) {
            synchronized (GRPCCatalogGrpc.class) {
                if ((getCloneMethod = GRPCCatalogGrpc.getCloneMethod) == null) {
                    GRPCCatalogGrpc.getCloneMethod = getCloneMethod =
                            io.grpc.MethodDescriptor.<Grpccatalog.CloneRequest, Grpccatalog.CloneResponse>newBuilder()
                                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                                    .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Clone"))
                                    .setSampledToLocalTracing(true)
                                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                            Grpccatalog.CloneRequest.getDefaultInstance()))
                                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                            Grpccatalog.CloneResponse.getDefaultInstance()))
                                    .setSchemaDescriptor(new GRPCCatalogMethodDescriptorSupplier("Clone"))
                                    .build();
                }
            }
        }
        return getCloneMethod;
    }

    private static volatile io.grpc.MethodDescriptor<Grpccatalog.GetGarbageRequest,
            Grpccatalog.GetGarbageResponse> getGetGarbageMethod;

    @io.grpc.stub.annotations.RpcMethod(
            fullMethodName = SERVICE_NAME + '/' + "GetGarbage",
            requestType = Grpccatalog.GetGarbageRequest.class,
            responseType = Grpccatalog.GetGarbageResponse.class,
            methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
    public static io.grpc.MethodDescriptor<Grpccatalog.GetGarbageRequest,
            Grpccatalog.GetGarbageResponse> getGetGarbageMethod() {
        io.grpc.MethodDescriptor<Grpccatalog.GetGarbageRequest, Grpccatalog.GetGarbageResponse> getGetGarbageMethod;
        if ((getGetGarbageMethod = GRPCCatalogGrpc.getGetGarbageMethod) == null) {
            synchronized (GRPCCatalogGrpc.class) {
                if ((getGetGarbageMethod = GRPCCatalogGrpc.getGetGarbageMethod) == null) {
                    GRPCCatalogGrpc.getGetGarbageMethod = getGetGarbageMethod =
                            io.grpc.MethodDescriptor.<Grpccatalog.GetGarbageRequest, Grpccatalog.GetGarbageResponse>newBuilder()
                                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                                    .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetGarbage"))
                                    .setSampledToLocalTracing(true)
                                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                            Grpccatalog.GetGarbageRequest.getDefaultInstance()))
                                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                            Grpccatalog.GetGarbageResponse.getDefaultInstance()))
                                    .setSchemaDescriptor(new GRPCCatalogMethodDescriptorSupplier("GetGarbage"))
                                    .build();
                }
            }
        }
        return getGetGarbageMethod;
    }

    private static volatile io.grpc.MethodDescriptor<Grpccatalog.ClearGarbageRequest,
            Grpccatalog.ClearGarbageResponse> getClearGarbageMethod;

    @io.grpc.stub.annotations.RpcMethod(
            fullMethodName = SERVICE_NAME + '/' + "ClearGarbage",
            requestType = Grpccatalog.ClearGarbageRequest.class,
            responseType = Grpccatalog.ClearGarbageResponse.class,
            methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
    public static io.grpc.MethodDescriptor<Grpccatalog.ClearGarbageRequest,
            Grpccatalog.ClearGarbageResponse> getClearGarbageMethod() {
        io.grpc.MethodDescriptor<Grpccatalog.ClearGarbageRequest, Grpccatalog.ClearGarbageResponse> getClearGarbageMethod;
        if ((getClearGarbageMethod = GRPCCatalogGrpc.getClearGarbageMethod) == null) {
            synchronized (GRPCCatalogGrpc.class) {
                if ((getClearGarbageMethod = GRPCCatalogGrpc.getClearGarbageMethod) == null) {
                    GRPCCatalogGrpc.getClearGarbageMethod = getClearGarbageMethod =
                            io.grpc.MethodDescriptor.<Grpccatalog.ClearGarbageRequest, Grpccatalog.ClearGarbageResponse>newBuilder()
                                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                                    .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ClearGarbage"))
                                    .setSampledToLocalTracing(true)
                                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                            Grpccatalog.ClearGarbageRequest.getDefaultInstance()))
                                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                            Grpccatalog.ClearGarbageResponse.getDefaultInstance()))
                                    .setSchemaDescriptor(new GRPCCatalogMethodDescriptorSupplier("ClearGarbage"))
                                    .build();
                }
            }
        }
        return getClearGarbageMethod;
    }

    private static volatile io.grpc.MethodDescriptor<Grpccatalog.DefineTypeRequest,
            Grpccatalog.DefineTypeResponse> getDefineTypeMethod;

    @io.grpc.stub.annotations.RpcMethod(
            fullMethodName = SERVICE_NAME + '/' + "DefineType",
            requestType = Grpccatalog.DefineTypeRequest.class,
            responseType = Grpccatalog.DefineTypeResponse.class,
            methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
    public static io.grpc.MethodDescriptor<Grpccatalog.DefineTypeRequest,
            Grpccatalog.DefineTypeResponse> getDefineTypeMethod() {
        io.grpc.MethodDescriptor<Grpccatalog.DefineTypeRequest, Grpccatalog.DefineTypeResponse> getDefineTypeMethod;
        if ((getDefineTypeMethod = GRPCCatalogGrpc.getDefineTypeMethod) == null) {
            synchronized (GRPCCatalogGrpc.class) {
                if ((getDefineTypeMethod = GRPCCatalogGrpc.getDefineTypeMethod) == null) {
                    GRPCCatalogGrpc.getDefineTypeMethod = getDefineTypeMethod =
                            io.grpc.MethodDescriptor.<Grpccatalog.DefineTypeRequest, Grpccatalog.DefineTypeResponse>newBuilder()
                                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                                    .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DefineType"))
                                    .setSampledToLocalTracing(true)
                                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                            Grpccatalog.DefineTypeRequest.getDefaultInstance()))
                                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                            Grpccatalog.DefineTypeResponse.getDefaultInstance()))
                                    .setSchemaDescriptor(new GRPCCatalogMethodDescriptorSupplier("DefineType"))
                                    .build();
                }
            }
        }
        return getDefineTypeMethod;
    }

    private static volatile io.grpc.MethodDescriptor<Grpccatalog.ExecuteQueryRequest,
            Grpccatalog.ExecuteQueryResponse> getExecuteQueryMethod;

    @io.grpc.stub.annotations.RpcMethod(
            fullMethodName = SERVICE_NAME + '/' + "ExecuteQuery",
            requestType = Grpccatalog.ExecuteQueryRequest.class,
            responseType = Grpccatalog.ExecuteQueryResponse.class,
            methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
    public static io.grpc.MethodDescriptor<Grpccatalog.ExecuteQueryRequest,
            Grpccatalog.ExecuteQueryResponse> getExecuteQueryMethod() {
        io.grpc.MethodDescriptor<Grpccatalog.ExecuteQueryRequest, Grpccatalog.ExecuteQueryResponse> getExecuteQueryMethod;
        if ((getExecuteQueryMethod = GRPCCatalogGrpc.getExecuteQueryMethod) == null) {
            synchronized (GRPCCatalogGrpc.class) {
                if ((getExecuteQueryMethod = GRPCCatalogGrpc.getExecuteQueryMethod) == null) {
                    GRPCCatalogGrpc.getExecuteQueryMethod = getExecuteQueryMethod =
                            io.grpc.MethodDescriptor.<Grpccatalog.ExecuteQueryRequest, Grpccatalog.ExecuteQueryResponse>newBuilder()
                                    .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
                                    .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ExecuteQuery"))
                                    .setSampledToLocalTracing(true)
                                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                            Grpccatalog.ExecuteQueryRequest.getDefaultInstance()))
                                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                            Grpccatalog.ExecuteQueryResponse.getDefaultInstance()))
                                    .setSchemaDescriptor(new GRPCCatalogMethodDescriptorSupplier("ExecuteQuery"))
                                    .build();
                }
            }
        }
        return getExecuteQueryMethod;
    }

    private static volatile io.grpc.MethodDescriptor<Grpccatalog.CommitRequest,
            Grpccatalog.CommitResponse> getCommitMethod;

    @io.grpc.stub.annotations.RpcMethod(
            fullMethodName = SERVICE_NAME + '/' + "Commit",
            requestType = Grpccatalog.CommitRequest.class,
            responseType = Grpccatalog.CommitResponse.class,
            methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
    public static io.grpc.MethodDescriptor<Grpccatalog.CommitRequest,
            Grpccatalog.CommitResponse> getCommitMethod() {
        io.grpc.MethodDescriptor<Grpccatalog.CommitRequest, Grpccatalog.CommitResponse> getCommitMethod;
        if ((getCommitMethod = GRPCCatalogGrpc.getCommitMethod) == null) {
            synchronized (GRPCCatalogGrpc.class) {
                if ((getCommitMethod = GRPCCatalogGrpc.getCommitMethod) == null) {
                    GRPCCatalogGrpc.getCommitMethod = getCommitMethod =
                            io.grpc.MethodDescriptor.<Grpccatalog.CommitRequest, Grpccatalog.CommitResponse>newBuilder()
                                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                                    .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Commit"))
                                    .setSampledToLocalTracing(true)
                                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                            Grpccatalog.CommitRequest.getDefaultInstance()))
                                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                            Grpccatalog.CommitResponse.getDefaultInstance()))
                                    .setSchemaDescriptor(new GRPCCatalogMethodDescriptorSupplier("Commit"))
                                    .build();
                }
            }
        }
        return getCommitMethod;
    }

    private static volatile io.grpc.MethodDescriptor<Grpccatalog.PreCommitRequest,
            Grpccatalog.PreCommitResponse> getPreCommitMethod;

    @io.grpc.stub.annotations.RpcMethod(
            fullMethodName = SERVICE_NAME + '/' + "PreCommit",
            requestType = Grpccatalog.PreCommitRequest.class,
            responseType = Grpccatalog.PreCommitResponse.class,
            methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
    public static io.grpc.MethodDescriptor<Grpccatalog.PreCommitRequest,
            Grpccatalog.PreCommitResponse> getPreCommitMethod() {
        io.grpc.MethodDescriptor<Grpccatalog.PreCommitRequest, Grpccatalog.PreCommitResponse> getPreCommitMethod;
        if ((getPreCommitMethod = GRPCCatalogGrpc.getPreCommitMethod) == null) {
            synchronized (GRPCCatalogGrpc.class) {
                if ((getPreCommitMethod = GRPCCatalogGrpc.getPreCommitMethod) == null) {
                    GRPCCatalogGrpc.getPreCommitMethod = getPreCommitMethod =
                            io.grpc.MethodDescriptor.<Grpccatalog.PreCommitRequest, Grpccatalog.PreCommitResponse>newBuilder()
                                    .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                                    .setFullMethodName(generateFullMethodName(SERVICE_NAME, "PreCommit"))
                                    .setSampledToLocalTracing(true)
                                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                            Grpccatalog.PreCommitRequest.getDefaultInstance()))
                                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                            Grpccatalog.PreCommitResponse.getDefaultInstance()))
                                    .setSchemaDescriptor(new GRPCCatalogMethodDescriptorSupplier("PreCommit"))
                                    .build();
                }
            }
        }
        return getPreCommitMethod;
    }

    private static volatile io.grpc.MethodDescriptor<Grpccatalog.BulkLoadRequest,
            Grpccatalog.BulkLoadResponse> getBulkLoadMethod;

    @io.grpc.stub.annotations.RpcMethod(
            fullMethodName = SERVICE_NAME + '/' + "BulkLoad",
            requestType = Grpccatalog.BulkLoadRequest.class,
            responseType = Grpccatalog.BulkLoadResponse.class,
            methodType = io.grpc.MethodDescriptor.MethodType.CLIENT_STREAMING)
    public static io.grpc.MethodDescriptor<Grpccatalog.BulkLoadRequest,
            Grpccatalog.BulkLoadResponse> getBulkLoadMethod() {
        io.grpc.MethodDescriptor<Grpccatalog.BulkLoadRequest, Grpccatalog.BulkLoadResponse> getBulkLoadMethod;
        if ((getBulkLoadMethod = GRPCCatalogGrpc.getBulkLoadMethod) == null) {
            synchronized (GRPCCatalogGrpc.class) {
                if ((getBulkLoadMethod = GRPCCatalogGrpc.getBulkLoadMethod) == null) {
                    GRPCCatalogGrpc.getBulkLoadMethod = getBulkLoadMethod =
                            io.grpc.MethodDescriptor.<Grpccatalog.BulkLoadRequest, Grpccatalog.BulkLoadResponse>newBuilder()
                                    .setType(io.grpc.MethodDescriptor.MethodType.CLIENT_STREAMING)
                                    .setFullMethodName(generateFullMethodName(SERVICE_NAME, "BulkLoad"))
                                    .setSampledToLocalTracing(true)
                                    .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                            Grpccatalog.BulkLoadRequest.getDefaultInstance()))
                                    .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                            Grpccatalog.BulkLoadResponse.getDefaultInstance()))
                                    .setSchemaDescriptor(new GRPCCatalogMethodDescriptorSupplier("BulkLoad"))
                                    .build();
                }
            }
        }
        return getBulkLoadMethod;
    }

    /**
     * Creates a new async stub that supports all call types for the service
     */
    public static GRPCCatalogStub newStub(io.grpc.Channel channel) {
        io.grpc.stub.AbstractStub.StubFactory<GRPCCatalogStub> factory =
                new io.grpc.stub.AbstractStub.StubFactory<GRPCCatalogStub>() {
                    @java.lang.Override
                    public GRPCCatalogStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
                        return new GRPCCatalogStub(channel, callOptions);
                    }
                };
        return GRPCCatalogStub.newStub(factory, channel);
    }

    /**
     * Creates a new blocking-style stub that supports unary and streaming output calls on the service
     */
    public static GRPCCatalogBlockingStub newBlockingStub(
            io.grpc.Channel channel) {
        io.grpc.stub.AbstractStub.StubFactory<GRPCCatalogBlockingStub> factory =
                new io.grpc.stub.AbstractStub.StubFactory<GRPCCatalogBlockingStub>() {
                    @java.lang.Override
                    public GRPCCatalogBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
                        return new GRPCCatalogBlockingStub(channel, callOptions);
                    }
                };
        return GRPCCatalogBlockingStub.newStub(factory, channel);
    }

    /**
     * Creates a new ListenableFuture-style stub that supports unary calls on the service
     */
    public static GRPCCatalogFutureStub newFutureStub(
            io.grpc.Channel channel) {
        io.grpc.stub.AbstractStub.StubFactory<GRPCCatalogFutureStub> factory =
                new io.grpc.stub.AbstractStub.StubFactory<GRPCCatalogFutureStub>() {
                    @java.lang.Override
                    public GRPCCatalogFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
                        return new GRPCCatalogFutureStub(channel, callOptions);
                    }
                };
        return GRPCCatalogFutureStub.newStub(factory, channel);
    }

    /**
     */
    public static abstract class GRPCCatalogImplBase implements io.grpc.BindableService {

        /**
         */
        public void startTxn(Grpccatalog.StartTxnRequest request,
                             io.grpc.stub.StreamObserver<Grpccatalog.StartTxnResponse> responseObserver) {
            io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getStartTxnMethod(), responseObserver);
        }

        /**
         */
        public void snapshot(Grpccatalog.SnapshotRequest request,
                             io.grpc.stub.StreamObserver<Grpccatalog.SnapshotResponse> responseObserver) {
            io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSnapshotMethod(), responseObserver);
        }

        /**
         */
        public void clone(Grpccatalog.CloneRequest request,
                          io.grpc.stub.StreamObserver<Grpccatalog.CloneResponse> responseObserver) {
            io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCloneMethod(), responseObserver);
        }

        /**
         */
        public void getGarbage(Grpccatalog.GetGarbageRequest request,
                               io.grpc.stub.StreamObserver<Grpccatalog.GetGarbageResponse> responseObserver) {
            io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetGarbageMethod(), responseObserver);
        }

        /**
         */
        public void clearGarbage(Grpccatalog.ClearGarbageRequest request,
                                 io.grpc.stub.StreamObserver<Grpccatalog.ClearGarbageResponse> responseObserver) {
            io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getClearGarbageMethod(), responseObserver);
        }

        /**
         */
        public void defineType(Grpccatalog.DefineTypeRequest request,
                               io.grpc.stub.StreamObserver<Grpccatalog.DefineTypeResponse> responseObserver) {
            io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDefineTypeMethod(), responseObserver);
        }

        /**
         */
        public void executeQuery(Grpccatalog.ExecuteQueryRequest request,
                                 io.grpc.stub.StreamObserver<Grpccatalog.ExecuteQueryResponse> responseObserver) {
            io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getExecuteQueryMethod(), responseObserver);
        }

        /**
         */
        public void commit(Grpccatalog.CommitRequest request,
                           io.grpc.stub.StreamObserver<Grpccatalog.CommitResponse> responseObserver) {
            io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCommitMethod(), responseObserver);
        }

        /**
         */
        public void preCommit(Grpccatalog.PreCommitRequest request,
                              io.grpc.stub.StreamObserver<Grpccatalog.PreCommitResponse> responseObserver) {
            io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getPreCommitMethod(), responseObserver);
        }

        /**
         */
        public io.grpc.stub.StreamObserver<Grpccatalog.BulkLoadRequest> bulkLoad(
                io.grpc.stub.StreamObserver<Grpccatalog.BulkLoadResponse> responseObserver) {
            return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(getBulkLoadMethod(), responseObserver);
        }

        @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
            return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
                    .addMethod(
                            getStartTxnMethod(),
                            io.grpc.stub.ServerCalls.asyncUnaryCall(
                                    new MethodHandlers<
                                            Grpccatalog.StartTxnRequest,
                                            Grpccatalog.StartTxnResponse>(
                                            this, METHODID_START_TXN)))
                    .addMethod(
                            getSnapshotMethod(),
                            io.grpc.stub.ServerCalls.asyncUnaryCall(
                                    new MethodHandlers<
                                            Grpccatalog.SnapshotRequest,
                                            Grpccatalog.SnapshotResponse>(
                                            this, METHODID_SNAPSHOT)))
                    .addMethod(
                            getCloneMethod(),
                            io.grpc.stub.ServerCalls.asyncUnaryCall(
                                    new MethodHandlers<
                                            Grpccatalog.CloneRequest,
                                            Grpccatalog.CloneResponse>(
                                            this, METHODID_CLONE)))
                    .addMethod(
                            getGetGarbageMethod(),
                            io.grpc.stub.ServerCalls.asyncUnaryCall(
                                    new MethodHandlers<
                                            Grpccatalog.GetGarbageRequest,
                                            Grpccatalog.GetGarbageResponse>(
                                            this, METHODID_GET_GARBAGE)))
                    .addMethod(
                            getClearGarbageMethod(),
                            io.grpc.stub.ServerCalls.asyncUnaryCall(
                                    new MethodHandlers<
                                            Grpccatalog.ClearGarbageRequest,
                                            Grpccatalog.ClearGarbageResponse>(
                                            this, METHODID_CLEAR_GARBAGE)))
                    .addMethod(
                            getDefineTypeMethod(),
                            io.grpc.stub.ServerCalls.asyncUnaryCall(
                                    new MethodHandlers<
                                            Grpccatalog.DefineTypeRequest,
                                            Grpccatalog.DefineTypeResponse>(
                                            this, METHODID_DEFINE_TYPE)))
                    .addMethod(
                            getExecuteQueryMethod(),
                            io.grpc.stub.ServerCalls.asyncServerStreamingCall(
                                    new MethodHandlers<
                                            Grpccatalog.ExecuteQueryRequest,
                                            Grpccatalog.ExecuteQueryResponse>(
                                            this, METHODID_EXECUTE_QUERY)))
                    .addMethod(
                            getCommitMethod(),
                            io.grpc.stub.ServerCalls.asyncUnaryCall(
                                    new MethodHandlers<
                                            Grpccatalog.CommitRequest,
                                            Grpccatalog.CommitResponse>(
                                            this, METHODID_COMMIT)))
                    .addMethod(
                            getPreCommitMethod(),
                            io.grpc.stub.ServerCalls.asyncUnaryCall(
                                    new MethodHandlers<
                                            Grpccatalog.PreCommitRequest,
                                            Grpccatalog.PreCommitResponse>(
                                            this, METHODID_PRE_COMMIT)))
                    .addMethod(
                            getBulkLoadMethod(),
                            io.grpc.stub.ServerCalls.asyncClientStreamingCall(
                                    new MethodHandlers<
                                            Grpccatalog.BulkLoadRequest,
                                            Grpccatalog.BulkLoadResponse>(
                                            this, METHODID_BULK_LOAD)))
                    .build();
        }
    }

    /**
     */
    public static final class GRPCCatalogStub extends io.grpc.stub.AbstractAsyncStub<GRPCCatalogStub> {
        private GRPCCatalogStub(
                io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
            super(channel, callOptions);
        }

        @java.lang.Override
        protected GRPCCatalogStub build(
                io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
            return new GRPCCatalogStub(channel, callOptions);
        }

        /**
         */
        public void startTxn(Grpccatalog.StartTxnRequest request,
                             io.grpc.stub.StreamObserver<Grpccatalog.StartTxnResponse> responseObserver) {
            io.grpc.stub.ClientCalls.asyncUnaryCall(
                    getChannel().newCall(getStartTxnMethod(), getCallOptions()), request, responseObserver);
        }

        /**
         */
        public void snapshot(Grpccatalog.SnapshotRequest request,
                             io.grpc.stub.StreamObserver<Grpccatalog.SnapshotResponse> responseObserver) {
            io.grpc.stub.ClientCalls.asyncUnaryCall(
                    getChannel().newCall(getSnapshotMethod(), getCallOptions()), request, responseObserver);
        }

        /**
         */
        public void clone(Grpccatalog.CloneRequest request,
                          io.grpc.stub.StreamObserver<Grpccatalog.CloneResponse> responseObserver) {
            io.grpc.stub.ClientCalls.asyncUnaryCall(
                    getChannel().newCall(getCloneMethod(), getCallOptions()), request, responseObserver);
        }

        /**
         */
        public void getGarbage(Grpccatalog.GetGarbageRequest request,
                               io.grpc.stub.StreamObserver<Grpccatalog.GetGarbageResponse> responseObserver) {
            io.grpc.stub.ClientCalls.asyncUnaryCall(
                    getChannel().newCall(getGetGarbageMethod(), getCallOptions()), request, responseObserver);
        }

        /**
         */
        public void clearGarbage(Grpccatalog.ClearGarbageRequest request,
                                 io.grpc.stub.StreamObserver<Grpccatalog.ClearGarbageResponse> responseObserver) {
            io.grpc.stub.ClientCalls.asyncUnaryCall(
                    getChannel().newCall(getClearGarbageMethod(), getCallOptions()), request, responseObserver);
        }

        /**
         */
        public void defineType(Grpccatalog.DefineTypeRequest request,
                               io.grpc.stub.StreamObserver<Grpccatalog.DefineTypeResponse> responseObserver) {
            io.grpc.stub.ClientCalls.asyncUnaryCall(
                    getChannel().newCall(getDefineTypeMethod(), getCallOptions()), request, responseObserver);
        }

        /**
         */
        public void executeQuery(Grpccatalog.ExecuteQueryRequest request,
                                 io.grpc.stub.StreamObserver<Grpccatalog.ExecuteQueryResponse> responseObserver) {
            io.grpc.stub.ClientCalls.asyncServerStreamingCall(
                    getChannel().newCall(getExecuteQueryMethod(), getCallOptions()), request, responseObserver);
        }

        /**
         */
        public void commit(Grpccatalog.CommitRequest request,
                           io.grpc.stub.StreamObserver<Grpccatalog.CommitResponse> responseObserver) {
            io.grpc.stub.ClientCalls.asyncUnaryCall(
                    getChannel().newCall(getCommitMethod(), getCallOptions()), request, responseObserver);
        }

        /**
         */
        public void preCommit(Grpccatalog.PreCommitRequest request,
                              io.grpc.stub.StreamObserver<Grpccatalog.PreCommitResponse> responseObserver) {
            io.grpc.stub.ClientCalls.asyncUnaryCall(
                    getChannel().newCall(getPreCommitMethod(), getCallOptions()), request, responseObserver);
        }

        /**
         */
        public io.grpc.stub.StreamObserver<Grpccatalog.BulkLoadRequest> bulkLoad(
                io.grpc.stub.StreamObserver<Grpccatalog.BulkLoadResponse> responseObserver) {
            return io.grpc.stub.ClientCalls.asyncClientStreamingCall(
                    getChannel().newCall(getBulkLoadMethod(), getCallOptions()), responseObserver);
        }
    }

    /**
     */
    public static final class GRPCCatalogBlockingStub extends io.grpc.stub.AbstractBlockingStub<GRPCCatalogBlockingStub> {
        private GRPCCatalogBlockingStub(
                io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
            super(channel, callOptions);
        }

        @java.lang.Override
        protected GRPCCatalogBlockingStub build(
                io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
            return new GRPCCatalogBlockingStub(channel, callOptions);
        }

        /**
         */
        public Grpccatalog.StartTxnResponse startTxn(Grpccatalog.StartTxnRequest request) {
            return io.grpc.stub.ClientCalls.blockingUnaryCall(
                    getChannel(), getStartTxnMethod(), getCallOptions(), request);
        }

        /**
         */
        public Grpccatalog.SnapshotResponse snapshot(Grpccatalog.SnapshotRequest request) {
            return io.grpc.stub.ClientCalls.blockingUnaryCall(
                    getChannel(), getSnapshotMethod(), getCallOptions(), request);
        }

        /**
         */
        public Grpccatalog.CloneResponse clone(Grpccatalog.CloneRequest request) {
            return io.grpc.stub.ClientCalls.blockingUnaryCall(
                    getChannel(), getCloneMethod(), getCallOptions(), request);
        }

        /**
         */
        public Grpccatalog.GetGarbageResponse getGarbage(Grpccatalog.GetGarbageRequest request) {
            return io.grpc.stub.ClientCalls.blockingUnaryCall(
                    getChannel(), getGetGarbageMethod(), getCallOptions(), request);
        }

        /**
         */
        public Grpccatalog.ClearGarbageResponse clearGarbage(Grpccatalog.ClearGarbageRequest request) {
            return io.grpc.stub.ClientCalls.blockingUnaryCall(
                    getChannel(), getClearGarbageMethod(), getCallOptions(), request);
        }

        /**
         */
        public Grpccatalog.DefineTypeResponse defineType(Grpccatalog.DefineTypeRequest request) {
            return io.grpc.stub.ClientCalls.blockingUnaryCall(
                    getChannel(), getDefineTypeMethod(), getCallOptions(), request);
        }

        /**
         */
        public java.util.Iterator<Grpccatalog.ExecuteQueryResponse> executeQuery(
                Grpccatalog.ExecuteQueryRequest request) {
            return io.grpc.stub.ClientCalls.blockingServerStreamingCall(
                    getChannel(), getExecuteQueryMethod(), getCallOptions(), request);
        }

        /**
         */
        public Grpccatalog.CommitResponse commit(Grpccatalog.CommitRequest request) {
            return io.grpc.stub.ClientCalls.blockingUnaryCall(
                    getChannel(), getCommitMethod(), getCallOptions(), request);
        }

        /**
         */
        public Grpccatalog.PreCommitResponse preCommit(Grpccatalog.PreCommitRequest request) {
            return io.grpc.stub.ClientCalls.blockingUnaryCall(
                    getChannel(), getPreCommitMethod(), getCallOptions(), request);
        }
    }

    /**
     */
    public static final class GRPCCatalogFutureStub extends io.grpc.stub.AbstractFutureStub<GRPCCatalogFutureStub> {
        private GRPCCatalogFutureStub(
                io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
            super(channel, callOptions);
        }

        @java.lang.Override
        protected GRPCCatalogFutureStub build(
                io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
            return new GRPCCatalogFutureStub(channel, callOptions);
        }

        /**
         */
        public com.google.common.util.concurrent.ListenableFuture<Grpccatalog.StartTxnResponse> startTxn(
                Grpccatalog.StartTxnRequest request) {
            return io.grpc.stub.ClientCalls.futureUnaryCall(
                    getChannel().newCall(getStartTxnMethod(), getCallOptions()), request);
        }

        /**
         */
        public com.google.common.util.concurrent.ListenableFuture<Grpccatalog.SnapshotResponse> snapshot(
                Grpccatalog.SnapshotRequest request) {
            return io.grpc.stub.ClientCalls.futureUnaryCall(
                    getChannel().newCall(getSnapshotMethod(), getCallOptions()), request);
        }

        /**
         */
        public com.google.common.util.concurrent.ListenableFuture<Grpccatalog.CloneResponse> clone(
                Grpccatalog.CloneRequest request) {
            return io.grpc.stub.ClientCalls.futureUnaryCall(
                    getChannel().newCall(getCloneMethod(), getCallOptions()), request);
        }

        /**
         */
        public com.google.common.util.concurrent.ListenableFuture<Grpccatalog.GetGarbageResponse> getGarbage(
                Grpccatalog.GetGarbageRequest request) {
            return io.grpc.stub.ClientCalls.futureUnaryCall(
                    getChannel().newCall(getGetGarbageMethod(), getCallOptions()), request);
        }

        /**
         */
        public com.google.common.util.concurrent.ListenableFuture<Grpccatalog.ClearGarbageResponse> clearGarbage(
                Grpccatalog.ClearGarbageRequest request) {
            return io.grpc.stub.ClientCalls.futureUnaryCall(
                    getChannel().newCall(getClearGarbageMethod(), getCallOptions()), request);
        }

        /**
         */
        public com.google.common.util.concurrent.ListenableFuture<Grpccatalog.DefineTypeResponse> defineType(
                Grpccatalog.DefineTypeRequest request) {
            return io.grpc.stub.ClientCalls.futureUnaryCall(
                    getChannel().newCall(getDefineTypeMethod(), getCallOptions()), request);
        }

        /**
         */
        public com.google.common.util.concurrent.ListenableFuture<Grpccatalog.CommitResponse> commit(
                Grpccatalog.CommitRequest request) {
            return io.grpc.stub.ClientCalls.futureUnaryCall(
                    getChannel().newCall(getCommitMethod(), getCallOptions()), request);
        }

        /**
         */
        public com.google.common.util.concurrent.ListenableFuture<Grpccatalog.PreCommitResponse> preCommit(
                Grpccatalog.PreCommitRequest request) {
            return io.grpc.stub.ClientCalls.futureUnaryCall(
                    getChannel().newCall(getPreCommitMethod(), getCallOptions()), request);
        }
    }

    private static final int METHODID_START_TXN = 0;
    private static final int METHODID_SNAPSHOT = 1;
    private static final int METHODID_CLONE = 2;
    private static final int METHODID_GET_GARBAGE = 3;
    private static final int METHODID_CLEAR_GARBAGE = 4;
    private static final int METHODID_DEFINE_TYPE = 5;
    private static final int METHODID_EXECUTE_QUERY = 6;
    private static final int METHODID_COMMIT = 7;
    private static final int METHODID_PRE_COMMIT = 8;
    private static final int METHODID_BULK_LOAD = 9;

    private static final class MethodHandlers<Req, Resp> implements
            io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
            io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
            io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
            io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
        private final GRPCCatalogImplBase serviceImpl;
        private final int methodId;

        MethodHandlers(GRPCCatalogImplBase serviceImpl, int methodId) {
            this.serviceImpl = serviceImpl;
            this.methodId = methodId;
        }

        @java.lang.Override
        @java.lang.SuppressWarnings("unchecked")
        public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
            switch (methodId) {
                case METHODID_START_TXN:
                    serviceImpl.startTxn((Grpccatalog.StartTxnRequest) request,
                            (io.grpc.stub.StreamObserver<Grpccatalog.StartTxnResponse>) responseObserver);
                    break;
                case METHODID_SNAPSHOT:
                    serviceImpl.snapshot((Grpccatalog.SnapshotRequest) request,
                            (io.grpc.stub.StreamObserver<Grpccatalog.SnapshotResponse>) responseObserver);
                    break;
                case METHODID_CLONE:
                    serviceImpl.clone((Grpccatalog.CloneRequest) request,
                            (io.grpc.stub.StreamObserver<Grpccatalog.CloneResponse>) responseObserver);
                    break;
                case METHODID_GET_GARBAGE:
                    serviceImpl.getGarbage((Grpccatalog.GetGarbageRequest) request,
                            (io.grpc.stub.StreamObserver<Grpccatalog.GetGarbageResponse>) responseObserver);
                    break;
                case METHODID_CLEAR_GARBAGE:
                    serviceImpl.clearGarbage((Grpccatalog.ClearGarbageRequest) request,
                            (io.grpc.stub.StreamObserver<Grpccatalog.ClearGarbageResponse>) responseObserver);
                    break;
                case METHODID_DEFINE_TYPE:
                    serviceImpl.defineType((Grpccatalog.DefineTypeRequest) request,
                            (io.grpc.stub.StreamObserver<Grpccatalog.DefineTypeResponse>) responseObserver);
                    break;
                case METHODID_EXECUTE_QUERY:
                    serviceImpl.executeQuery((Grpccatalog.ExecuteQueryRequest) request,
                            (io.grpc.stub.StreamObserver<Grpccatalog.ExecuteQueryResponse>) responseObserver);
                    break;
                case METHODID_COMMIT:
                    serviceImpl.commit((Grpccatalog.CommitRequest) request,
                            (io.grpc.stub.StreamObserver<Grpccatalog.CommitResponse>) responseObserver);
                    break;
                case METHODID_PRE_COMMIT:
                    serviceImpl.preCommit((Grpccatalog.PreCommitRequest) request,
                            (io.grpc.stub.StreamObserver<Grpccatalog.PreCommitResponse>) responseObserver);
                    break;
                default:
                    throw new AssertionError();
            }
        }

        @java.lang.Override
        @java.lang.SuppressWarnings("unchecked")
        public io.grpc.stub.StreamObserver<Req> invoke(
                io.grpc.stub.StreamObserver<Resp> responseObserver) {
            switch (methodId) {
                case METHODID_BULK_LOAD:
                    return (io.grpc.stub.StreamObserver<Req>) serviceImpl.bulkLoad(
                            (io.grpc.stub.StreamObserver<Grpccatalog.BulkLoadResponse>) responseObserver);
                default:
                    throw new AssertionError();
            }
        }
    }

    private static abstract class GRPCCatalogBaseDescriptorSupplier
            implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
        GRPCCatalogBaseDescriptorSupplier() {}

        @java.lang.Override
        public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
            return Grpccatalog.getDescriptor();
        }

        @java.lang.Override
        public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
            return getFileDescriptor().findServiceByName("GRPCCatalog");
        }
    }

    private static final class GRPCCatalogFileDescriptorSupplier
            extends GRPCCatalogBaseDescriptorSupplier {
        GRPCCatalogFileDescriptorSupplier() {}
    }

    private static final class GRPCCatalogMethodDescriptorSupplier
            extends GRPCCatalogBaseDescriptorSupplier
            implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
        private final String methodName;

        GRPCCatalogMethodDescriptorSupplier(String methodName) {
            this.methodName = methodName;
        }

        @java.lang.Override
        public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
            return getServiceDescriptor().findMethodByName(methodName);
        }
    }

    private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

    public static io.grpc.ServiceDescriptor getServiceDescriptor() {
        io.grpc.ServiceDescriptor result = serviceDescriptor;
        if (result == null) {
            synchronized (GRPCCatalogGrpc.class) {
                result = serviceDescriptor;
                if (result == null) {
                    serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
                            .setSchemaDescriptor(new GRPCCatalogFileDescriptorSupplier())
                            .addMethod(getStartTxnMethod())
                            .addMethod(getSnapshotMethod())
                            .addMethod(getCloneMethod())
                            .addMethod(getGetGarbageMethod())
                            .addMethod(getClearGarbageMethod())
                            .addMethod(getDefineTypeMethod())
                            .addMethod(getExecuteQueryMethod())
                            .addMethod(getCommitMethod())
                            .addMethod(getPreCommitMethod())
                            .addMethod(getBulkLoadMethod())
                            .build();
                }
            }
        }
        return result;
    }
}
