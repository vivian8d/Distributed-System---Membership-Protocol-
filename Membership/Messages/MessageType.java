package cs425.mp3.Messages;

public enum MessageType {
    Join,
    Leave,
    Crash,
    Ping,
    Ack,
    MemberListRequest,
    IntroducerCheckAlive,

    Metadata,

    GetReplicasToPut,
    ReturnPutReplicas,
    File,
    AckFile,

    GetReplicasToGet,
    ReturnGetReplicas,
    RequestFile,

    RequestDelete,
    DeleteFile,

    RequestVersion,
    RequestReplicate,

    FileOpPut,
    FileOpGet,
    FileOpDelete,
    FileOpGetVersion,
    FileOpStore,

    InferInfo,
    Infer,
    InferSuccess,

    BatchSize,
    MetadataModel,
    GetOutput,


    // Bully Message Type
    ELECTION,
    ELECTION_RESPONSE,
    VICTORY
}
