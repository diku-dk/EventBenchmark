using Common.Workload;

namespace Common.Streaming;

public record TransactionMark(string tid, TransactionType type, int actorId, MarkStatus status, string source);