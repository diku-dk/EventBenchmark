using Common.Workload;

namespace Common.Streaming;

public record TransactionMark(int tid, TransactionType type, int actorId, MarkStatus status, string source);