using Common.Workload;

namespace Common.Streaming;

public class TransactionMark {

    public string tid { get; set; }

    public TransactionType type { get; set; }

    public int actorId { get; set; }

    public MarkStatus status { get; set; }

    public string source { get; set; }

    public TransactionMark(){ }

    public TransactionMark(string tid, TransactionType type, int actorId, MarkStatus status, string source)
    {
        this.tid = tid;
        this.type = type;
        this.actorId = actorId;
        this.status = status;
        this.source = source;
    }

}