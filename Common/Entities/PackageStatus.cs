namespace Common.Entities
{
    public enum PackageStatus
    {
        created,
        ready_to_ship,
        canceled,
        shipped,
        lost,
        stolen,
        seized_for_inspection,
        returning_to_sender,
        returned_to_sender,
        awaiting_pickup_by_receiver,
        delivered
    }
}