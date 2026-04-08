using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace Monitor;

public class PayloadObject
{
    public String message_type { get; set; }
    public object payload { get; set; }
    
    public Dictionary<String, BlockingCollection<PayloadObject>> mailboxes;

    public PayloadObject()
    {
        
    }
    
    public PayloadObject(String message_type, object payload, Dictionary<String, BlockingCollection<PayloadObject>> mailboxes)
    {
        this.message_type = message_type;
        this.payload = payload;
    }
}