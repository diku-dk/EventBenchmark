using System;

namespace Monitor;

public class PayloadObject
{
    public String message_type { get; set; }
    public object payload { get; set; }

    public PayloadObject()
    {
        
    }
    
    public PayloadObject(String message_type, object payload)
    {
        this.message_type = message_type;
        this.payload = payload;
    }
}