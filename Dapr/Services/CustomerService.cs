using Daprr.Services;
using Daprr.Workers;
using Daprr.Workrs;

namespace Daprr.Services;

public class CustomerService : ICustomerService
{

    private Dictionary<int, CustomerThread>? customers { get; set; }

    public CustomerService(){}

    public void Run(int customerId, int tid) => customers[customerId].Run(tid);

}

