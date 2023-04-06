using System;
using System.Threading.Tasks;

namespace Marketplace.Infra
{
    /**
	 * https://learn.microsoft.com/en-us/dotnet/csharp/language-reference/proposals/csharp-8.0/default-interface-methods
	 */
    public interface SnapperActor
	{
		Task noOp() { return Task.CompletedTask; }
	}
}

