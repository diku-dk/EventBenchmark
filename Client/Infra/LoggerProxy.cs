using System;
using Microsoft.Extensions.Logging;

namespace Client.Infra
{
    /*
     * Based on https://learn.microsoft.com/en-us/dotnet/csharp/programming-guide/classes-and-structs/static-constructors
     */
    public class LoggerProxy
	{
        // Static variable that must be initialized at run time.
        static readonly ILogger _logger;

        // Static constructor is called at most one time, before any
        // instance constructor is invoked or member is accessed.
        static LoggerProxy()
        {
            using var loggerFactory = LoggerFactory.Create(loggingBuilder => loggingBuilder
                                                    .SetMinimumLevel(LogLevel.Warning)
                                                    .AddConsole());
            _logger = loggerFactory.CreateLogger("DefaultClientLogger");
        }

        public static ILogger GetInstance()
        {
            return _logger;
        }
    }
}

