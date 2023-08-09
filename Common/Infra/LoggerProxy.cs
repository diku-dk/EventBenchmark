using Microsoft.Extensions.Logging;

namespace Common.Infra
{
    /*
     * Based on https://learn.microsoft.com/en-us/dotnet/csharp/programming-guide/classes-and-structs/static-constructors
     */
    public class LoggerProxy
	{
        // Static variable that must be initialized at run time.
        static readonly ILoggerFactory loggerFactory;

        // Static constructor is called at most one time, before any
        // instance constructor is invoked or member is accessed.
        static LoggerProxy()
        {
            loggerFactory = LoggerFactory.Create(loggingBuilder => loggingBuilder
                                                    .SetMinimumLevel(LogLevel.Information)
                                                    .AddConsole());
            
        }

        public static ILogger GetInstance(string categoryName)
        {
            return loggerFactory.CreateLogger(categoryName);
        }

    }
}

