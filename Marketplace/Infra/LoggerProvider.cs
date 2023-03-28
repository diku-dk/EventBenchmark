using System;
using Microsoft.Extensions.Logging;

namespace Marketplace.Infra
{
    /**
     * https://learn.microsoft.com/en-us/dotnet/core/extensions/custom-logging-provider
     */
    public class LoggerProvider : ILoggerProvider
	{
		public LoggerProvider()
		{
		}

        public ILogger CreateLogger(string categoryName)
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }
    }

    public class DefaultLogger : ILogger
    {
        public IDisposable BeginScope<TState>(TState state) where TState : notnull => default!;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
        {
            Console.WriteLine();
        }
    }

}

