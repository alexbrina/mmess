using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Otc.Messaging.Abstractions;
using Otc.Messaging.RabbitMQ.Configurations;
using System;
using System.Collections.Generic;
using System.CommandLine;
using System.CommandLine.Invocation;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ConsoleApp
{
    public static class Program
    {
        private static IMessaging Broker;

        static void Main(string[] args)
        {
            var cmd = new RootCommand("RabbitMQ Mass Messaging");

            // Setup global options
            cmd.AddGlobalOption(new Option<string>(new string[] { "--host", "-hn" }, () => "localhost"));
            cmd.AddGlobalOption(new Option<int>(new string[] { "--port", "-hp" }, () => -1));
            cmd.AddGlobalOption(new Option<string>(new string[] { "--user", "-u" }, () => "guest"));
            cmd.AddGlobalOption(new Option<string>(new string[] { "--pass", "-p" }, () => "guest"));

            // Setup publish command options
            var pub = new Command("publish");
            var oExchange = new Option<string>(new string[] { "--exchange", "-e" });
            oExchange.Argument.Arity = ArgumentArity.ExactlyOne;
            pub.AddOption(oExchange);
            pub.AddOption(new Option<int>(new string[] { "--interval", "-i" }, () => 1000, "Publish sleep interval (milliseconds)."));
            pub.AddOption(new Option<int>(new string[] { "--size", "-s" }, () => 1024, "Message size (bytes)."));
            pub.Handler = CommandHandler.Create<string, int, int>(Publish);
            cmd.AddCommand(pub);

            // Setup subscribe command options
            var sub = new Command("subscribe");
            var oQueues = new Option<string[]>(new string[] { "--queues", "-q" });
            oQueues.Argument.Arity = ArgumentArity.OneOrMore;
            sub.AddOption(oQueues);
            sub.AddOption(new Option<int>(new string[] { "--interval", "-i" }, () => 1000, "Handling sleep interval (milliseconds)."));
            sub.Handler = CommandHandler.Create<string[], int>(Subscribe);
            cmd.AddCommand(sub);

            // Read global options
            var arguments = cmd.Parse(args);

            // Broker
            IServiceCollection services = new ServiceCollection();
            var serviceProvider = services
                .AddLogging(b => b.AddConsole().SetMinimumLevel(LogLevel.Warning))
                .AddRabbitMQ(new RabbitMQConfiguration
                {
                    Hosts = new List<string> { arguments.CommandResult.ValueForOption<string>("--host") },
                    Port = arguments.CommandResult.ValueForOption<int>("--port"),
                    User = arguments.CommandResult.ValueForOption<string>("--user"),
                    Password = arguments.CommandResult.ValueForOption<string>("--pass"),
                    PerQueuePrefetchCount = 10,
                    MessageHandlerErrorBehavior = MessageHandlerErrorBehavior.RejectOnFistDelivery
                })
                .BuildServiceProvider();
            Broker = serviceProvider.GetService<IMessaging>();

            // Invoke execution
            cmd.InvokeAsync(args).Wait();
        }

        public static void Publish(string exchange, int interval, int size)
        {
            size = Math.Abs(size);
            interval = Math.Abs(interval);
            var tasks = new List<Task>();

            void action(int j)
            {
                tasks.Add(Task.Factory.StartNew(() =>
                {
                    var i = 1;
                    while (true)
                    {
                        try
                        {
                            using (var pub = Broker.CreatePublisher())
                            {
                                var message = $"Message {DateTimeOffset.Now:yyyy'-'MM'-'dd' 'HH':'mm':'ss'.'fffffff}";
                                pub.Publish(Encoding.UTF8.GetBytes((message + "\n" + new string('$', size)).Substring(0, size)), exchange);
                                Console.WriteLine($"{message} sent! (#{j}:#{i++})");
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Publish Exception\n" + ex);
                        }

                        Thread.Sleep(interval);
                    }
                }));
            }

            for (int j = 0; j < 500; j++)
            {
                action(j);
                Thread.Sleep(10);
            }

            Task.WaitAll(tasks.ToArray());
        }

        public static void Subscribe(string[] queues, int interval)
        {
            void Handler(byte[] message, IMessageContext messageContext)
            {
                Thread.Sleep(interval);
                var msg = Encoding.UTF8.GetString(message);
                if (string.IsNullOrEmpty(msg))
                {
                    msg = "Empty message";
                }
                var nli = msg.IndexOf("\n");
                msg = msg.Substring(0, nli < 0 ? msg.Length : nli);

                Console.WriteLine($"{msg} received from {messageContext.Queue}!");
            }

            var sub = Broker.Subscribe(Handler, queues);

            // Wait until key press
            sub.Start();
            while (!Console.KeyAvailable)
            {
                Thread.Sleep(500);
            }
            sub.Stop();
            sub.Dispose();
        }
    }
}