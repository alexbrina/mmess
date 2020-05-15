using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.CommandLine;
using System.CommandLine.Invocation;
using System.Text;
using System.Threading;

namespace ConsoleApp
{
    public static class Program
    {
        static void Main(string[] args)
        {
            var cmd = new RootCommand("RabbitMQ Mass Messaging");

            // Global options
            cmd.AddGlobalOption(new Option<string>(new string[] { "--host", "-hn" }, () => "localhost"));
            cmd.AddGlobalOption(new Option<int>(new string[] { "--port", "-hp" }, () => -1));
            cmd.AddGlobalOption(new Option<string>(new string[] { "--user", "-u" }, () => "guest"));
            cmd.AddGlobalOption(new Option<string>(new string[] { "--pass", "-p" }, () => "guest"));

            // Publish command options
            var pub = new Command("publish");

            var oExchange = new Option<string>(new string[] { "--exchange", "-e" });
            oExchange.Argument.Arity = ArgumentArity.ExactlyOne;
            pub.AddOption(oExchange);
            pub.AddOption(new Option<int>(new string[] { "--interval", "-i" }, () => 1000, "Publish sleep interval (milliseconds)."));
            pub.AddOption(new Option<int>(new string[] { "--size", "-s" }, () => 1024, "Message size (bytes)."));
            pub.Handler = CommandHandler.Create<string, int, int, string, int, string, string>(Publish);
            cmd.AddCommand(pub);

            // Subscribe command options
            var sub = new Command("subscribe");

            var oQueues = new Option<string[]>(new string[] { "--queues", "-q" });
            oQueues.Argument.Arity = ArgumentArity.OneOrMore;
            sub.AddOption(oQueues);
            sub.AddOption(new Option<int>(new string[] { "--interval", "-i" }, () => 1000, "Handling sleep interval (milliseconds)."));
            sub.Handler = CommandHandler.Create<string[], int, string, int, string, string>(Subscribe);
            cmd.AddCommand(sub);

            // Invoke execution
            cmd.InvokeAsync(args).Wait();
        }

        public static void Publish(string exchange, int interval, int size, string host, int port, string user, string pass)
        {
            var cc = CreateConnection(host, port, user, pass);
            var cn = CreateChannel(cc);

            size = Math.Abs(size);
            interval = Math.Abs(interval);
            var i = 1;
            Console.WriteLine("Press ESC to stop");

            do
            {
                while (! Console.KeyAvailable)
                {
                    try
                    {
                        var message = $"Message {DateTimeOffset.Now:yyyy'-'MM'-'dd' 'HH':'mm':'ss'.'fffffff}";
                        var bp = cn.CreateBasicProperties();
                        bp.DeliveryMode = 2; // persistent
                        cn.BasicPublish(exchange, "", bp, Encoding.UTF8.GetBytes((message + "\n" + new string('$', size)).Substring(0, size)));
                        cn.WaitForConfirmsOrDie(TimeSpan.FromMilliseconds(15000));
                        Console.WriteLine($"{message} sent! (#{i++})");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Exception :: {ex.Message}\n{ex.InnerException?.Message}");
                        (cc, cn) = TryRecover(cc, cn, host, port, user, pass);
                    }

                    Thread.Sleep(interval);
                }
            } while (Console.ReadKey(true).Key != ConsoleKey.Escape);

            // Closing
            if (cn?.IsOpen ?? false) cn.Close();
            cn?.Dispose();

            if (cc?.IsOpen ?? false) cc.Close();
            cc?.Dispose();
        }

        public static void Subscribe(string[] queues, int interval, string host, int port, string user, string pass)
        {
            var cc = CreateConnection(host, port, user, pass);
            var cn = CreateChannel(cc);

            var consumersToQueues = new Dictionary<string, string>();
            var cs = new EventingBasicConsumer(cn);

            interval = Math.Abs(interval);

            // Message handler
            cs.Received += (sender, ea) =>
            {
                try
                {
                    Monitor.Enter(consumersToQueues);

                    var msg = Encoding.UTF8.GetString(ea.Body.ToArray());
                    if (string.IsNullOrEmpty(msg))
                    {
                        msg = "Empty message";
                    }
                    var nli = msg.IndexOf("\n");
                    msg = msg.Substring(0, nli < 0 ? msg.Length : nli);

                    if (consumersToQueues.TryGetValue(ea.ConsumerTag, out string queue))
                    {
                        Console.WriteLine($"{msg} received from {queue}! (#{ea.DeliveryTag})");
                        Thread.Sleep(interval);
                        cn.BasicAck(ea.DeliveryTag, false);
                    }
                    else
                    {
                        Console.WriteLine($"{msg} received but was ignored because subscription was stopped! (#{ea.DeliveryTag})");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Exception :: {ex.Message}\n{ex.InnerException?.Message}");
                    if (cn?.IsOpen ?? false)
                    {
                        cn.BasicNack(ea.DeliveryTag, false, true);
                    }
                    (cc, cn) = TryRecover(cc, cn, host, port, user, pass);
                }
                finally
                {
                    Monitor.Exit(consumersToQueues);
                }
            };

            // Start consuming
            lock (consumersToQueues)
            {
                foreach (var queue in queues)
                {
                    var tag = cn.BasicConsume(queue, false, cs);
                    consumersToQueues.Add(tag, queue);
                }
            }

            // Wait until Esc key press
            Console.WriteLine("Press ESC to stop");
            do
            {
                while (!Console.KeyAvailable)
                {
                    Thread.Sleep(500);
                }
            } while (Console.ReadKey(true).Key != ConsoleKey.Escape);

            // Stop consuming
            lock (consumersToQueues)
            {
                if (cn?.IsOpen ?? false)
                {
                    foreach (var tag in consumersToQueues)
                    {
                        cn.BasicCancel(tag.Key);
                    }
                }
            }

            // Closing
            if (cn?.IsOpen ?? false) cn.Close();
            cn?.Dispose();

            if (cc?.IsOpen ?? false) cc.Close();
            cc?.Dispose();
        }

        private static IConnection CreateConnection(string host, int port, string user, string pass)
        {
            var cf = new ConnectionFactory
            {
                HostName = host,
                Port = port,
                UserName = user,
                Password = pass,
                AutomaticRecoveryEnabled = true,
                RequestedHeartbeat = TimeSpan.FromSeconds(20)
            };

            var cc = cf.CreateConnection();

            cc.ConnectionBlocked += (sender, ea) => Console.WriteLine($"Connection Event :: ConnectionBlocked :: {ea.Reason}");
            cc.ConnectionUnblocked += (sender, ea) => Console.WriteLine($"Connection Event :: ConnectionUnblocked");
            cc.ConnectionShutdown += (sender, ea) => Console.WriteLine($"Connection Event :: ConnectionShutdown :: {ea.ReplyCode} - {ea.ReplyText}");
            cc.CallbackException += (sender, ea) => Console.WriteLine($"Connection Event :: CallbackException :: {ea.Exception}");

            Console.WriteLine("New Connection Created!");

            return cc;
        }

        private static IModel CreateChannel(IConnection cc)
        {
            var cn = cc.CreateModel();

            cn.BasicReturn += (sender, ea) => Console.WriteLine($"Channel Event :: BasicReturn :: {ea.ReplyCode} - {ea.ReplyText}");
            cn.BasicNacks += (sender, ea) => Console.WriteLine($"Channel Event :: BasicNacks :: DeliveryTag {ea.DeliveryTag}");
            cn.ModelShutdown += (sender, ea) => Console.WriteLine($"Channel Event :: ModelShutdown :: {ea.ReplyCode} - {ea.ReplyText}");
            cn.CallbackException += (sender, ea) => Console.WriteLine($"Channel Event :: CallbackException :: {ea.Exception}");

            cn.ConfirmSelect();
            cn.BasicQos(0, 10, true);

            Console.WriteLine("New Channel Created!");

            return cn;
        }

        private static (IConnection, IModel) TryRecover(IConnection cc, IModel cn, string host, int port, string user, string pass, byte retry = 1)
        {
            if (retry > 10)
            {
                throw new TimeoutException("Max retries reached, sorry.");
            }

            try
            {
                if (!cc?.IsOpen ?? true)
                {
                    cc?.Dispose();
                    cc = CreateConnection(host, port, user, pass);
                }

                if (!cn?.IsOpen ?? true)
                {
                    cn?.Dispose();
                    cn = CreateChannel(cc);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exception :: {ex.Message}\n{ex.InnerException?.Message}");
                var wait = Convert.ToInt32((Math.Pow(2,retry) - 1));
                Console.WriteLine($"Retry :: #{retry} :: Waiting {wait} seconds before retry");
                Thread.Sleep(wait * 1000);
                (cc, cn) = TryRecover(cc, cn, host, port, user, pass, ++retry);
            }

            return (cc, cn);
        }
    }
}