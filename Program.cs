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
            sub.AddOption(new Option<int>(new string[] { "--interval", "-i" }, () => 2000, "Handling sleep interval (milliseconds)."));
            sub.Handler = CommandHandler.Create<string[], int, string, int, string, string>(Subscribe);
            cmd.AddCommand(sub);

            // Invoke execution
            cmd.InvokeAsync(args).Wait();
        }

        public static void Publish(string exchange, int interval, int size,
            string host, int port, string user, string pass)
        {
            size = Math.Abs(size);

            var cf = new ConnectionFactory
            {
                HostName = host,
                Port = port,
                UserName = user,
                Password = pass,
                AutomaticRecoveryEnabled = true
            };

            using (var cc = cf.CreateConnection())
            {
                IModel createChannel()
                {
                    var cn = cc.CreateModel();

                    cn.BasicReturn += (sender, ea) => Console.WriteLine("\n*** Basic Return ***" +
                        ea.ReplyCode + " - " + ea.ReplyText + "\n");

                    cn.ConfirmSelect();

                    return cn;
                }

                var cn = createChannel();

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

                            cn.BasicPublish(exchange, "", bp, Encoding.UTF8.GetBytes(
                                (message + "\n" + new string('$', size)).Substring(0, size)));

                            cn.WaitForConfirmsOrDie(TimeSpan.FromMilliseconds(15000));

                            Console.WriteLine(message + $" sent! #{i++}");
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine("\n*** Exception Thrown ***\n" +
                                ex.Message + "\n" + ex.InnerException?.Message + "\n");

                            if (cn.IsClosed && cc.IsOpen)
                            {
                                cn.Dispose();
                                cn = createChannel();
                                Console.WriteLine("\n*** Channel closed. New channel created! ***\n");
                            }
                        }

                        Thread.Sleep(Math.Abs(interval));
                    }
                } while (Console.ReadKey(true).Key != ConsoleKey.Escape);

                cn.Dispose();
                cc.Close();
            }
        }

        public static void Subscribe(string[] queues, int interval, string host, int port, string user, string pass)
        {
            var cf = new ConnectionFactory
            {
                HostName = host,
                Port = port,
                UserName = user,
                Password = pass,
                AutomaticRecoveryEnabled = true
            };

            using (var cc = cf.CreateConnection())
            {
                using (var cn = cc.CreateModel())
                {
                    cn.BasicQos(0, 10, false);

                    var consumersToQueues = new Dictionary<string, string>();

                    var cs = new EventingBasicConsumer(cn);

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

                            if (!consumersToQueues.TryGetValue(ea.ConsumerTag, out string queue))
                            {
                                Console.WriteLine(msg + $" received from {queue}! {ea.DeliveryTag}");
                                Console.WriteLine("\n*** Warning ***\n" +
                                    "This message was ignored because subscription was stopped!\n");
                                return;
                            }

                            Console.WriteLine(msg + $" received from {queue}! #{ea.DeliveryTag}");

                            Thread.Sleep(Math.Abs(interval));

                            cn.BasicAck(ea.DeliveryTag, false);
                        }
                        catch (Exception ex)
                        {
                            cn.BasicNack(ea.DeliveryTag, false, true);

                            Console.WriteLine("\n*** Exception Thrown ***\n" +
                                ex.Message + "\n" + ex.InnerException?.Message + "\n");
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
                }
            }
        }
    }
}