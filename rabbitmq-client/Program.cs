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
        private static string Host;
        private static int Port;
        private static string User;
        private static string Pass;
        private static TimeSpan RecoveryInterval;
        private static byte RecoveryAttempts;

        static void Main(string[] args)
        {
            var cmd = new RootCommand("RabbitMQ Mass Messaging");

            // Setup global options
            cmd.AddGlobalOption(new Option<string>(new string[] { "--host", "-hn" }, () => "localhost"));
            cmd.AddGlobalOption(new Option<int>(new string[] { "--port", "-hp" }, () => -1));
            cmd.AddGlobalOption(new Option<string>(new string[] { "--user", "-u" }, () => "guest"));
            cmd.AddGlobalOption(new Option<string>(new string[] { "--pass", "-p" }, () => "guest"));
            cmd.AddGlobalOption(new Option<int>(new string[] { "--recovery-interval", "-ri" }, () => 15000, "Network recovery interval (milliseconds)."));
            cmd.AddGlobalOption(new Option<byte>(new string[] { "--recovery-attempts", "-ra" }, () => 30, "Network recovery attempts (0 to 255)."));

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
            Host = arguments.CommandResult.ValueForOption<string>("--host");
            Port = arguments.CommandResult.ValueForOption<int>("--port");
            User = arguments.CommandResult.ValueForOption<string>("--user");
            Pass = arguments.CommandResult.ValueForOption<string>("--pass");
            RecoveryInterval = TimeSpan.FromMilliseconds(Math.Abs(arguments.CommandResult.ValueForOption<int>("--recovery-interval")));
            RecoveryAttempts = arguments.CommandResult.ValueForOption<byte>("--recovery-attempts");

            Console.WriteLine(RecoveryInterval);
            Console.WriteLine(RecoveryAttempts);

            // Invoke execution
            cmd.InvokeAsync(args).Wait();
        }

        public static void Publish(string exchange, int interval, int size)
        {
            var cc = CreateConnection();
            var cn = CreateChannel(cc);

            size = Math.Abs(size);
            interval = Math.Abs(interval);
            var i = 1;

            // Wait until key press
            while (!Console.KeyAvailable)
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
                    Console.WriteLine($"Publish Exception :: " + ex.GetType());
                    cn = WaitAutoRecovery(cc, cn);
                }

                Thread.Sleep(interval);
            }

            // Closing
            if (cn?.IsOpen ?? false) cn.Close();
            cn?.Dispose();

            if (cc?.IsOpen ?? false) cc.Close();
            cc?.Dispose();
        }

        public static void Subscribe(string[] queues, int interval)
        {
            var Exiting = false;
            var consumersToQueues = new Dictionary<string, string>();

            var cc = CreateConnection();
            var cn = CreateChannel(cc);
            var cs = CreateConsumer(cn);
            cs.Received += Handler;

            cs.ConsumerCancelled += (sender, ea) =>
            {
                if (!Exiting)
                {
                    WaitAutoRecovery(cc, cs.Model);
                }
            };

            void Handler(object sender, BasicDeliverEventArgs ea)
            {
                interval = Math.Abs(interval);

                lock (consumersToQueues)
                {
                    var msg = Encoding.UTF8.GetString(ea.Body.ToArray());
                    if (string.IsNullOrEmpty(msg))
                    {
                        msg = "Empty message";
                    }
                    var nli = msg.IndexOf("\n");
                    msg = msg.Substring(0, nli < 0 ? msg.Length : nli);
                    
                    if (cs.Model != cn)
                    {
                        Console.WriteLine(" ******************** NOT EQUAL ******************** ");
                    }

                    if (consumersToQueues.TryGetValue(ea.ConsumerTag, out string queue))
                    {
                        Thread.Sleep(interval);
                        Console.Write($"{msg} received from {queue}! (#{ea.DeliveryTag})");
                        if  (cs.Model.IsOpen)
                        {
                            cs.Model.BasicAck(ea.DeliveryTag, false);
                            Console.WriteLine(" ... ack ok.");
                        }
                        else
                        {
                            Console.WriteLine($" ... but not ack, channel is closed.");
                        }
                    }
                    else
                    {
                        Console.WriteLine($"{msg} received! (#{ea.DeliveryTag}) ... ignored because channel was stopping.");
                    }
                }
            }

            void StartConsuming(IModel cn, EventingBasicConsumer cs)
            {
                Console.WriteLine($"{nameof(StartConsuming)}...");

                lock (consumersToQueues)
                {
                    if (cn?.IsOpen ?? false)
                    {
                        foreach (var queue in queues)
                        {
                            var tag = cn.BasicConsume(queue, false, cs);
                            consumersToQueues.Add(tag, queue);
                        }
                    }
                }
            }

            void StopConsuming(IModel cn)
            {
                Console.WriteLine($"{nameof(StopConsuming)}...");

                lock (consumersToQueues)
                {
                    if (cn?.IsOpen ?? false)
                    {
                        foreach (var tag in consumersToQueues)
                        {
                            cn.BasicCancel(tag.Key);
                            Console.WriteLine($"{nameof(StopConsuming)} {tag.Key} stoped.");
                        }
                    }

                    consumersToQueues.Clear();
                }
            }

            StartConsuming(cn, cs);
            while (!Console.KeyAvailable && !Exiting)
            {
                Thread.Sleep(500);
            }
            Exiting = true;
            StopConsuming(cn);

            // Closing
            if (cn?.IsOpen ?? false) cn.Close();
            cn?.Dispose();

            if (cc?.IsOpen ?? false) cc.Close();
            cc?.Dispose();
        }

        private static IConnection CreateConnection()
        {
            var cf = new ConnectionFactory
            {
                HostName = Host,
                Port = Port,
                UserName = User,
                Password = Pass,
                AutomaticRecoveryEnabled = true,
                NetworkRecoveryInterval = RecoveryInterval,
                RequestedHeartbeat = RecoveryInterval
            };

            var cc = cf.CreateConnection();

            cc.ConnectionBlocked += (sender, ea) => Console.WriteLine($"Connection Event :: ConnectionBlocked :: {ea.Reason}");
            cc.ConnectionUnblocked += (sender, ea) => Console.WriteLine($"Connection Event :: ConnectionUnblocked");
            cc.ConnectionShutdown += (sender, ea) => Console.WriteLine($"Connection Event :: ConnectionShutdown :: {ea.ReplyCode} - {ea.ReplyText}");
            cc.CallbackException += (sender, ea) => Console.WriteLine($"Connection Event :: CallbackException :: " + ea.Exception);

            Console.WriteLine("New Connection Created!");

            return cc;
        }

        private static IModel CreateChannel(IConnection cc)
        {
            var cn = cc.CreateModel();

            cn.BasicReturn += (sender, ea) => Console.WriteLine($"Channel Event :: BasicReturn :: {ea.ReplyCode} - {ea.ReplyText}");
            cn.BasicNacks += (sender, ea) => Console.WriteLine($"Channel Event :: BasicNacks :: DeliveryTag {ea.DeliveryTag}");
            cn.ModelShutdown += (sender, ea) => Console.WriteLine($"Channel Event :: ModelShutdown :: {ea.ReplyCode} - {ea.ReplyText}");
            cn.CallbackException += (sender, ea) => Console.WriteLine($"Channel Event :: CallbackException :: " + ea.Exception);

            cn.ConfirmSelect();
            cn.BasicQos(0, 10, true);

            Console.WriteLine("New Channel Created!");

            return cn;
        }

        private static EventingBasicConsumer CreateConsumer(IModel cn)
        {
            var cs = new EventingBasicConsumer(cn);

            cs.Registered += (sender, ea) => Console.WriteLine($"Consumer Event :: Registered :: {string.Join(", ", ea.ConsumerTags)}");
            cs.Unregistered += (sender, ea) => Console.WriteLine($"Consumer Event :: Unregistered :: {string.Join(", ", ea.ConsumerTags)}");
            cs.ConsumerCancelled += (sender, ea) => Console.WriteLine($"Consumer Event :: ConsumerCancelled :: {string.Join(", ", ea.ConsumerTags)}");
            cs.Shutdown += (sender, ea) => Console.WriteLine($"Consumer Event :: Shutdown :: {ea.Initiator} - {ea.ReplyCode} - {ea.ReplyText}");

            Console.WriteLine("New Consumer Created!");

            return cs;
        }

        private static IModel WaitAutoRecovery(IConnection cc, IModel cn, byte retry = 1)
        {
            if (!cc.IsOpen)
            {
                if (retry > RecoveryAttempts)
                {
                    Console.WriteLine("Max recovery attempts reached. Sorry, I tried.");
                    Environment.Exit(1);
                }
                var wait = Convert.ToInt32(RecoveryInterval.TotalSeconds);
                Console.WriteLine($"{nameof(WaitAutoRecovery)} :: #{retry}/{RecoveryAttempts} :: Waiting {wait} seconds before checking connection recovery.");
                Thread.Sleep(wait * 1000);
                WaitAutoRecovery(cc, cn, ++retry);
            }
            else if (!cn.IsOpen)
            {
                Console.WriteLine($"{nameof(WaitAutoRecovery)} :: Connection restored but channel is closed.");
                return CreateChannel(cc);
            }

            return cn;
        }
    }
}
