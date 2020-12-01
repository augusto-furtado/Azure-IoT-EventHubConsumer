using System;
using Microsoft.Azure.EventHubs;
using System.Threading.Tasks;
using System.Text;
using System.Collections.Generic;

namespace OperatorApp
{
    class Program
    {

        // Set the Event Hub-compatible endpoint. At the Event Hub, get the string connection from your "listen" shared access policy
        //split the event hub ednpoint in the variables below: Endpoint=sb://eventhubbyaugusto.servicebus.windows.net/;SharedAccessKeyName=listentelemetry;SharedAccessKey=8fflOEDO1TuUjDYAYzzBW6XW9HKYktvZBtiluAfStcE=;EntityPath=eventhubfordevices
        private readonly static string s_eventHubsCompatibleEndpoint = "sb://eventhubbyaugusto.servicebus.windows.net/";
        private readonly static string s_eventHubsCompatiblePath = "eventhubfordevices";
        private readonly static string s_iotHubSasKey = "8fflOEDO1TuUjDYAYzzBW6XW9HKYktvZBtiluAfStcE=";
        private readonly static string s_iotHubSasKeyName = "listentelemetry";
        //set the consumer group created on the event hub instance or leave the default
        private readonly static string s_consumerGroup = "$Default";
        private static EventHubClient s_eventHubClient;

        
        // Asynchronously create a PartitionReceiver for a partition and then start reading any messages sent from the simulated client.
        private static async Task ReceiveMessagesFromDeviceAsync(string partition)
        {
            // Create the receiver using the consumer group
            var eventHubReceiver = s_eventHubClient.CreateReceiver(s_consumerGroup, partition, EventPosition.FromEnqueuedTime(DateTime.Now));
            Console.WriteLine("Created receiver on partition: " + partition);

            while (true)
            {
                // Check for EventData - this methods times out if there is nothing to retrieve.
                var events = await eventHubReceiver.ReceiveAsync(100);

                // If there is data in the batch, process it.
                if (events == null) continue;

                foreach (EventData eventData in events)
                {
                    string data = Encoding.UTF8.GetString(eventData.Body.Array);

                    greenMessage("Message received on Event Hub: " + data);

                    foreach (var prop in eventData.Properties)
                    {
                        if (prop.Value.ToString() == "true")
                        {
                           redMessage(prop.Key);
                        }
                    }
                    Console.WriteLine();
                }
            }

        }

        private static void colorMessage(string text, ConsoleColor clr)
        {
            Console.ForegroundColor = clr;
            Console.WriteLine(text);
            Console.ResetColor();
        }

        private static void greenMessage(string text)
        {
            colorMessage(text, ConsoleColor.Green);
        }

        private static void redMessage(string text)
        {
            colorMessage(text, ConsoleColor.Red);
        }

        static void Main(string[] args)
        {
            // Create an EventHubClient instance to connect to the IoT Hub Event Hubs-compatible endpoint.
            var connectionString = new EventHubsConnectionStringBuilder(new Uri(s_eventHubsCompatibleEndpoint), s_eventHubsCompatiblePath, s_iotHubSasKeyName, s_iotHubSasKey);
            s_eventHubClient = EventHubClient.CreateFromConnectionString(connectionString.ToString());

            // Create a PartitionReceiver for each partition on the hub.
            var runtimeInfo = s_eventHubClient.GetRuntimeInformationAsync().GetAwaiter().GetResult();
            var d2cPartitions = runtimeInfo.PartitionIds;

            // Create receivers to listen for messages.
            var tasks = new List<Task>();
            foreach (string partition in d2cPartitions)
            {
                tasks.Add(ReceiveMessagesFromDeviceAsync(partition));
            }

            // Wait for all the PartitionReceivers to finish.
            Task.WaitAll(tasks.ToArray());
        }
    }
}
