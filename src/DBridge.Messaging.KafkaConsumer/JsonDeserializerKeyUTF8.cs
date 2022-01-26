namespace DBridge.Messaging.KafkaConsumer
{
    using Confluent.Kafka;

    using Newtonsoft.Json;

    using System;
    using System.Text;

    internal class JsonDeserializerUTF8<TKeyType> : IDeserializer<TKeyType>
    {
        public readonly Encoding encoder;

        public JsonDeserializerUTF8()
        {
            encoder = Encoding.UTF8;
        }

        public TKeyType Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            return JsonConvert.DeserializeObject<TKeyType>(encoder.GetString(data.ToArray()));
        }
    }
}
