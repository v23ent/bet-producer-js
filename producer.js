import confluent from '@confluentinc/kafka-javascript';
import 'dotenv/config';

const { Kafka } = confluent.KafkaJS;
const configKafka = {
    "bootstrap.servers": process.env.KAFKA_BOOTSTRAP,
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": process.env.KAFKA_KEY,
    "sasl.password": process.env.KAFKA_SECRET
}

const choice = (arr) => arr[Math.floor(Math.random() * arr.length)];

function make_bet() {
    const ACCOUNTS = Array.from({ length: 500 }, (_, i) => `acc-${String(i).padStart(4, "0")}`);
    const EVENTS = Array.from({ length: 20 }, (_, i) => `race-${i + 1}`);
    const MARKETS = ["WIN", "PLACE", "EACH_WAY"];
    const CHANNELS = ["APP", "WEB", "RETAIL"];

    return {
        "bet_id": crypto.randomUUID(),
        "account_id": choice(ACCOUNTS),
        "event_id": choice(EVENTS),
        "market": choice(MARKETS),
        "stake": 100,
        "odds": 1 + 20 * Math.random(),
        "channel": choice(CHANNELS),
        "placed_at": Date.now()
    }
}

const kafka = new Kafka();
let producer = kafka.producer(configKafka);

async function main() {
    await producer.connect();

    try {
        const bet = make_bet();
        const result = await producer.send({
            "topic": "bets",
            "messages": [
                {
                    "key": JSON.stringify(bet.account_id),
                    "value": JSON.stringify(bet)
                }
            ]
        });

        console.log(result);
    } finally {
        await producer.disconnect();
    }
}

main().catch((error) => {
    console.error("Error producing bet:", error);
    process.exitCode = 1;
});