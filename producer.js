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

    return {
        "bet_id": crypto.randomUUID(),
        "account_id": choice(ACCOUNTS),
        "event_id": choice(EVENTS),
        "market": choice(["WIN", "PLACE", "EACH_WAY"]),
        "stake": 100,
        "odds": 1 + 20 * Math.random(),
        "channel": choice(["APP", "WEB", "RETAIL"]),
        "placed_at": Date.now()
    }
}

const kafka = new Kafka();
let producer = kafka.producer(configKafka);

await producer.connect();

let bet = make_bet();
let result = await producer.send({
        "topic": "bets",
        "messages": [
            {
                "key": JSON.stringify(bet.account_id),
                "value": JSON.stringify(bet)
            }
        ]
    }
);

await producer.disconnect();

console.log(result);