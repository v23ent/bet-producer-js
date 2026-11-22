import confluent from '@confluentinc/kafka-javascript';

const { Kafka } = confluent.KafkaJS;
const configKafka = {
    "bootstrap.servers": "lkc-gq338g3.us-west-2.aws.public.confluent.cloud:9092",
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": "2QDHBCKO4LTH5HUG",
    "sasl.password": "cfltD8ax9MktYvzRVXfFkrApcdj1+TEXReZM8fvlQ9QDMAEWYwbhnyhxfCEsrYwA"
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