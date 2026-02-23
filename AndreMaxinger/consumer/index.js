const amqp = require('amqplib');
const express = require ('express');
const cors = require('cors');

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://rabbitmq';
const EXCHANGE = 'light_hub';
const LIGHT_TOGGLED_EVENT = 'light.toggled';
const QUEUE = 'light_read_model';

let state = { on: false };

function handleEvent(event) {
    if (event.type === 'LightToggled') {
        state.on = !state.on;
    }
}

async function connectToRabbitMQ() {
    const connection = await amqp.connect(RABBITMQ_URL);

    connection.on('close', () => {
        console.error('RabbitMQ connection closed');
        process.exit(1);
    })

    const channel = await connection.createChannel();
    await channel.assertExchange(EXCHANGE, 'direct', { durable: true });

    return channel;
}
            
async function setupSubscription(channel) {
    const q = await channel.assertQueue(QUEUE, { durable: true} );
    await channel.bindQueue(q.queue, EXCHANGE, LIGHT_TOGGLED_EVENT);

    await channel.consume(q.queue, (msg) => {
        if (!msg) return;

        try {
            const event = JSON.parse(msg.content.toString());
            handleEvent(event);
            channel.ack(msg);
        } catch (err) {
            console.error('Failed to process message', err);
            channel.nack(msg);
        }
    });
}

function startHttpServer() {
    const app = express();
    app.use(cors());

    app.get('/queries/light-state', async (req, res) => {
        res.json(state) 
    });

    app.listen(3002, () => {
        console.log('Consumer on 3002')
    });
}

async function start() {
    try {
        const channel = await connectToRabbitMQ();
        await setupSubscription(channel);
        startHttpServer();
    } catch (err) {
        console.error('Failed to start consumer', err);
        process.exit(1);
    }
}

start();