const amqp = require('amqplib');
const express = require ('express');
const cors = require('cors');

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://rabbitmq';
const EXCHANGE = 'light_hub';
const LIGHT_TOGGLED_EVENT = 'light.toggled';

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

function startHttpServer(channel) {
    const app = express();
    app.use(cors());

    app.post('/commands/toggle-light', async (req, res) => {
        const event = { type: 'LightToggled' }

        channel.publish(
            EXCHANGE,
            LIGHT_TOGGLED_EVENT,
            Buffer.from(JSON.stringify(event)),
            { persistent: true });

        res.status(202).json({ status: 'Request published' });
    });

    app.listen(3001, () => {
        console.log('Producer on 3001');
    });
}

async function start() {
    try {
        const channel = await connectToRabbitMQ();
        startHttpServer(channel);
    } catch (err) {
        console.error('Failed to start producer', err);
        process.exit(1);
    }
}

start();