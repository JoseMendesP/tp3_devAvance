const { Kafka } = require('kafkajs');
const redis = require('redis');


const client = redis.createClient();

function incrementWordCount(mot) {
    client.incr(mot, (err, reply) => {
        if (err) {
            console.error('Erreur lors de l\'incrémentation du compteur pour le mot', mot, err);
        } else {
            console.log('Compteur pour le mot', mot, 'incrémenté avec succès. La nouvelle valeur :', reply);
        }
    });
}

async function connexion() {
    const redpanda = new Kafka({
        brokers: ['localhost:19092'],
    });

    const consumer = redpanda.consumer({ groupId: 'group304' });

    await consumer.connect();
    console.log('Bien connecté au broker RedPanda.');

    await consumer.subscribe({ topic: 'mon-super-topic', fromBeginning: true });

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const messageValue = message.value.toString();
            console.log({
                value: messageValue,
                timestamp: new Date(message.timestamp).toLocaleString()
            });


            const mots = messageValue.split(/\s+/);
            mots.forEach(mot => {
                incrementWordCount(mot.toLowerCase()); 
            });
        },
    });
}

connexion().catch(console.error);
