// Minimal stand-in for the email micro-service so the framework runs end to end.
//   POST /api/v1/emails          -> validates, stores, publishes QUEUED to email.events, returns 202
//   GET  /api/v1/emails/:id      -> current state
//   DELETE /api/v1/emails/:id    -> cancels a QUEUED email
// Kafka: consumes `email.send.requested`, "sends" the mail and publishes SENT to `email.events`.
const http = require('http');
const crypto = require('crypto');
const { Kafka } = require('kafkajs');

const REQUESTS_TOPIC = 'email.send.requested';
const EVENTS_TOPIC = 'email.events';
const EMAIL_RE = /^[^@\s]+@[^@\s]+\.[^@\s]+$/;
const emails = new Map();

const kafka = new Kafka({ clientId: 'email-service-stub', brokers: [process.env.KAFKA_BROKERS || 'kafka:9092'] });
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'email-service' });

const publish = (email) =>
  producer.send({
    topic: EVENTS_TOPIC,
    messages: [{ key: email.id, value: JSON.stringify({ id: email.id, to: email.to, status: email.status, timestamp: new Date().toISOString() }) }],
  });

const json = (res, code, body) => {
  res.writeHead(code, { 'Content-Type': 'application/json' });
  res.end(body === undefined ? '' : JSON.stringify(body));
};

const readBody = (req) => new Promise((resolve) => {
  let data = '';
  req.on('data', (c) => (data += c));
  req.on('end', () => { try { resolve(JSON.parse(data)); } catch { resolve(null); } });
});

function validate(b) {
  const errors = [];
  if (!b || typeof b !== 'object') return ['body must be valid JSON'];
  if (!b.to || !EMAIL_RE.test(b.to)) errors.push('to: must be a valid email address');
  if (!b.subject || !String(b.subject).trim()) errors.push('subject: must not be blank');
  if (b.subject && String(b.subject).length > 200) errors.push('subject: max length is 200');
  if (!b.body || !String(b.body).trim()) errors.push('body: must not be blank');
  return errors;
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://x');
  const m = url.pathname.match(/^\/api\/v1\/emails(?:\/([\w-]+))?$/);

  if (url.pathname === '/health') return json(res, 200, { status: 'UP' });
  if (!m) return json(res, 404, { error: 'Not Found' });

  if (req.method === 'POST' && !m[1]) {
    if (req.headers['x-api-key'] !== 'test-api-key') return json(res, 401, { error: 'Unauthorized' });
    const body = await readBody(req);
    const errors = validate(body);
    if (errors.length) return json(res, 400, { error: 'Validation failed', details: errors });
    const email = { id: crypto.randomUUID(), to: body.to, subject: body.subject, body: body.body, status: 'QUEUED' };
    emails.set(email.id, email);
    await publish(email);
    return json(res, 202, { id: email.id, status: email.status });
  }
  if (req.method === 'GET' && m[1]) {
    const e = emails.get(m[1]);
    return e ? json(res, 200, e) : json(res, 404, { error: 'Email not found' });
  }
  if (req.method === 'DELETE' && m[1]) {
    const e = emails.get(m[1]);
    if (!e) return json(res, 404, { error: 'Email not found' });
    if (e.status !== 'QUEUED') return json(res, 409, { error: 'Only QUEUED emails can be cancelled' });
    e.status = 'CANCELLED';
    await publish(e);
    return json(res, 204);
  }
  return json(res, 405, { error: 'Method Not Allowed' });
});

(async () => {
  const admin = kafka.admin();
  await admin.connect();
  await admin.createTopics({ topics: [REQUESTS_TOPIC, EVENTS_TOPIC].map((topic) => ({ topic })) });
  await admin.disconnect();
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: REQUESTS_TOPIC, fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ message }) => {
      let req;
      try { req = JSON.parse(message.value.toString()); } catch { return; }
      if (validate(req).length) return; // invalid messages are dropped
      const email = { id: req.id || crypto.randomUUID(), to: req.to, subject: req.subject, body: req.body, status: 'SENT' };
      emails.set(email.id, email);
      await publish(email);
    },
  });
  server.listen(process.env.PORT || 8080, () => console.log('email-service-stub ready'));
})().catch((e) => { console.error(e); process.exit(1); });
