// The coordinator must go idle: with nothing but pings, Cloudflare evicts it from memory, while its connections
// stay open and its membership waits in storage for the next frame to wake it.
import { runInDurableObject } from "cloudflare:test";
import { env, exports } from "cloudflare:workers";
import { expect, it } from "vitest";

const SUBSCRIBE = 1;
const PUBLISH = 3;
const ACKNOWLEDGE = 4;

// frame is a client frame: a tag byte, then little endian integers, then any message bytes.
function frame(tag, integers, message = []) {
  const bytes = new Uint8Array(1 + 8 * integers.length + message.length);
  const view = new DataView(bytes.buffer);
  bytes[0] = tag;
  integers.forEach((integer, i) => view.setBigUint64(1 + 8 * i, BigInt(integer), true));
  bytes.set(message, 1 + 8 * integers.length);
  return bytes;
}

// decode is the server frame data holds, or the text of a ping's answer.
function decode(data) {
  if (typeof data === "string") {
    return { text: data };
  }
  const bytes = new Uint8Array(data);
  const view = new DataView(data);
  const integer = (i) => view.getBigUint64(1 + 8 * i, true);
  switch (bytes[0]) {
    case 6:
      return { subscribed: integer(0), last: null };
    case 7:
      return { subscribed: integer(0), last: [...bytes.slice(9)] };
    case 8:
      return { publisher: integer(0), request: integer(1), key: integer(2), message: [...bytes.slice(25)] };
    case 9:
      return { done: integer(0) };
  }
  throw new Error(`frame ${bytes[0]} is not a server frame`);
}

// connect opens a connection to volume's coordinator. Its next resolves to the next frame it receives.
async function connect(volume) {
  const response = await exports.default.fetch(`https://coordinator/volume/${volume}`, {
    headers: { Upgrade: "websocket" },
  });
  const socket = response.webSocket;
  socket.binaryType = "arraybuffer";
  socket.accept();
  const received = [];
  const waiting = [];
  socket.addEventListener("message", (event) => {
    const frame = decode(event.data);
    const waiter = waiting.shift();
    waiter ? waiter(frame) : received.push(frame);
  });
  socket.next = () =>
    received.length > 0 ? Promise.resolve(received.shift()) : new Promise((resolve) => waiting.push(resolve));
  return socket;
}

const sleep = (milliseconds) => new Promise((resolve) => setTimeout(resolve, milliseconds));

it("an idle coordinator is evicted while its holders stay connected", { timeout: 30_000 }, async () => {
  const volume = crypto.randomUUID();
  const coordinator = env.COORDINATOR.get(env.COORDINATOR.idFromName(volume));
  const holder = await connect(volume);
  const publisher = await connect(volume);
  // The mark lasts as long as the object stays in memory, which a frame does not end.
  await runInDurableObject(coordinator, (object) => {
    object.marked = true;
  });
  holder.send(frame(SUBSCRIBE, [7]));
  expect(await holder.next()).toEqual({ subscribed: 7n, last: null });
  expect(await runInDurableObject(coordinator, (object) => object.marked)).toBe(true);

  // Cloudflare answers pings without waking the object, and evicts it after 10 seconds without events.
  for (let second = 0; second < 12; second++) {
    await sleep(1000);
    holder.send("ping");
    expect(await holder.next()).toEqual({ text: "pong" });
  }
  expect(await runInDurableObject(coordinator, (object) => object.marked)).toBeUndefined();

  publisher.send(frame(PUBLISH, [1, 7], [42]));
  const delivered = await holder.next();
  expect(delivered).toMatchObject({ request: 1n, key: 7n, message: [42] });
  holder.send(frame(ACKNOWLEDGE, [delivered.publisher, 1]));
  expect(await publisher.next()).toEqual({ done: 1n });
});
