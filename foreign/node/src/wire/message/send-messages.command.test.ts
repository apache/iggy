
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { uuidv7, uuidv4 } from 'uuidv7'
import { SEND_MESSAGES, type SendMessages } from './send-messages.command.js';
import type { MessageIdKind } from './message.utils.js';
import { HeaderValue } from './header.utils.js';

describe('SendMessages', () => {

  describe('serialize', () => {

    const t1 = {
      streamId: 911,
      topicId: 213,
      messages: [
        { payload: 'a' },
        { id: 0 as const, payload: 'b' },
        { id: 0n as const, payload: 'c' },
        { id: uuidv4(), payload: 'd' },
        { id: uuidv7(), payload: 'e' },
      ],
    };

    it('serialize SendMessages into a buffer', () => {
      assert.deepEqual(
        SEND_MESSAGES.serialize(t1).length,
        6 + 6 + 2 + 25 * 5
      );
    });

    it('serialize all kinds of messageId', () => {
      assert.doesNotThrow(
        () => SEND_MESSAGES.serialize(t1),
      );
    });

    
    it('throw on invalid number message id', () => {
      const t = { ...t1, messages: [{ id: 42 as MessageIdKind, payload: 'm' }] };
      assert.throws(
        () => SEND_MESSAGES.serialize(t)
      );
    });

    it('throw on invalid bigint message id', () => {
      const t = { ...t1, messages: [{ id: 123n as MessageIdKind, payload: 'm' }] };
      assert.throws(
        () => SEND_MESSAGES.serialize(t)
      );
    });

    it('throw on invalid string message id', () => {
      const t = { ...t1, messages: [{ id: 'foo', payload: 'm' }] };
      assert.throws(
        () => SEND_MESSAGES.serialize(t)
      );
    });

    it('serialize message with headers', () => {
      const t: SendMessages = {
        streamId: 911,
        topicId: 213,
        messages: [
          { payload: 'm', headers: { p: HeaderValue.Bool(true) } },
          { payload: 'q', headers: { 'v-aze': HeaderValue.Uint8(128) } },
          { payload: 'x', headers: { q: HeaderValue.Double(1/3) } },
          { payload: 's', headers: { x: HeaderValue.Uint32(123) } },
          { payload: 'r', headers: { y: HeaderValue.Uint64(42n) } },
          { payload: 'g', headers: { y: HeaderValue.Float(42.3) } },
          { payload: 'c', headers: { ID: HeaderValue.String(uuidv7()) } },
          { payload: 'l', headers: { val: HeaderValue.Raw(Buffer.from(uuidv4())) } }
        ]
      };
      assert.doesNotThrow(
        () => SEND_MESSAGES.serialize(t)
      );
    });



  });
});
