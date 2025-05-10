/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import { WebTransport } from '@fails-components/webtransport';
// import {
//   Http3WebTransportClient
// } from '@fails-components/webtransport-transport-http3-quiche';


const createQuicSocket = ({port, host, ...rest}) => {
  return new WebTransport({port, host, ...rest});
};

console.log(WebTransport);

let isOpen = false;

async function readData(reader) {

  reader.closed
    .catch((err) => console.error("Failed to close", err.toString()))
    .finally(() => isOpen = false);

  while (isOpen) {
    try {
      const { done, value } = await reader.read();
      if (done) { break; }

      console.log("Received:", value);
    } catch (err) {
      console.log("Failed to read...", err.toString());
      break;
    }
  }
};



/**
 * @param {string} [certHash]
 * @returns {Uint8Array}
 */
export function readCertHash(certHash) {
  return Uint8Array.from(`${certHash}`.split(':').map((i) => parseInt(i, 16)))
}

const main = async () => {

  try {
    // const transport = new WebTransport("https://demo.web-transport.io:4433");
    const transport = new WebTransport("https://localhost:8080");

    
    // console.log('init', socket)
    console.dir(transport);
    // const wtOptions = {
    //   serverCertificateHashes: [
    //     {
    //       algorithm: 'sha-256',
    //       value: readCertHash(process.env.CERT_HASH)
    //     }
    //   ],
    //   // @ts-ignore
    //   forceReliable: true
    // }

    // const transport = new WebTransport("https://localhost:8080");
    // // console.dir(transport);

    // transport.ready.then((...arg) => console.log('READY !', arg));

    // const readableStream = transport.datagrams.readable.getReader();
    // console.dir(readableStream);

    // const writer = transport.datagrams.writable.getWriter();
    // console.dir(writer);

    // isOpen = true;
    // readData(readableStream);
  } catch (err) {
    console.error('connexion failed', err);
    isOpen = false;
  }


};

main();
