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

import { onMount } from 'svelte';

type Key =
  | 'Backspace'
  | 'Tab'
  | 'Enter'
  | 'Shift'
  | 'Ctrl'
  | 'Alt'
  | 'PauseBreak'
  | 'CapsLock'
  | 'Escape'
  | 'Space'
  | 'PageUp'
  | 'PageDown'
  | 'End'
  | 'Home'
  | 'LeftArrow'
  | 'UpArrow'
  | 'RightArrow'
  | 'DownArrow'
  | 'Insert'
  | 'Delete'
  | 'Zero'
  | 'ClosedParen'
  | 'One'
  | 'ExclamationMark'
  | 'Two'
  | 'AtSign'
  | 'Three'
  | 'PoundSign'
  | 'Hash'
  | 'Four'
  | 'DollarSign'
  | 'Five'
  | 'PercentSign'
  | 'Six'
  | 'Caret'
  | 'Hat'
  | 'Seven'
  | 'Ampersand'
  | 'Eight'
  | 'Star'
  | 'Asterisk'
  | 'Nine'
  | 'OpenParen'
  | 'A'
  | 'B'
  | 'C'
  | 'D'
  | 'E'
  | 'F'
  | 'G'
  | 'H'
  | 'I'
  | 'J'
  | 'K'
  | 'L'
  | 'M'
  | 'N'
  | 'O'
  | 'P'
  | 'Q'
  | 'R'
  | 'S'
  | 'T'
  | 'U'
  | 'V'
  | 'W'
  | 'X'
  | 'Y'
  | 'Z'
  | 'LeftWindowKey'
  | 'RightWindowKey'
  | 'SelectKey'
  | 'Numpad0'
  | 'Numpad1'
  | 'Numpad2'
  | 'Numpad3'
  | 'Numpad4'
  | 'Numpad5'
  | 'Numpad6'
  | 'Numpad7'
  | 'Numpad8'
  | 'Numpad9'
  | 'Multiply'
  | 'Add'
  | 'Subtract'
  | 'DecimalPoint'
  | 'Divide'
  | 'F1'
  | 'F2'
  | 'F3'
  | 'F4'
  | 'F5'
  | 'F6'
  | 'F7'
  | 'F8'
  | 'F9'
  | 'F10'
  | 'F11'
  | 'F12'
  | 'NumLock'
  | 'ScrollLock'
  | 'SemiColon'
  | 'Equals'
  | 'Comma'
  | 'Dash'
  | 'Period'
  | 'UnderScore'
  | 'PlusSign'
  | 'ForwardSlash'
  | 'Tilde'
  | 'GraveAccent'
  | 'OpenBracket'
  | 'ClosedBracket'
  | 'Quote';

export function addOnKeyDownListener(key: Key, handler: (e: KeyboardEvent) => void) {
  onMount(() => {
    const eventHandler = (e: KeyboardEvent) => {
      if (e.key === key) handler(e);
    };

    window.addEventListener('keydown', eventHandler);
    return () => window.removeEventListener('keydown', eventHandler);
  });
}
