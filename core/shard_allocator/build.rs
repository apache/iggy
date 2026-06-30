// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Vendored `hwloc` references `cbrt`, which makes the linker pull in a
// `libm`. musl folds the math functions into its `libc`, so the Rust
// musl sysroot ships no `libm.a`. Without one, `-lm` falls through to
// the host glibc's `libm.a`, whose `cbrt` needs glibc-internal
// `__frexp`/`__ldexp` symbols that do not exist on musl, and the static
// link fails. Drop an empty `libm.a` stub on the search path so `-lm`
// resolves to nothing and `cbrt` is satisfied later by musl's own
// `libc`. No effect on non-musl targets.

use std::env;
use std::fs;
use std::path::Path;

fn main() {
    if env::var("CARGO_CFG_TARGET_ENV").as_deref() != Ok("musl") {
        return;
    }

    let out_dir = env::var("OUT_DIR").expect("OUT_DIR is set by cargo for build scripts");
    let stub = Path::new(&out_dir).join("libm.a");

    // `!<arch>\n` is the canonical header of an empty `ar` archive.
    fs::write(&stub, b"!<arch>\n").expect("write empty libm.a stub");

    println!("cargo:rustc-link-search=native={out_dir}");
}
