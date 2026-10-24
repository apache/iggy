/* Licensed to the Apache Software Foundation (ASF) under one
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

use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{Read, Write},
    mem,
    path::Path,
};

use pyo3_stub_gen::{
    Result, TypeInfo,
    generate::{ClassDef, EnumDef, MemberDef},
};

const LICENSE: &str = "# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# \"License\"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# \"AS IS\" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

";

fn main() -> Result<()> {
    // `stub_info` is a function defined by `define_stub_info_gatherer!` macro.
    let mut stub = apache_iggy::client::stub_info()?;
    for module in stub.modules.values_mut() {
        let enums = mem::take(&mut module.enum_);
        module.class.extend(
            enums
                .into_iter()
                .map(|(type_id, enum_def)| (type_id, native_class(enum_def))),
        );
    }
    stub.generate()?;
    // Anchor on the manifest dir: `stub.generate()` writes to the crate root, so
    // a cwd-relative path leaves the tracked stub without its license header.
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("apache_iggy.pyi");
    let mut f = File::open(&path)?;
    let mut content = LICENSE.to_owned();
    f.read_to_string(&mut content)?;
    content = content.replacen("__all__ = [", "__all__ = [\n    \"Durability\",", 1);
    // Durability is the only `enum.Enum` left once the pyo3 enums are plain classes.
    content = content.replacen("import typing\n", "import enum\nimport typing\n", 1);
    // pyo3-stub-gen splits the hyphenated project name around a base class from this module.
    content = content.replace("(apacheIggyError)", "(IggyError)");
    // The same split hits a pyclass-typed field of a complex enum variant.
    content = content.replace("apacheMetadataChangeCursor", "MetadataChangeCursor");
    // The runtime creates a str-backed Enum, whose str() differs from StrEnum.
    content.push_str("\nclass Durability(str, enum.Enum):  # noqa: UP042\n    REPLICATED = 'replicated'\n    PERSISTED = 'persisted'\n");

    let mut f = File::create(path)?;
    f.write_all(content.as_bytes())?;
    Ok(())
}

/// A pyo3 enum is a plain class whose variants are class attributes, not an `enum.Enum`.
fn native_class(enum_def: EnumDef) -> ClassDef {
    let variant_type = TypeInfo {
        name: format!("typing.ClassVar[{}]", enum_def.name),
        source_module: None,
        import: HashSet::from(["typing".into()]),
        type_refs: HashMap::new(),
    };
    let variants = enum_def.variants.iter().map(|&(name, doc)| MemberDef {
        name,
        r#type: variant_type.clone(),
        doc,
        default: None,
        deprecated: None,
    });
    let mut class = ClassDef {
        name: enum_def.name,
        module: enum_def.module,
        doc: enum_def.doc,
        attrs: variants.chain(enum_def.attrs).collect(),
        getter_setters: Default::default(),
        methods: Default::default(),
        bases: Vec::new(),
        classes: Vec::new(),
        match_args: None,
        subclass: false,
    };
    for getter in enum_def.getters {
        class
            .getter_setters
            .insert(getter.name.to_string(), (Some(getter), None));
    }
    for setter in enum_def.setters {
        let name = setter.name.to_string();
        class.getter_setters.entry(name).or_default().1 = Some(setter);
    }
    for method in enum_def.methods {
        class
            .methods
            .entry(method.name.to_string())
            .or_default()
            .push(method);
    }
    class
}
