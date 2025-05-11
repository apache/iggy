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

use bench_report::hardware::BenchmarkHardware;
use std::rc::Rc;
use yew::prelude::*;

#[derive(Clone, Debug, PartialEq, Default)]
pub struct HardwareState {
    pub hardware_list: Vec<BenchmarkHardware>,
    pub selected_hardware: Option<String>,
}

pub enum HardwareAction {
    SetHardwareList(Vec<BenchmarkHardware>),
    SelectHardware(Option<String>),
}

#[derive(Clone, PartialEq)]
pub struct HardwareContext {
    pub state: HardwareState,
    pub dispatch: Callback<HardwareAction>,
}

impl HardwareContext {
    pub fn new(state: HardwareState, dispatch: Callback<HardwareAction>) -> Self {
        Self { state, dispatch }
    }
}

impl Reducible for HardwareState {
    type Action = HardwareAction;

    fn reduce(self: Rc<Self>, action: Self::Action) -> Rc<Self> {
        let next_state = match action {
            HardwareAction::SetHardwareList(hardware_list) => HardwareState {
                hardware_list,
                selected_hardware: self.selected_hardware.clone(),
            },
            HardwareAction::SelectHardware(hardware) => HardwareState {
                hardware_list: self.hardware_list.clone(),
                selected_hardware: hardware,
            },
        };

        next_state.into()
    }
}

#[derive(Properties, PartialEq)]
pub struct HardwareProviderProps {
    #[prop_or_default]
    pub children: Children,
}

#[function_component(HardwareProvider)]
pub fn hardware_provider(props: &HardwareProviderProps) -> Html {
    let state = use_reducer(HardwareState::default);

    let context = HardwareContext::new(
        (*state).clone(),
        Callback::from(move |action| state.dispatch(action)),
    );

    html! {
        <ContextProvider<HardwareContext> context={context}>
            { for props.children.iter() }
        </ContextProvider<HardwareContext>>
    }
}

#[hook]
pub fn use_hardware() -> HardwareContext {
    use_context::<HardwareContext>().expect("Hardware context not found")
}
