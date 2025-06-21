use std::sync::{Condvar, Mutex};

#[derive(Default)]
pub struct Gate<T> {
    state: Mutex<GateState<T>>,
}

#[derive(Default)]
pub struct GateState<T> {
    result: Option<T>,
}

impl<T> GateState<T> {
    pub fn set_result(&mut self, result: T) {
        self.result = Some(result);
    }

    pub fn inner(&self) -> &Option<T> {
        &self.result
    }
}

impl<T> Gate<T>
where
    T: Default,
{
    pub fn new() -> Self {
        Gate {
            state: Default::default(),
        }
    }

    pub async fn with_async<R>(&self, f: impl AsyncFnOnce(&mut GateState<T>) -> R) {
        let mut guard = self.state.lock().unwrap();
        f(&mut guard).await;
    }

    pub async fn with<R>(&self, f: impl FnOnce(&mut GateState<T>) -> R) {
        let mut guard = self.state.lock().unwrap();
        f(&mut guard);
    }
}
