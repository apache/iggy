use std::sync::Mutex;

#[derive(Default)]
pub struct Barrier<T> {
    state: Mutex<BarrierState<T>>,
}

#[derive(Default)]
pub struct BarrierState<T> {
    result: Option<T>,
}

impl<T> BarrierState<T> {
    pub fn set_result(&mut self, result: T) {
        self.result = Some(result);
    }

    pub fn inner(&self) -> &Option<T> {
        &self.result
    }
}

impl<T> Barrier<T>
where
    T: Default,
{
    pub fn new() -> Self {
        Barrier {
            state: Default::default(),
        }
    }

    pub async fn with_async<R>(&self, f: impl AsyncFnOnce(&mut BarrierState<T>) -> R) {
        let mut guard = self.state.lock().unwrap();
        f(&mut guard).await;
    }

    pub async fn with<R>(&self, f: impl FnOnce(&mut BarrierState<T>) -> R) {
        let mut guard = self.state.lock().unwrap();
        f(&mut guard);
    }
}
