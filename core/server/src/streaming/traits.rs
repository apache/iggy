use crate::configs::system::SystemConfig;
use crate::shard::task_registry::TaskRegistry;
use std::future::Future;
use std::rc::Rc;

// TODO: Major revision of this trait.
pub trait MainOps {
    type Namespace;
    type PollingArgs;
    type Consumer;
    type In;
    type Out;
    type Error;

    fn append_messages(
        &self,
        config: &SystemConfig,
        registry: &Rc<TaskRegistry>,
        ns: &Self::Namespace,
        input: Self::In,
    ) -> impl Future<Output = Result<(), Self::Error>>;
    fn poll_messages(
        &self,
        ns: &Self::Namespace,
        consumer: Self::Consumer,
        args: Self::PollingArgs,
    ) -> impl Future<Output = Result<Self::Out, Self::Error>>;
}
