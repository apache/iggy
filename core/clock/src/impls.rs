use crate::Clock;
use iggy_common::IggyTimestamp;

pub struct IggySystemClock;

impl Clock for IggySystemClock {
    type Realtime = IggyTimestamp;

    fn realtime(&self) -> Self::Realtime {
        IggyTimestamp::now()
    }
}
