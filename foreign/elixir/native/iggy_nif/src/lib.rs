use rustler::{Env, Term, resource};

pub mod atom;
pub mod client;

rustler::init!(
    "Elixir.IggyEx",
    load = on_load
);

fn on_load(env: Env, _info: Term) -> bool {
    #[allow(non_local_definitions, unused_must_use)]
    {
        let _ = resource!(client::IggyResource, env);
    }
    true
}
