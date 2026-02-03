use simulator::Simulator;

fn main() {
    let sim = Simulator::new(3);
    println!("Created simulator with {} replicas", sim.replicas.len());
    for replica in &sim.replicas {
        println!("  - {} (id: {})", replica.name, replica.id);
    }
}
