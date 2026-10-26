// DurableAgainst is the failure that ensure_durable makes committed data survive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum DurableAgainst {
    // SoftwareFailure is this process or the operating system crashing while the machine keeps power.
    SoftwareFailure,
    // PowerFailure is the machine losing power.
    PowerFailure,
}
