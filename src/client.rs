use crate::{
    client::{
        ClientDispatch::{Execute, Reply, StartReplica},
        ClientRunMode::{Normal, Subscription, Transaction},
    },
    command::Command::{
        self, Discard, Echo, Exec, Multi, Ping, Psync, ReplconfCapa, ReplconfListeningPort,
        Subscribe, Watch,
    },
    resp::Resp,
};

pub enum ClientDispatch {
    // Let store execute command
    Execute(Command),
    // Immediate reply
    Reply(Resp),
    // Start this client as replica
    StartReplica(Command),
}

pub enum ClientRunMode {
    Normal,
    Transaction { queue: Vec<Command> },
    Subscription,
}

impl ClientRunMode {
    // Client router and state switcher
    pub fn run(self, command: Command) -> (ClientRunMode, ClientDispatch) {
        match (self, command) {
            // Normal commands
            (Normal, ReplconfListeningPort { .. }) => (Normal, Reply(Resp::simple_string("OK"))),
            (Normal, ReplconfCapa { .. }) => (Normal, Reply(Resp::simple_string("OK"))),
            (Normal, command @ Psync { .. }) => (Normal, StartReplica(command)),
            (Normal, Echo { message }) => (Normal, Reply(Resp::BulkString(message))),
            (Normal, Ping { message }) => match message {
                Some(message) => (Normal, Reply(Resp::BulkString(message))),
                None => (Normal, Reply(Resp::simple_string("PONG"))),
            },
            (Normal, Multi) => (
                Transaction { queue: Vec::new() },
                Reply(Resp::simple_string("OK")),
            ),
            (Normal, Exec) => (Normal, Reply(Resp::simple_error("ERR EXEC without MULTI"))),
            (Normal, Discard) => (
                Normal,
                Reply(Resp::simple_error("ERR DISCARD without MULTI")),
            ),
            (Normal, command @ Subscribe { .. }) => (Subscription, Execute(command)),
            (Normal, command) => (Normal, Execute(command)),

            // Transaction commands
            (Transaction { .. }, command @ Discard) => (Normal, Execute(command)),
            (Transaction { queue }, Watch { .. }) => (
                Transaction { queue },
                Reply(Resp::simple_error("ERR WATCH inside MULTI is not allowed")),
            ),
            (Transaction { queue }, Exec) => (
                Normal,
                Execute(Command::InternalExecuteTx { commands: queue }),
            ),
            (Transaction { mut queue }, command) => {
                queue.push(command);
                (Transaction { queue }, Reply(Resp::simple_string("QUEUED")))
            }

            // Subscription commands
            (Subscription, command @ Subscribe { .. }) => (Subscription, Execute(command)),
            (Subscription, command) => (
                Subscription,
                Reply(Resp::simple_error(&format!(
                    "ERR Can't execute '{}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context",
                    command.name()
                ))),
            ),
        }
    }
}
