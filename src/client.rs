use crate::{
    client::{
        ClientDispatch::{Execute, ExecuteNoReply, MustAuth, Reply, StartReplica},
        ClientRunMode::{Normal, Subscription, Transaction},
    },
    command::Command::{
        self, Auth, Discard, Echo, Exec, Multi, Ping, Psync, Publish, ReplconfAck, ReplconfCapa,
        ReplconfListeningPort, Subscribe, Unsubscribe, Watch,
    },
    resp::Resp,
};

pub enum ClientDispatch {
    // Store executes command and returns reply (resp) back to the client
    Execute(Command),
    // Store executes command, but no reply; client does not await on any reply from store
    ExecuteNoReply(Command),
    // Immediate reply, without store processing
    Reply(Resp),
    // Start this client as replica
    StartReplica(Command),
    // Auth
    MustAuth(String, String),
}

pub enum ClientRunMode {
    Normal,
    Transaction { queue: Vec<Command> },
    Subscription,
}

impl ClientRunMode {
    // Client router and state switcher
    pub fn run(self, need_auth: bool, command: Command) -> (ClientRunMode, ClientDispatch) {
        if let Auth { username, password } = command.clone() {
            if need_auth {
                return (Normal, MustAuth(username, password));
            }
        } else {
            if need_auth {
                return (
                    Normal,
                    Reply(Resp::simple_error("NOAUTH Authentication required.")),
                );
            }
        }

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
            (Normal, command @ ReplconfAck { .. }) => (Normal, ExecuteNoReply(command)),
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
            (Subscription, Ping { .. }) => (
                Subscription,
                Reply(Resp::Array(vec![
                    Resp::bulk_string("pong"),
                    Resp::bulk_string(""),
                ])),
            ),
            (Subscription, command @ Publish { .. }) => (Subscription, Execute(command)),
            (Subscription, command @ Subscribe { .. }) => (Subscription, Execute(command)),
            (Subscription, command @ Unsubscribe { .. }) => (Subscription, Execute(command)),
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
