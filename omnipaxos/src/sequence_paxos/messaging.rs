use crate::sequence_paxos::*;

impl<T, B> SequencePaxos<T, B>
    where
        T: Entry,
        B: Storage<T>,
{
    pub(crate) fn send_msg_to(&mut self, pid: NodeId, msg: PaxosMsg<T>) {
        if self.mode_changer.peer_connectivity.is_connected_to(pid) {
            self.outgoing.push(PaxosMessage {
                from: self.pid,
                to: pid,
                msg,
                action: MessageAction::Handle,
            });
        } else {
            let m = PaxosMessage {
                from: self.pid,
                to: pid,
                msg,
                action: MessageAction::Forward(pid),
            };
            self.broadcast(m);
        }
    }

    pub(crate) fn send_to_all_peers(&mut self, msg: PaxosMsg<T>) {
        for pid in self.mode_changer.peer_connectivity.get_direct_connections() {
            let m = PaxosMessage {
                from: self.pid,
                to: pid,
                msg: msg.clone(),
                action: MessageAction::HandleAndForward(pid),
            };
            self.outgoing.push(m);
        }
    }

    fn broadcast(&mut self, m: PaxosMessage<T>) {
        let original_sender = m.from;
        let mut msg = m;
        msg.from = self.pid;
        for pid in &self.peers {
            if pid != &original_sender {
                msg.to = *pid;
                self.outgoing.push(msg.clone())
            }
        }
    }

    /// Handle an incoming message.
    pub(crate) fn handle(&mut self, m: PaxosMessage<T>) {
        match m.action {
            MessageAction::Handle => {
                self.handle_msg(m);
            }
            MessageAction::HandleAndForward(pid) => {
                self.handle_msg(m.clone());
                self.broadcast(m);
            }
            MessageAction::Forward(pid) => {
                if pid == self.pid {
                    self.handle_msg(m);
                } else {
                    self.broadcast(m);
                }
            }
        }
    }

    pub(crate) fn handle_msg(&mut self, m: PaxosMessage<T>) {
        match m.msg {
            PaxosMsg::PrepareReq(prepreq) => self.handle_preparereq(prepreq, m.from),
            PaxosMsg::Prepare(prep) => self.handle_prepare(prep, m.from),
            PaxosMsg::Promise(prom) => match &self.state {
                (Role::Leader, Phase::Prepare) => self.handle_promise_prepare(prom, m.from),
                (Role::Leader, Phase::Accept) => self.handle_promise_accept(prom, m.from),
                _ => {}
            },
            PaxosMsg::AcceptSync(acc_sync) => self.handle_acceptsync(acc_sync),
            PaxosMsg::AcceptDecide(acc) => self.handle_acceptdecide(acc),
            PaxosMsg::NotAccepted(not_acc) => self.handle_notaccepted(not_acc, m.from),
            PaxosMsg::Accepted(accepted) => self.handle_accepted(accepted, m.from),
            PaxosMsg::Decide(d) => self.handle_decide(d),
            PaxosMsg::ProposalForward(proposals) => self.handle_forwarded_proposal(proposals),
            PaxosMsg::Compaction(c) => self.handle_compaction(c),
            PaxosMsg::AcceptStopSign(acc_ss) => self.handle_accept_stopsign(acc_ss),
            PaxosMsg::ForwardStopSign(f_ss) => self.handle_forwarded_stopsign(f_ss),
            PaxosMsg::Replicate(r) => {
                self.handle_replicate(r);
            }
            PaxosMsg::ReplicateAck(ra) => {
                self.handle_replicate_ack(ra);
            }
            PaxosMsg::AcceptOrder(ao) => {
                self.handle_acceptorder(ao);
            }
            PaxosMsg::DecidedSlot(ds) => self.handle_decidedslot(ds),
        }
    }
}
