use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use near_primitives::types::AccountId;

use sqlx::types::Uuid;
use tokio::sync::{broadcast, mpsc};
use tracing::error;

use crate::connections::Connections;

pub type BadgeId = sqlx::types::Uuid;
pub type BadgeCheckerRegistrationId = usize;

#[derive(Clone, Debug)]
pub struct BadgeCheckResult {
    pub account_id: AccountId,
    pub awarded: HashSet<BadgeId>,
    pub checked: &'static HashSet<BadgeId>,
}

pub type BadgeWorker = fn(
    connections: Arc<Connections>,
    input: broadcast::Receiver<(AccountId, mpsc::Sender<BadgeCheckResult>)>,
);

#[derive(Debug)]
pub struct BadgeRegistry {
    account_threads: usize,
    connections: Arc<Connections>,
    registration_to_fn: HashMap<
        BadgeCheckerRegistrationId,
        broadcast::Sender<(AccountId, mpsc::Sender<BadgeCheckResult>)>,
    >,
    badge_to_registration: HashMap<BadgeId, BadgeCheckerRegistrationId>,
}

impl BadgeRegistry {
    pub fn new(connections: Arc<Connections>, account_threads: usize) -> Self {
        Self {
            account_threads,
            connections,
            registration_to_fn: HashMap::new(),
            badge_to_registration: HashMap::new(),
        }
    }

    #[tracing::instrument(skip(self))]
    pub fn register<'a, T>(&mut self, badge_ids: T, start_checker: BadgeWorker)
    where
        T: IntoIterator<Item = &'a Uuid> + std::fmt::Debug,
    {
        static REGISTRATION_ID: AtomicUsize = AtomicUsize::new(0);

        let badge_ids: HashSet<_> = badge_ids.into_iter().collect();

        if !self
            .badge_to_registration
            .keys()
            .collect::<HashSet<_>>()
            .is_disjoint(&badge_ids)
        {
            return;
        }

        let registration_id = REGISTRATION_ID.fetch_add(1, Ordering::Relaxed);
        let (account_send, account_recv) = broadcast::channel(self.account_threads);

        for badge_id in badge_ids {
            self.badge_to_registration
                .insert(*badge_id, registration_id);
        }

        self.registration_to_fn
            .insert(registration_id, account_send);

        start_checker(self.connections.clone(), account_recv);
    }

    #[tracing::instrument(skip(self))]
    pub async fn award_badges(
        &self,
        account_id: AccountId,
        ignore_badge_ids: HashSet<BadgeId>,
    ) -> HashSet<BadgeId> {
        let registration_ids = self
            .badge_to_registration
            .iter()
            .filter_map(|(badge_id, registration_id)| {
                if ignore_badge_ids.contains(badge_id) {
                    None
                } else {
                    Some(registration_id)
                }
            })
            .collect::<HashSet<_>>();

        if registration_ids.is_empty() {
            return HashSet::new();
        }

        let (result_send, mut result_recv) = mpsc::channel(registration_ids.len());

        for registration_id in registration_ids {
            let sender = &self.registration_to_fn[registration_id];
            let send_result = sender.send((account_id.clone(), result_send.clone()));
            if let Err(error) = send_result {
                error!("Error sending {account_id} to channel: {error:?}");
            }
        }

        drop(result_send);

        let mut result = HashSet::new();

        while let Some(part) = result_recv.recv().await {
            result.extend(part.awarded);
        }

        result
    }
}

#[macro_export]
macro_rules! create_badge_worker {
    ($query_fn: ident) => {
        #[tracing::instrument(skip_all)]
        pub fn run(
            connections: std::sync::Arc<$crate::connections::Connections>,
            mut input: tokio::sync::broadcast::Receiver<(
                near_primitives::types::AccountId,
                tokio::sync::mpsc::Sender<$crate::badge::BadgeCheckResult>,
            )>,
        ) {
            tokio::spawn(async move {
                while let Ok((account_id, output)) = input.recv().await {
                    let connections = connections.clone();

                    tokio::spawn(async move {
                        let result = $query_fn(connections, account_id.clone()).await;
                        match result {
                            Ok(result) => {
                                let send_result = output.send(result).await;
                                if let Err(e) = send_result {
                                    tracing::error!("Error sending badge result: {e:?}");
                                }
                            }
                            Err(e) => tracing::error!("Error in badge worker result: {e:?}"),
                        }
                    });
                }
            });
        }
    };
}

pub mod transfer;
