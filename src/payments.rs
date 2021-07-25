use rust_decimal::Decimal;
use serde::{Deserialize, Serialize, Serializer};
use sled::Iter;

//in an async web service context
// this code has to be offloaded to non async threads
// probably in the rayon runtime

pub struct Accounts<T> {
    db: T,
}

impl<T> Accounts<T>
where
    T: Container,
{
    pub fn new(db: T) -> Self {
        Self { db }
    }
}

impl Accounts<DB> {
    pub fn iter(&self) -> AccountsIterator {
        AccountsIterator {
            iter: self.db.db.iter(),
        }
    }
}

pub struct DB {
    db: sled::Db,
}

impl DB {
    pub fn new(db: sled::Db) -> Self {
        Self { db }
    }
}

impl Drop for DB {
    fn drop(&mut self) {
        let _ = self.db.clear();
        let _ = self.db.flush();
    }
}

pub struct AccountsIterator {
    iter: Iter,
}

impl Iterator for AccountsIterator {
    type Item = AccountData;

    fn next(&mut self) -> Option<Self::Item> {
        let res = self.iter.next()?;
        let (_, bytes) = res.unwrap();
        let acc: Account = bincode::deserialize(&bytes).expect("all hell broke loose");
        let acc_data: AccountData = acc.into();
        Some(acc_data)
    }
}

impl<T> Accounts<T>
where
    T: Container,
{
    pub fn handle(&mut self, action: impl Action<T>) -> Result<(), ActionError> {
        action.apply(&mut self.db)
    }
}

pub trait Container {
    fn get_or_create(&self, id: &ClientID) -> Result<Account, ActionError>;
    fn get_account(&self, id: &ClientID) -> Result<Account, ActionError>;
    fn save_account(&mut self, acc: Account);
}

mod private {
    pub trait Sealed {}
    impl<T> Sealed for T where T: super::Container {}
}

impl Container for DB {
    fn get_or_create(&self, id: &ClientID) -> Result<Account, ActionError> {
        let acc = match self.get_account(id) {
            Err(ActionError::InvalidClientID) => Account::new(id.clone()),
            Ok(k) => k,
            Err(e) => return Err(e),
        };

        Ok(acc)
    }

    fn get_account(&self, id: &ClientID) -> Result<Account, ActionError> {
        let bytes = self
            .db
            .get(id.to_le_bytes())
            .unwrap()
            .ok_or(ActionError::InvalidClientID)?;

        let acc: Account = bincode::deserialize(&bytes).expect("all hell broke loose");
        Ok(acc)
    }

    fn save_account(&mut self, acc: Account) {
        let bytes = bincode::serialize(&acc).expect("all hell broke loose");
        self.db
            .insert(acc.client.to_le_bytes(), bytes)
            .expect("all hell broke loose");
        let _ = self.db.flush();
    }
}

//the description is missing one column - locked
//client,available,held,total,
// 2,2,0,2,false
// 1,1.5,0,1.5,false

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct AccountData {
    client: ClientID,
    #[serde(serialize_with = "round_serialize")]
    available: Decimal,
    //we don't need both fields
    //we can calculate one of those values on the fly
    //however, i cannot make that call now as i am not familiar with the exact performance requirements
    // and therefore if the tradeoff with having an extra calculation or using more memory is worth it
    #[serde(serialize_with = "round_serialize")]
    held: Decimal,
    #[serde(serialize_with = "round_serialize")]
    total: Decimal,
    locked: bool,
}

impl From<Account> for AccountData {
    fn from(acc: Account) -> Self {
        Self {
            client: acc.client,
            available: acc.available.round_dp(4),
            held: acc.held.round_dp(4),
            total: acc.total.round_dp(4),
            locked: acc.locked,
        }
    }
}

#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
pub struct Account {
    client: ClientID,
    #[serde(serialize_with = "round_serialize")]
    available: Decimal,
    //we don't need both fields
    //we can calculate one of those values on the fly
    //however, i cannot make that call now as i am not familiar with the exact performance requirements
    // and therefore if the tradeoff with having an extra calculation or using more memory is worth it
    #[serde(serialize_with = "round_serialize")]
    held: Decimal,
    #[serde(serialize_with = "round_serialize")]
    total: Decimal,
    locked: bool,

    deposits: Vec<Transaction<Deposit>>,
    withdrawals: Vec<Transaction<Withdrawal>>,
    disputes: Vec<Disputed>,
    resolves: Vec<Resolved>,
}

fn round_serialize<S>(x: &Decimal, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if x.is_zero() {
        return s.serialize_str("0");
    }
    s.serialize_str(&x.round_dp(4).to_string())
}

impl Account {
    fn new(cid: ClientID) -> Self {
        Self {
            client: cid,
            available: Decimal::from(0),
            held: Decimal::from(0),
            total: Decimal::from(0),
            locked: false,
            deposits: vec![],
            withdrawals: vec![],
            disputes: vec![],
            resolves: vec![],
        }
    }
}

// prevents users on writing exhaustive code
// so their code won't break when/if we add new variants
#[non_exhaustive]
#[derive(Debug, PartialEq)]
pub enum ActionError {
    AccountLocked,
    InsufficientFunds,
    InvalidClientID,
    InvalidTxID,
}

pub trait Action<T>
where
    T: Container,
{
    // we consume the action
    // we don't want the possibility
    // that it could be executed twice
    fn apply(self, accts: &mut T) -> Result<(), ActionError>;
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum TransactionType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

type ClientID = u16;
type TxID = u32;

// This pattern below is using Rust's
// type system as a state machine
// there won't be a possibility of a mistake
// to run an invalid action on a state
#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
pub struct Transaction<T> {
    t: T,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TransactionData {
    // An enum is more memory efficient and easier to work with
    // compared to a String
    #[serde(rename = "type")]
    t_type: TransactionType,
    client: ClientID,
    tx: TxID,
    amount: Option<Decimal>,
}

impl TransactionData {
    pub fn tx_type(&self) -> TransactionType {
        self.t_type
    }
}

impl<T> Action<T> for Transaction<Deposit>
where
    T: Container,
{
    fn apply(self, accts: &mut T) -> Result<(), ActionError> {
        let mut acc = accts.get_or_create(&self.t.client)?;
        check_is_locked(&acc)?;
        check_tx_exists(&self.t.tx, &acc)?;

        acc.available += self.t.amount;
        acc.total += self.t.amount;
        acc.deposits.push(self);

        accts.save_account(acc);
        Ok(())
    }
}

// prevents users on writing exhaustive code
// so their code won't break when/if we add new variants
#[non_exhaustive]
#[derive(Debug)]
pub enum InnerError {
    InvalidType(TransactionType),
    MissingAmount,
    HasAmount,
}

#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
pub struct Deposit {
    client: ClientID,
    tx: TxID,
    amount: Decimal,
}

#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
struct Disputed {
    deposit: Transaction<Deposit>,
}

impl Transaction<Deposit> {
    pub fn new(t: TransactionData) -> Result<Self, InnerError> {
        if t.t_type != TransactionType::Deposit {
            return Err(InnerError::InvalidType(t.t_type));
        }

        let amount = t.amount.ok_or(InnerError::MissingAmount)?;

        let deposit = Deposit {
            client: t.client,
            tx: t.tx,
            amount,
        };
        Ok(Self { t: deposit })
    }

    fn dispute(self, d: Transaction<Dispute>) -> Result<Disputed, ActionError> {
        if d.t.tx != self.t.tx {
            return Err(ActionError::InvalidTxID);
        }

        if d.t.client != self.t.client {
            return Err(ActionError::InvalidClientID);
        }

        Ok(Disputed { deposit: self })
    }
}

impl Disputed {
    fn resolve(self, r: Resolve) -> Result<Resolved, ActionError> {
        if r.tx != self.deposit.t.tx {
            return Err(ActionError::InvalidTxID);
        }

        if r.client != self.deposit.t.client {
            return Err(ActionError::InvalidClientID);
        }

        Ok(Resolved { disputed: self })
    }
}

#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
struct Resolved {
    disputed: Disputed,
}

impl Resolved {
    fn chargeback(self, r: Chargeback) -> Result<Chargedback, ActionError> {
        if r.tx != self.disputed.deposit.t.tx {
            return Err(ActionError::InvalidTxID);
        }

        if r.client != self.disputed.deposit.t.client {
            return Err(ActionError::InvalidClientID);
        }

        Ok(Chargedback { resolved: self })
    }
}

#[allow(unused)]
struct Chargedback {
    resolved: Resolved,
}

#[inline(always)]
fn check_is_locked(acc: &Account) -> Result<(), ActionError> {
    if acc.locked {
        Err(ActionError::AccountLocked)
    } else {
        Ok(())
    }
}

#[allow(unused)]
#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
pub struct Withdrawal {
    client: ClientID,
    tx: TxID,
    amount: Decimal,
}

impl Transaction<Withdrawal> {
    pub fn new(t: TransactionData) -> Result<Self, InnerError> {
        if t.t_type != TransactionType::Withdrawal {
            return Err(InnerError::InvalidType(t.t_type));
        }
        let amount = t.amount.ok_or(InnerError::MissingAmount)?;

        Ok(Self {
            t: Withdrawal {
                client: t.client,
                tx: t.tx,
                amount,
            },
        })
    }
}

#[inline(always)]
fn check_sufficient_funds(amount: &Decimal, acc: &Account) -> Result<(), ActionError> {
    if &acc.available < amount {
        Err(ActionError::InsufficientFunds)
    } else {
        Ok(())
    }
}

fn check_tx_exists(tx: &TxID, acc: &Account) -> Result<(), ActionError> {
    let is_deposit = acc.deposits.iter().find(|&a| a.t.tx == *tx).is_some();
    let is_withdrawal = acc.withdrawals.iter().find(|&a| a.t.tx == *tx).is_some();

    if is_deposit || is_withdrawal {
        return Err(ActionError::InvalidTxID);
    }
    Ok(())
}

impl<T> Action<T> for Transaction<Withdrawal>
where
    T: Container,
{
    fn apply(self, accts: &mut T) -> Result<(), ActionError> {
        let mut acc = accts.get_account(&self.t.client)?;
        check_is_locked(&acc)?;
        check_tx_exists(&self.t.tx, &acc)?;
        check_sufficient_funds(&self.t.amount, &acc)?;

        acc.available = check_div_negative(&acc.available, &self.t.amount)?;
        acc.total = check_div_negative(&acc.total, &self.t.amount)?;
        acc.withdrawals.push(self);

        accts.save_account(acc);

        Ok(())
    }
}

//What can actually be disputed?
// From the description, it looks like only a deposit can be
pub struct Dispute {
    client: ClientID,
    tx: TxID,
}

impl Transaction<Dispute> {
    pub fn new(t: TransactionData) -> Result<Self, InnerError> {
        if t.t_type != TransactionType::Dispute {
            return Err(InnerError::InvalidType(t.t_type));
        }

        if t.amount.is_some() {
            return Err(InnerError::HasAmount);
        }

        Ok(Self {
            t: Dispute {
                client: t.client,
                tx: t.tx,
            },
        })
    }
}

impl<T> Action<T> for Transaction<Dispute>
where
    T: Container,
{
    fn apply(self, accts: &mut T) -> Result<(), ActionError> {
        let mut acc = accts.get_account(&self.t.client)?;
        check_is_locked(&acc)?;
        let pos = acc
            .deposits
            .iter()
            .position(|e| e.t.tx == self.t.tx)
            .ok_or(ActionError::InvalidTxID)?;

        let tx = acc.deposits.remove(pos);

        let amount = tx.t.amount;
        let disputed = tx.dispute(self)?;

        acc.disputes.push(disputed);
        acc.available = check_div_negative(&acc.available, &amount)?;
        acc.held += amount;

        accts.save_account(acc);

        Ok(())
    }
}

pub struct Resolve {
    client: ClientID,
    tx: TxID,
}

impl Transaction<Resolve> {
    pub fn new(t: TransactionData) -> Result<Self, InnerError> {
        if t.t_type != TransactionType::Resolve {
            return Err(InnerError::InvalidType(t.t_type));
        }

        if t.amount.is_some() {
            return Err(InnerError::HasAmount);
        }

        Ok(Self {
            t: Resolve {
                client: t.client,
                tx: t.tx,
            },
        })
    }
}

impl<T> Action<T> for Transaction<Resolve>
where
    T: Container,
{
    fn apply(self, accts: &mut T) -> Result<(), ActionError> {
        let mut acc = accts.get_account(&self.t.client)?;
        check_is_locked(&acc)?;
        let pos = acc
            .disputes
            .iter()
            .position(|e| e.deposit.t.tx == self.t.tx)
            .ok_or(ActionError::InvalidTxID)?;

        let tx = acc.disputes.remove(pos);
        let amount = tx.deposit.t.amount;
        let resolved = tx.resolve(self.t)?;

        acc.resolves.push(resolved);
        acc.held = check_div_negative(&acc.held, &amount)?;
        acc.available += amount;

        accts.save_account(acc);

        Ok(())
    }
}

// the resolve already decreases the held amount
// but the description of chargeback says the held funds
// decrease too
pub struct Chargeback {
    client: ClientID,
    tx: TxID,
}

impl Transaction<Chargeback> {
    pub fn new(t: TransactionData) -> Result<Self, InnerError> {
        if t.t_type != TransactionType::Chargeback {
            return Err(InnerError::InvalidType(t.t_type));
        }

        if t.amount.is_some() {
            return Err(InnerError::HasAmount);
        }

        Ok(Self {
            t: Chargeback {
                client: t.client,
                tx: t.tx,
            },
        })
    }
}

fn check_div_negative(a: &Decimal, b: &Decimal) -> Result<Decimal, ActionError> {
    let c = a - b;

    if c.is_sign_negative() {
        return Err(ActionError::InsufficientFunds);
    }
    Ok(c)
}

impl<T> Action<T> for Transaction<Chargeback>
where
    T: Container,
{
    fn apply(self, accts: &mut T) -> Result<(), ActionError> {
        let mut acc = accts.get_account(&self.t.client)?;
        check_is_locked(&acc)?;

        let pos = acc
            .resolves
            .iter()
            .position(|e| e.disputed.deposit.t.tx == self.t.tx)
            .ok_or(ActionError::InvalidTxID)?;

        let tx = acc.resolves.remove(pos);
        let amount = tx.disputed.deposit.t.amount;
        // this is the final state so we don't need to store anything
        // at least for this task
        let _ = tx.chargeback(self.t)?;
        acc.available = check_div_negative(&acc.available, &amount)?;
        acc.total = check_div_negative(&acc.total, &amount)?;
        acc.locked = true;

        accts.save_account(acc);

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use rust_decimal::prelude::FromPrimitive;
    use std::collections::HashMap;

    #[derive(Default)]
    struct MockContainer {
        data: HashMap<ClientID, Account>,
    }

    impl Container for MockContainer {
        fn get_or_create(&self, id: &ClientID) -> Result<Account, ActionError> {
            match self.data.get(id) {
                Some(s) => Ok(s.clone()),
                None => Ok(Account::new(*id)),
            }
        }

        fn get_account(&self, id: &ClientID) -> Result<Account, ActionError> {
            self.data
                .get(id)
                .ok_or(ActionError::InvalidClientID)
                .map(|a| a.clone())
        }

        fn save_account(&mut self, acc: Account) {
            self.data.insert(acc.client, acc);
        }
    }

    #[test]
    fn deposit() {
        let tx = Transaction::<Deposit>::new(TransactionData {
            t_type: TransactionType::Deposit,
            client: 1,
            tx: 1,
            amount: Some(Decimal::from(1)),
        })
        .unwrap();

        let c = MockContainer::default();
        let mut actts = Accounts::new(c);
        actts.handle(tx.clone()).unwrap();
        let acc = actts.db.get_account(&1).unwrap();

        let mut tx2 = tx.clone();
        tx2.t.tx += 1;

        let mut expect = Account {
            client: 1,
            available: Decimal::from(1),
            held: Decimal::from(0),
            total: Decimal::from(1),
            locked: false,
            deposits: vec![tx.clone()],
            withdrawals: vec![],
            disputes: vec![],
            resolves: vec![],
        };

        assert_eq!(acc, expect);

        expect.deposits.push(tx2.clone());
        actts.handle(tx2.clone()).unwrap();
        let acc = actts.db.get_account(&1).unwrap();
        expect.available += Decimal::from(1);
        expect.total += Decimal::from(1);

        assert_eq!(acc, expect);
    }

    #[test]
    fn duplicate_deposit() {
        let tx = Transaction::<Deposit>::new(TransactionData {
            t_type: TransactionType::Deposit,
            client: 1,
            tx: 1,
            amount: Some(Decimal::from(1)),
        })
        .unwrap();

        let c = MockContainer::default();
        let mut actts = Accounts::new(c);
        actts.handle(tx.clone()).unwrap();
        let err = actts.handle(tx.clone()).expect_err("duplicate deposit");
        assert_eq!(err, ActionError::InvalidTxID);
    }

    #[test]
    fn withdrawal() {
        let tx = Transaction::<Deposit>::new(TransactionData {
            t_type: TransactionType::Deposit,
            client: 1,
            tx: 1,
            amount: Some(Decimal::from(1)),
        })
        .unwrap();

        let c = MockContainer::default();
        let mut actts = Accounts::new(c);
        actts.handle(tx.clone()).unwrap();

        let withdrawal = Transaction::<Withdrawal>::new(TransactionData {
            t_type: TransactionType::Withdrawal,
            client: 1,
            tx: 2,
            amount: Some(Decimal::from(1)),
        })
        .unwrap();

        actts.handle(withdrawal.clone()).unwrap();
        let acc = actts.db.get_account(&1).unwrap();

        let expect = Account {
            client: 1,
            available: Default::default(),
            held: Default::default(),
            total: Default::default(),
            locked: false,
            deposits: vec![tx.clone()],
            withdrawals: vec![withdrawal],
            disputes: vec![],
            resolves: vec![],
        };

        assert_eq!(acc, expect);
    }

    #[test]
    fn withdrawal_negative() {
        let tx = Transaction::<Deposit>::new(TransactionData {
            t_type: TransactionType::Deposit,
            client: 1,
            tx: 1,
            amount: Some(Decimal::from(1)),
        })
        .unwrap();

        let c = MockContainer::default();
        let mut actts = Accounts::new(c);
        actts.handle(tx.clone()).unwrap();

        let withdrawal = Transaction::<Withdrawal>::new(TransactionData {
            t_type: TransactionType::Withdrawal,
            client: 1,
            tx: 2,
            amount: Some(Decimal::from(2)),
        })
        .unwrap();

        let err = match actts.handle(withdrawal.clone()) {
            Ok(_) => panic!("should get an error"),
            Err(e) => e,
        };

        assert_eq!(err, ActionError::InsufficientFunds);
    }

    #[test]
    fn decimal_format() {
        let tx = Transaction::<Deposit>::new(TransactionData {
            t_type: TransactionType::Deposit,
            client: 1,
            tx: 1,
            amount: Some(Decimal::from_f64(1.11111).unwrap()),
        })
        .unwrap();

        let c = MockContainer::default();
        let mut actts = Accounts::new(c);
        actts.handle(tx.clone()).unwrap();

        let acc = actts.db.get_account(&1).unwrap();
        let acc_data: AccountData = acc.into();

        assert_eq!(
            acc_data,
            AccountData {
                client: 1,
                available: Decimal::from_f64(1.1111).unwrap(),
                held: Default::default(),
                total: Decimal::from_f64(1.1111).unwrap(),
                locked: false
            }
        );
    }

    #[test]
    fn cannot_use_frozen_account() {
        let tx = Transaction::<Deposit>::new(TransactionData {
            t_type: TransactionType::Deposit,
            client: 1,
            tx: 1,
            amount: Some(Decimal::from_f64(1.0).unwrap()),
        })
        .unwrap();

        let dispute = Transaction::<Dispute>::new(TransactionData {
            t_type: TransactionType::Dispute,
            client: 1,
            tx: 1,
            amount: None,
        })
        .unwrap();

        let resolve = Transaction::<Resolve>::new(TransactionData {
            t_type: TransactionType::Resolve,
            client: 1,
            tx: 1,
            amount: None,
        })
        .unwrap();

        let chargeback = Transaction::<Chargeback>::new(TransactionData {
            t_type: TransactionType::Chargeback,
            client: 1,
            tx: 1,
            amount: None,
        })
        .unwrap();

        let c = MockContainer::default();
        let mut actts = Accounts::new(c);

        actts.handle(tx.clone()).unwrap();
        actts.handle(dispute).unwrap();
        actts.handle(resolve).unwrap();
        actts.handle(chargeback).unwrap();

        let mut tx2 = tx.clone();
        tx2.t.tx += 1;

        let err = actts
            .handle(tx2)
            .expect_err("expect frozen account to not be accessible");
        assert_eq!(err, ActionError::AccountLocked);
    }

    #[test]
    fn dispute_process() {
        let tx = Transaction::<Deposit>::new(TransactionData {
            t_type: TransactionType::Deposit,
            client: 1,
            tx: 1,
            amount: Some(Decimal::from_f64(1.0).unwrap()),
        })
        .unwrap();

        let dispute = Transaction::<Dispute>::new(TransactionData {
            t_type: TransactionType::Dispute,
            client: 1,
            tx: 1,
            amount: None,
        })
        .unwrap();

        let resolve = Transaction::<Resolve>::new(TransactionData {
            t_type: TransactionType::Resolve,
            client: 1,
            tx: 1,
            amount: None,
        })
        .unwrap();

        let chargeback = Transaction::<Chargeback>::new(TransactionData {
            t_type: TransactionType::Chargeback,
            client: 1,
            tx: 1,
            amount: None,
        })
        .unwrap();

        let c = MockContainer::default();
        let mut actts = Accounts::new(c);

        actts.handle(tx.clone()).unwrap();

        let acc = actts.db.get_account(&1).unwrap();
        let acc_data: AccountData = acc.into();

        assert_eq!(
            acc_data,
            AccountData {
                client: 1,
                available: Decimal::from_f64(1.0).unwrap(),
                held: Default::default(),
                total: Decimal::from_f64(1.0).unwrap(),
                locked: false
            }
        );

        actts.handle(dispute).unwrap();

        let acc = actts.db.get_account(&1).unwrap();
        let acc_data: AccountData = acc.into();

        assert_eq!(
            acc_data,
            AccountData {
                client: 1,
                available: Decimal::from(0),
                held: Decimal::from(1),
                total: Decimal::from(1),
                locked: false
            }
        );

        actts.handle(resolve).unwrap();

        let acc = actts.db.get_account(&1).unwrap();
        let acc_data: AccountData = acc.into();

        assert_eq!(
            acc_data,
            AccountData {
                client: 1,
                available: Decimal::from(1),
                held: Decimal::from(0),
                total: Decimal::from(1),
                locked: false
            }
        );

        actts.handle(chargeback).unwrap();

        let acc = actts.db.get_account(&1).unwrap();
        let acc_data: AccountData = acc.into();

        assert_eq!(
            acc_data,
            AccountData {
                client: 1,
                available: Decimal::from(0),
                held: Decimal::from(0),
                total: Decimal::from(0),
                locked: true
            }
        );
    }
}
