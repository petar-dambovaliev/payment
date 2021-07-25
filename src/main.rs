use std::process;
use std::{env, io};

mod payments;

use payments::{
    Accounts, Chargeback, Deposit, Dispute, Resolve, Transaction, TransactionData, TransactionType,
    Withdrawal, DB,
};

use csv::{DeserializeRecordsIter, Writer};

macro_rules! handle {
    ($t:ty,$acc:ident,$td:ident) => {
        let t = match Transaction::<$t>::new($td) {
            Ok(td) => td,
            Err(_) => {
                //println!("{:#?}", e);
                continue;
            }
        };

        if let Err(_) = $acc.handle(t) {
            //println!("{:#?}", e);
        }
    };
}

const DB_PATH: &str = "./db/";

// A question for Kraken
// Why isn't the amount in the smallest divisible unit?
// It is less error prone and easier to handle
fn main() -> csv::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        println!("filepath to a csv file is required as an argument");
        process::exit(1);
    }

    let db = sled::open(DB_PATH).expect("cannot open the database");
    let accounts = parse_data(&args[1], db);
    write_data(accounts)
}

fn write_data(accts: Accounts<DB>) -> csv::Result<()> {
    let out = io::stdout();
    let mut w = Writer::from_writer(out.lock());

    for acc in accts.iter() {
        if let Err(_) = w.serialize(acc) {
            //println!("{:#?}", e);
        }
    }

    w.flush()?;
    Ok(())
}

fn parse_data(path: &String, db: sled::Db) -> Accounts<DB> {
    let mut r = csv::ReaderBuilder::default()
        .trim(csv::Trim::All)
        .from_path(path)
        .expect("all hell broke loose");

    let mut accounts = Accounts::new(DB::new(db));
    let iter: DeserializeRecordsIter<_, TransactionData> = r.deserialize();

    for res in iter {
        let td = match res {
            Ok(tr) => tr,
            Err(_) => {
                //println!("{:#?}", e);
                continue;
            }
        };

        match td.tx_type() {
            TransactionType::Deposit => {
                handle!(Deposit, accounts, td);
            }
            TransactionType::Withdrawal => {
                handle!(Withdrawal, accounts, td);
            }
            TransactionType::Dispute => {
                handle!(Dispute, accounts, td);
            }
            TransactionType::Resolve => {
                handle!(Resolve, accounts, td);
            }
            TransactionType::Chargeback => {
                handle!(Chargeback, accounts, td);
            }
        }
    }
    accounts
}
