Unit tests cover all transaction types and states.
See in the bottom of the `payments.rs` file.

I have used the type system as a state machine so it makes it more
difficult to make a mistake and have a transaction in an invalid state.
For more details, checkout the source code.