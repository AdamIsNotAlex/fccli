# Lessons

- Interactive input dependencies must use a bounded, cancellation-safe `TerminalInput`, never an arbitrary blocking `Read`, so shutdown can join input work before restoring the terminal.
- Guard every transport-specific use site with mutually exclusive feature `cfg`s; enabling both transport features must produce only the dedicated `compile_error!` diagnostic.
- Keep environment-validation accounting distinct: tracker completion records that work was attempted/accounted for, while `FINAL-SCOPE` `PASS` requires authoritative native evidence and native-environment blockers remain explicitly blocked.
