mod config;
mod error;
mod rpc;
mod supervisor;

#[macro_use]
extern crate log;

#[macro_use]
extern crate quick_error;

#[macro_use]
extern crate crossbeam;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
