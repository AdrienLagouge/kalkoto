pub fn greeter(name: &str) -> String {
    let output = format!("Call from kalkoto-lib : {}", name);
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = greeter("TestName");
        assert_eq!(result, "Call from kalkoto-lib : TestName".to_owned());
    }
}
