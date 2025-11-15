#![allow(dead_code)]

/// Test-only reimplementation of the sanitiser used during integration config loading.
pub fn sanitise_document(raw: &str) -> String {
    raw.lines()
        .map(|line| {
            let mut sanitised = line.replace(" = ", ": ");
            sanitised = replace_backticks(&sanitised);

            if sanitised.matches('"').count() % 2 != 0 {
                sanitised.push('"');
            }

            sanitised
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn replace_backticks(line: &str) -> String {
    let mut result = String::with_capacity(line.len());
    let mut open = false;

    for ch in line.chars() {
        if ch == '`' {
            result.push('"');
            open = !open;
        } else {
            result.push(ch);
        }
    }

    if open {
        result.push('"');
    }

    result
}
