use std::process::Command;

fn main() -> anyhow::Result<()> {
    let status = Command::new("sea-orm-cli")
        .args([
            "generate",
            "entity",
            "-o",
            "entity/src",
            "--with-serde",
            "both",
        ])
        .status()?;

    if !status.success() {
        std::process::exit(status.code().unwrap_or(1));
    }

    Ok(())
}
