# Claude Code Notes

## Cargo Commands

- `cargo check` - checks the mailclerk-server package (it's the default, no need for `--package mailclerk-server`)
- `cargo test` - tests the mailclerk-server package by default

## Important Notes

- A user's `daily_token_limit` can be set to `i64::MAX`. Take care to avoid integer overflows when adding to or multiplying this value.
- Use `.is_multiple_of(n)` instead of `num % n == 0` (stable in Rust 1.92).
- Prefer `#[derive(...)]` over manual trait implementations when the impl can be derived.
