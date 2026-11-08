# Command line tool

[fsq](https://github.com/abdolence/firestore-cli-rs) is a command line client for Firestore built on this crate. It is
for working with a database from a terminal: looking at data, trying a SQL statement before it goes into code, and
managing indexes.

![fsq demo](https://raw.githubusercontent.com/abdolence/firestore-cli-rs/master/media/fsq-demo.webp)

- a SQL REPL (an interactive prompt) with highlighting, completion of collections and fields, and results as tables, with `\x` for one block per document and `\x full` for the path, type and whole value of every field;
- `EXPLAIN`, which draws the query plan as a tree and warns about a table scan, a read of every document of a collection;
- `\l` lists collections, with `\l --recursive` for subcollections, and `\d` describes the fields of a collection;
- `export` and `import` of documents as JSON lines, in the tagged encoding of [JSON conversion](./json.md);
- `index plan`, `index sync` and the other `index` commands, on the `firestore.indexes.json` file of the Firebase CLI;
- `codegen` and `\gen`, which print the fluent API code of a `SELECT`, as [Fluent code for a statement](./sql/codegen.md) describes;
- `--format json` and `--format jsonl` output for scripts.

SQL needs a database of the Enterprise edition, see [Standard and Enterprise editions](./editions.md). `\l`, `\d`,
`export`, `import` and the index commands work on both editions.

## Installation

fsq has binaries for Linux, macOS, Windows and FreeBSD on its [GitHub releases](https://github.com/abdolence/firestore-cli-rs/releases).
The install script picks the one for your system.

On macOS, Linux and FreeBSD:

```sh
curl -fsSL https://raw.githubusercontent.com/abdolence/firestore-cli-rs/master/install.sh | sh
```

On Windows (PowerShell):

```powershell
irm https://raw.githubusercontent.com/abdolence/firestore-cli-rs/master/install.ps1 | iex
```

The scripts install the latest release, check its SHA-256 digest and put `fsq` in `~/.local/bin` on macOS, Linux and
FreeBSD, and in `%LOCALAPPDATA%\Programs\fsq` on Windows. Running a script again upgrades fsq. `FSQ_VERSION` and
`FSQ_INSTALL_DIR` choose another release and directory.

With Rust installed, Cargo builds it from source:

```sh
cargo install firestore-cli --locked
```

The crate is `firestore-cli` and the binary it installs is `fsq`.

## Examples

Connect to a project and a database:

```sh
fsq -p my-project -d my-database
```

Run a statement and print the code of the fluent API for it:

```text
my-project/my-database (enterprise)> SELECT title, rating FROM books WHERE rating >= 4 ORDER BY rating DESC LIMIT 10;
my-project/my-database (enterprise)> \gen
```

Export a collection and import it again:

```sh
fsq -p my-project -d my-database export books -o books.jsonl
fsq -p my-project -d my-database import books.jsonl
```

Show what changes a `firestore.indexes.json` file would make, without applying it:

```sh
fsq -p my-project index plan firestore.indexes.json
```

The options and the rest of the commands are in the
[fsq README](https://github.com/abdolence/firestore-cli-rs#readme).
