# Firestore emulator

To work with the Google Firestore emulator you can use the environment variable:

```bash
export FIRESTORE_EMULATOR_HOST="localhost:8080"
```

or specify it as an option using `FirestoreDb::with_options()`.

When `FIRESTORE_EMULATOR_HOST` is set, the library does not look up the Google
credentials and uses a stub token instead, since the emulator does not
authenticate the requests. This means you do not need any credentials
configured to develop against it. Specifying a token source explicitly, with
`FirestoreDb::with_options_token_source()` for example, still takes precedence.
