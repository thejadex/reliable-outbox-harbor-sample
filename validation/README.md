# Reliable outbox: collaborative sample prototype

A standalone software engineering task inspired by the event-dispatch reliability theme in the user-supplied approved task archive. The task has fresh code and a standard, digest-pinned Python image. Its authorship and source-theme provenance are disclosed in `reliable-outbox/AUTHORSHIP.md`.

## Verified results

Validated on October 29, 2026, with Harbor 0.8.0 in Docker Desktop on this machine. Each Harbor result is one trial; this is validation of the reference and untouched starter, not an estimate of solver success rates.

```
FSWE-CHECK v1 PASS collaborative-samples/reliable-outbox d10444c82850
```

| Run | Verified mean | Harbor exceptions |
| --- | ---: | ---: |
| oracle | 1.000 | 0 |
| nop | 0.000 | 0 |

The oracle passed all 22 behavioral tests. The nop run used the unchanged starter and scored zero. Raw Harbor result JSON, verifier logs, and reward files are in the `oracle/` and `nop/` evidence folders. The task SHA-256 manifest binds this evidence to the packaged task contents. The checker was copied unchanged from the supplied file.

## Task scope

Implement a durable SQLite event outbox with tenant isolation, atomic fanout, canonical-payload idempotency, subscription snapshots, partition FIFO, fenced leases, deterministic retries, retry exhaustion, and restart recovery. Behavioral tests include separate worker processes, concurrent first initialization, competing submissions and claims, stale-token rejection, malformed-input rollback, and a seeded delivery replay.

The agent receives only the unfinished module. The image contains neither the reference solution nor grading code. During verification, the driver runs candidate code as the unprivileged `agent` account, and the reward directory is root-owned. There are no real credentials, external application services, or third-party Python dependencies. Runtime internet access is disabled.

## Reproduce

Unzip the package and run these commands from its root, with Python 3.11+, Harbor and a running Docker daemon:

```sh
python3 validation/frontierswe-check.py reliable-outbox
harbor run -p reliable-outbox -a oracle --job-name outbox-oracle --jobs-dir jobs
harbor run -p reliable-outbox -a nop --job-name outbox-nop --jobs-dir jobs
```

Docker needs network access to fetch the base image during the first build. No model API key is needed for oracle or nop. Treat `reliable-outbox/` as the task folder; the package root is not a task. Revalidate all three checks after any task-file change. The original static checker must produce the exact fingerprint above for this version.

## Submission fields and remaining work

The checker line and oracle/nop means above are backed by the included evidence. This evidence accompanies the GitHub repository; no form has been submitted. When hosting, share the complete `reliable-outbox/` folder with reviewers, including its reference solution and authorship note; only the runtime environment presented to the evaluated agent should exclude the solution and grading files.

Two substantive eligibility questions remain. First, the user supplied the approved task archive, and Codex prepared most of this standalone implementation and verifier: that is substantial AI authorship. The invitation's restriction on primarily AI-generated samples therefore requires an accurate organizer decision before representing this as eligible. Second, the 72,000-second budget is configured, but actual long-horizon difficulty has not been established through a human solve or independent solver calibration. The checker and oracle/nop gates cannot establish either point. This prototype should be described with those limits rather than claimed to satisfy every invitation requirement.

The supplied invitation states a deadline of Friday, October 30, 12:00 ET. Assuming Eastern Daylight Time, that is 17:00 WAT in Lagos. It permits up to three samples and makes no promise of payment.
