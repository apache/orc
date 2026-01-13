---
layout: page
title: Security
---

Apache ORC is a library rather than an execution framework and thus
is less likely to have security vulnerabilities. However, if you have
discovered one, please follow the process below.

## Threat Model

We align our threat model with other foundational data format libraries like [Apache Arrow](https://arrow.apache.org/docs/dev/format/Security.html). When evaluating potential security vulnerabilities, especially those discovered via fuzzing tools, we apply the following principles to distinguish between normal robustness bugs and actual security vulnerabilities.

### 1. Trusted vs. Untrusted Data Boundaries

Apache ORC is a low-level format library designed to read and write data from trusted storage systems (e.g., internal data lakes, HDFS, S3). The parsing APIs assume that the underlying data originates from a trusted internal source.

If an attacker is able to write arbitrary, maliciously crafted ORC files into your internal storage, the system is already compromised at the infrastructure or access control level (e.g., IAM permissions, API gateways). The `orc` library itself is not the appropriate security boundary to defend against compromised storage infrastructure.

### 2. Robustness Issues vs. Security Vulnerabilities

A crash, Out-Of-Memory (OOM), Out-Of-Bounds (OOB) read, or assertion failure caused by feeding a maliciously fuzzed file directly into the low-level parser is considered a **robustness issue** (a regular software bug), not a security vulnerability.

Such issues are only treated as security vulnerabilities (CVEs) if they:
* Lead to Remote Code Execution (RCE).
* Bypass a defined security boundary or cause cross-tenant data leakage.
* Occur in a server component explicitly designed to process unverified, external data.

Missing bounds checks or crashes during the parsing of malformed files do not typically lead to code execution or break isolation sandboxes. They are treated as regular software defects.

### 3. Responsibilities of the Low-Level Library

The primary goal of a low-level serialization/deserialization library like ORC is high performance. Mandating strict defensive programming and validation on every internal memory operation would severely degrade performance.

Security validation and sanitization of untrusted inputs should occur at the data ingestion layer (e.g., before data is admitted into the data lake). It is outside the scope of the format parser to proactively defend against all possible artificially corrupted bits.

## Reporting a Vulnerability

We strongly encourage folks to report security vulnerabilities to our
private security mailing list first, before disclosing them in a
public forum.

Please note that the security mailing list should only be used for
reporting undisclosed security vulnerabilities in Apache ORC and
managing the process of fixing such vulnerabilities. We cannot accept
regular bug reports or other security related queries at this
address. All mail sent to this address that does not relate to an
undisclosed security problem in Apache ORC will be ignored.

The ORC security mailing list address is:
<a href="mailto:security@orc.apache.org">security@orc.apache.org</a>.
This is a private mailing list and only members of the ORC project
are subscribed.

Please note that we do not use a team GnuPG key. If you wish to
encrypt your e-mail to security@orc.apache.org then please use the GnuPG
keys from [ORC GPG keys](https://dist.apache.org/repos/dist/release/orc/KEYS) for
the members of the
[ORC PMC](https://people.apache.org/phonebook.html?ctte=orc).

## Vulnerability Handling

An overview of the vulnerability handling process is:

* The reporter sends email to the project privately.
* The project works privately with the reporter to resolve the vulnerability.
* The project releases a new version that includes the fix.
* The vulnerability is publicly announced via a [CVE](https://cve.mitre.org/) to the mailing lists and the original reporter.

The full process can be found on the
[Apache Security Process](https://www.apache.org/security/committers.html#vulnerability-handling) page.

## Fixed CVEs

* [CVE-2018-8015](CVE-2018-8015) - ORC files with malformed types cause stack overflow.
* [CVE-2025-47436](CVE-2025-47436) - Potential Heap Buffer Overflow during C++ LZO Decompression
