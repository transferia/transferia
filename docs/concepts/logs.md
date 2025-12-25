---
title: "Logs in {{ data-transfer-name }}"
description: "Learn about logs and logging level in {{ data-transfer-name }}."
---

# Logs in {{ data-transfer-name }}

This article provides information on concepts that will help you understand and better manage logs for your {{ data-transfer-name }}:

## Log entries {#log-entries}

**Log entry** records status or describes certain events in a transfer. These messages tell you that certain events occur or show that something is wrong and needs attention.

### Log entry structure {#log-entry-structure}

A log entry contains the full information on a logged event and consists of the following segments:

* Date and time stamp in `YYYY/MM/DD, HH:MM:SS` format.

* Log level indicator.

* Entry text.

## Log levels {#log-levels}

**Log level** is a piece of status information that tells you how important a log entry is.

{{ data-transfer-name }} supports the following log levels:

| Level       | Description                                                   |
|:------------|:--------------------------------------------------------------|
| **TRACE**   | Detailed diagnostics information                              |
| **DEBUG**   | Debugging information                                         |
| **INFO**    | Diagnostics information for statistics                        |
| **WARNING** | Warning about a non-critical malfunction, investigate further |
| **ERROR**   | Error report                                                  |
| **FATAL**   | Possible system failure, service shutdown                     |
