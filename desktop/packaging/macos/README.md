# macOS Packaging

This directory is reserved for local bundle, LaunchAgent, signing, and
notarization work.

## First Proof of Concept

Notarization is not required for the first local proof of concept. The first
packaging milestone should produce a local testable app bundle and daemon
installation flow.

Initial packaging goals:

- build a local `Heimdallr.app`;
- bundle the Swift app and Go daemon;
- keep mutable state under `~/Library/Application Support/Heimdallr/`;
- provide LaunchAgent install and uninstall helpers;
- support a development checkout engine path;
- avoid Apple Developer secrets in the repository.

## Later Distribution Lane

When requested, add:

- Developer ID signing;
- hardened runtime settings;
- notarization submission;
- stapling;
- clean-machine Gatekeeper smoke;
- release notes for runtime size, first-run downloads, DICOM firewall access,
  and TotalSegmentator license handling.

Do not commit `.app`, `.dmg`, `.pkg`, `.zip`, notarization logs with secrets,
certificates, provisioning profiles, or API credentials.
